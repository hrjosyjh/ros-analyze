#!/usr/bin/env python3
"""
ROS2 AGV launch.log 시간대별 분석기

14GB+ 로그 파일을 스트리밍으로 읽어 시간대별 통계를 생성합니다.
실시간 모니터링 모드(-f)로 로그 파일을 tail -f 방식으로 추적할 수 있습니다.

사용법:
    # 정적 분석 (기본: 증분 분석)
    python3 analyze_log.py launch.log                    # 증분 분석 (새 내용만, 기본)
    python3 analyze_log.py launch.log --full             # 전체 재분석
    python3 analyze_log.py launch.log --interval 10m     # 10분 단위
    python3 analyze_log.py launch.log --interval 1m      # 1분 단위
    python3 analyze_log.py launch.log --interval 30s     # 30초 단위
    python3 analyze_log.py launch.log --top-nodes 10     # 상위 10개 노드만 표시
    python3 analyze_log.py launch.log --errors-only      # ERROR/WARN/FATAL만 표시
    python3 analyze_log.py launch.log --node motor_driver_node-11  # 특정 노드 필터
    python3 analyze_log.py launch.log --csv report.csv   # CSV 파일로 출력

    # 실시간 모니터링
    python3 analyze_log.py launch.log -f                 # 실시간 대시보드
    python3 analyze_log.py launch.log -f --window 10m    # 최근 10분 윈도우
    python3 analyze_log.py launch.log -f --refresh 1     # 1초마다 갱신
    python3 analyze_log.py launch.log -f --tail 1000     # 마지막 1000줄부터 시작
    python3 analyze_log.py launch.log -f -e              # 에러/경고만 실시간 추적
    python3 analyze_log.py launch.log -f --node motor    # 특정 노드만 실시간 추적
"""

import argparse
import csv
import glob
import io
import json
import os
import re
import signal
import shlex
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

from log_parser import RE_ANSI, bucket_key, bucket_label, parse_datetime_arg, parse_interval, parse_line


# --- 메인 분석 ---

def _extract_msg_part(clean_line, node, max_len=200):
    """로그 원문에서 노드명 이후 메시지 부분을 추출합니다."""
    if not clean_line:
        return ''
    if node:
        idx = clean_line.find(node)
        if idx >= 0:
            after = clean_line.find(']', idx)
            if after >= 0:
                return clean_line[after + 1:].strip()[:max_len]
    tail = clean_line.strip()
    return tail[-max_len:]


class _FocusPatternInfo:
    __slots__ = ('count', 'example')

    def __init__(self):
        self.count = 0
        self.example = ''

class LogAnalyzer:
    def __init__(self, interval_sec, node_filter=None, errors_only=False, focus_query=None,
                 ts_from=None, ts_to=None):
        self.interval_sec = interval_sec
        self.node_filter = node_filter
        self.errors_only = errors_only
        self.focus_query = focus_query
        self.ts_from = ts_from    # 시작 시각 (epoch float or None)
        self.ts_to = ts_to        # 종료 시각 (epoch float or None)

        # 시간대별 통계
        self.bucket_total = defaultdict(int)           # bucket -> count
        self.bucket_levels = defaultdict(lambda: defaultdict(int))  # bucket -> {level: count}
        self.bucket_nodes = defaultdict(lambda: defaultdict(int))   # bucket -> {node: count}

        # 전체 통계
        self.total_lines = 0
        self.parsed_lines = 0
        self.matched_lines = 0
        self.unparsed_lines = 0
        self.level_totals = defaultdict(int)
        self.node_totals = defaultdict(int)
        self.node_level_totals = defaultdict(lambda: defaultdict(int))  # node -> {level: count}

        # 에러 메시지 수집 (최대 500개)
        self.error_messages = []
        self.max_error_msgs = 500

        # 포커스 노드 상세(부분 매칭, post-filter)
        self.focus_total = 0
        self.focus_levels = defaultdict(int)
        self.focus_ts_min = float('inf')
        self.focus_ts_max = float('-inf')
        self.focus_timeline = defaultdict(int)  # bucket -> count
        self.focus_timeline_levels = defaultdict(lambda: defaultdict(int))  # bucket -> {level: count}
        self.focus_error_samples = []
        self.max_focus_error_samples = 200

        # 시간 범위
        self.ts_min = float('inf')
        self.ts_max = float('-inf')

    def process_line(self, line):
        self.total_lines += 1

        result = parse_line(line)
        if result is None:
            self.unparsed_lines += 1
            return

        ts, node, level = result
        self.parsed_lines += 1

        # 시간 범위 필터
        if self.ts_from is not None and ts < self.ts_from:
            return
        if self.ts_to is not None and ts > self.ts_to:
            return

        # 필터 적용
        if self.node_filter and self.node_filter not in node:
            return
        if self.errors_only and level not in ('ERROR', 'WARN', 'FATAL'):
            return

        self.matched_lines += 1

        # 시간 범위 갱신
        if ts < self.ts_min:
            self.ts_min = ts
        if ts > self.ts_max:
            self.ts_max = ts

        bk = bucket_key(ts, self.interval_sec)

        self.bucket_total[bk] += 1
        self.bucket_levels[bk][level] += 1
        self.bucket_nodes[bk][node] += 1

        self.level_totals[level] += 1
        self.node_totals[node] += 1
        self.node_level_totals[node][level] += 1

        # --- 포커스 집계 (accepted stream 기준) ---
        if self.focus_query and self.focus_query in node:
            self.focus_total += 1
            self.focus_levels[level] += 1
            if ts < self.focus_ts_min:
                self.focus_ts_min = ts
            if ts > self.focus_ts_max:
                self.focus_ts_max = ts

            fbk = bucket_key(ts, self.interval_sec)
            self.focus_timeline[fbk] += 1
            self.focus_timeline_levels[fbk][level] += 1

            if (
                level in ('ERROR', 'WARN', 'FATAL')
                and len(self.focus_error_samples) < self.max_focus_error_samples
            ):
                dt_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                clean_msg = RE_ANSI.sub('', line.strip())
                msg_part = _extract_msg_part(clean_msg, node, max_len=220)
                self.focus_error_samples.append((dt_str, node, level, msg_part))

        # 에러 메시지 수집
        if level in ('ERROR', 'WARN', 'FATAL') and len(self.error_messages) < self.max_error_msgs:
            dt_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            clean_msg = RE_ANSI.sub('', line.strip())
            self.error_messages.append((dt_str, node, level, clean_msg[:200]))

    def print_report(self, top_nodes=5):
        if self.parsed_lines == 0:
            print("파싱된 로그 라인이 없습니다.")
            return
        if self.matched_lines == 0:
            print("필터에 매칭되는 로그 라인이 없습니다.")
            return

        sorted_buckets = sorted(self.bucket_total.keys())

        # --- 전체 요약 ---
        print("=" * 90)
        print("  ROS2 AGV 로그 시간대별 분석 보고서")
        print("=" * 90)
        print()
        print(f"  파일 분석 완료")
        print(f"  총 라인 수       : {self.total_lines:>15,}")
        print(f"  파싱 성공        : {self.parsed_lines:>15,}")
        print(f"  파싱 실패        : {self.unparsed_lines:>15,}")
        print(f"  시간 범위        : {datetime.fromtimestamp(self.ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"                   ~ {datetime.fromtimestamp(self.ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
        duration = self.ts_max - self.ts_min
        days = int(duration // 86400)
        hours = int((duration % 86400) // 3600)
        mins = int((duration % 3600) // 60)
        print(f"  총 기간          : {days}일 {hours}시간 {mins}분")
        print(f"  분석 단위        : {format_interval(self.interval_sec)}")
        print()

        # --- 로그 레벨별 전체 통계 ---
        print("-" * 90)
        print("  [로그 레벨별 전체 통계]")
        print("-" * 90)
        level_order = ['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG']
        for lvl in level_order:
            cnt = self.level_totals.get(lvl, 0)
            if cnt > 0:
                pct = cnt / self.matched_lines * 100
                bar = "#" * min(int(pct), 50)
                print(f"  {lvl:<6}  {cnt:>15,}  ({pct:>6.2f}%)  {bar}")
        print()

        # --- 노드별 전체 통계 (상위 N개) ---
        print("-" * 90)
        print(f"  [노드별 전체 통계 - 상위 {top_nodes}개]")
        print("-" * 90)
        sorted_nodes = sorted(self.node_totals.items(), key=lambda x: (-x[1], x[0]))
        max_node_count = sorted_nodes[0][1] if sorted_nodes else 1
        for node, cnt in sorted_nodes[:top_nodes]:
            pct = cnt / self.matched_lines * 100
            bar_len = int(cnt / max_node_count * 40)
            bar = "█" * bar_len
            print(f"  {node:<40} {cnt:>12,}  ({pct:>5.1f}%)  {bar}")
        if len(sorted_nodes) > top_nodes:
            others = sum(c for _, c in sorted_nodes[top_nodes:])
            pct = others / self.matched_lines * 100
            print(f"  {'(기타 ' + str(len(sorted_nodes) - top_nodes) + '개 노드)':<40} {others:>12,}  ({pct:>5.1f}%)")
        print()

        # --- 시간대별 로그 볼륨 ---
        print("-" * 90)
        print("  [시간대별 로그 볼륨]")
        print("-" * 90)
        max_count = max(self.bucket_total.values()) if self.bucket_total else 1
        avg_count = sum(self.bucket_total.values()) / len(self.bucket_total) if self.bucket_total else 0

        for bk in sorted_buckets:
            total = self.bucket_total[bk]
            label = bucket_label(bk, self.interval_sec)
            errors = self.bucket_levels[bk].get('ERROR', 0) + self.bucket_levels[bk].get('FATAL', 0)
            warns = self.bucket_levels[bk].get('WARN', 0)

            bar_len = int(total / max_count * 40)
            bar = "█" * bar_len

            spike = ""
            if avg_count > 0 and total > avg_count * 2:
                spike = " ⚠ SPIKE"

            err_str = ""
            if errors > 0 or warns > 0:
                parts = []
                if errors > 0:
                    parts.append(f"E:{errors}")
                if warns > 0:
                    parts.append(f"W:{warns}")
                err_str = f"  [{', '.join(parts)}]"

            print(f"  {label}  {total:>10,}{err_str:<16} {bar}{spike}")

        print()

        # --- 시간대별 로그 레벨 분포 ---
        print("-" * 90)
        print("  [시간대별 로그 레벨 분포]")
        print("-" * 90)
        header = f"  {'시간대':<20} {'FATAL':>8} {'ERROR':>8} {'WARN':>8} {'INFO':>10} {'DEBUG':>8} {'합계':>10}"
        print(header)
        print("  " + "-" * 78)
        for bk in sorted_buckets:
            label = bucket_label(bk, self.interval_sec)
            levels = self.bucket_levels[bk]
            fatal = levels.get('FATAL', 0)
            error = levels.get('ERROR', 0)
            warn = levels.get('WARN', 0)
            info = levels.get('INFO', 0)
            debug = levels.get('DEBUG', 0)
            total = self.bucket_total[bk]
            print(f"  {label:<20} {fatal:>8,} {error:>8,} {warn:>8,} {info:>10,} {debug:>8,} {total:>10,}")
        print()

        # --- 시간대별 상위 노드 ---
        print("-" * 90)
        print(f"  [시간대별 상위 활동 노드 (Top 3)]")
        print("-" * 90)
        for bk in sorted_buckets:
            label = bucket_label(bk, self.interval_sec)
            nodes = self.bucket_nodes[bk]
            top3 = sorted(nodes.items(), key=lambda x: (-x[1], x[0]))[:3]
            top3_str = ", ".join(f"{n}({c:,})" for n, c in top3)
            print(f"  {label}  {top3_str}")
        print()

        # --- 에러/경고 메시지 요약 ---
        if self.error_messages:
            print("-" * 90)
            print(f"  [ERROR/WARN/FATAL 메시지 샘플 (최대 {self.max_error_msgs}개)]")
            print("-" * 90)

            # 에러 메시지를 유형별로 그룹핑
            error_types = defaultdict(int)
            for dt_str, node, level, msg in self.error_messages:
                # 숫자/변수 부분을 제거하여 패턴별로 그룹핑
                pattern = re.sub(r'\d+\.\d+', 'N', msg)
                pattern = re.sub(r'\b\d+\b', 'N', pattern)
                key = (node, level, pattern[:120])
                error_types[key] += 1

            sorted_errors = sorted(error_types.items(), key=lambda x: x[1], reverse=True)
            for (node, level, pattern), count in sorted_errors[:30]:
                print(f"  [{level}] {node} (x{count})")
                # 원본 메시지에서 첫 번째 예시 찾기
                for dt_str, n, l, msg in self.error_messages:
                    if n == node and l == level:
                        # 타임스탬프 이후 메시지 부분만 추출
                        short_msg = msg[msg.find(']', msg.find(node)) + 1:].strip() if node in msg else msg[-120:]
                        print(f"         예시: {short_msg[:120]}")
                        break
                print()

        # --- 포커스 노드 상세 ---
        if self.focus_query is not None:
            print("-" * 90)
            print("  [포커스 노드 상세]")
            print("-" * 90)
            print(f"  포커스 쿼리: {self.focus_query if self.focus_query else '(빈 문자열)'}")

            if not self.focus_query:
                print("  포커스 쿼리가 비어 있어 상세 집계를 생략합니다.")
                print()
                return

            focus_nodes = [(n, c) for n, c in self.node_totals.items() if self.focus_query in n]
            focus_nodes.sort(key=lambda x: (-x[1], x[0]))
            print(f"  매칭 노드 수: {len(focus_nodes):,}개")
            if focus_nodes:
                top_list = ", ".join(f"{n}({c:,})" for n, c in focus_nodes[:10])
                print(f"  매칭 노드 Top10: {top_list}")

            if self.focus_total == 0:
                print("\n  포커스 노드에 매칭되는 로그가 없습니다.\n")
                return

            # 기본 통계
            err_cnt = self.focus_levels.get('ERROR', 0) + self.focus_levels.get('FATAL', 0)
            warn_cnt = self.focus_levels.get('WARN', 0)
            duration = self.focus_ts_max - self.focus_ts_min

            print()
            print(f"  총 로그 수   : {self.focus_total:>12,}")
            print(f"  시작 시각    : {datetime.fromtimestamp(self.focus_ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  마지막 시각  : {datetime.fromtimestamp(self.focus_ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
            if duration >= 60:
                print(f"  활동 기간    : {int(duration // 3600)}시간 {int((duration % 3600) // 60)}분 {int(duration % 60)}초")
            else:
                print(f"  활동 기간    : {duration:.1f}초")
            if duration > 0:
                print(f"  평균 처리율  : {self.focus_total / duration:.1f} lines/sec")
            print()

            # 레벨 분포
            print("  [포커스 로그 레벨 분포]")
            for lvl in ['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG']:
                cnt = self.focus_levels.get(lvl, 0)
                if cnt > 0:
                    pct = cnt / self.focus_total * 100
                    bar = "█" * min(int(pct), 50)
                    print(f"  {lvl:<6}  {cnt:>12,}  ({pct:>6.2f}%)  {bar}")
            print()

            # 시간대별 타임라인 + 피크
            if self.focus_timeline:
                sorted_bks = sorted(self.focus_timeline.keys())
                max_count = max(self.focus_timeline.values())

                print(f"  [포커스 시간대별 활동 타임라인]  (단위: {format_interval(self.interval_sec)})")
                for bk in sorted_bks:
                    total = self.focus_timeline[bk]
                    label = bucket_label(bk, self.interval_sec)
                    lvls = self.focus_timeline_levels[bk]
                    errors = lvls.get('ERROR', 0) + lvls.get('FATAL', 0)
                    warns = lvls.get('WARN', 0)

                    bar_len = int(total / max_count * 45) if max_count > 0 else 0
                    bar = "█" * bar_len

                    err_str = ""
                    if errors > 0 or warns > 0:
                        parts = []
                        if errors > 0:
                            parts.append(f"E:{errors}")
                        if warns > 0:
                            parts.append(f"W:{warns}")
                        err_str = f"  [{', '.join(parts)}]"

                    print(f"  {label}  {total:>8,}{err_str:<16} {bar}")
                print()

                avg = sum(self.focus_timeline.values()) / len(self.focus_timeline)
                peaks = [(bk, c) for bk, c in self.focus_timeline.items() if avg > 0 and c > avg * 2]
                if peaks:
                    peaks.sort(key=lambda x: x[1], reverse=True)
                    print(f"  포커스 피크 구간 (평균 {avg:,.0f}의 2배 이상):")
                    for bk, c in peaks[:5]:
                        label = bucket_label(bk, self.interval_sec)
                        print(f"    {label}  →  {c:,} ({c / avg:.1f}x)")
                    print()
                else:
                    print(f"  포커스 피크 구간: 없음 (평균 {avg:,.0f})")
                    print()

            # 에러/경고 패턴 요약 (샘플 기반, bounded)
            print(f"  [포커스 에러/경고 메시지 패턴]  (ERROR/FATAL {err_cnt:,} + WARN {warn_cnt:,} 중 샘플 {len(self.focus_error_samples)}건)")
            if not self.focus_error_samples:
                print("  (샘플 없음)")
                print()
                return

            patterns = defaultdict(_FocusPatternInfo)
            for _, node, level, msg in self.focus_error_samples:
                normalized = re.sub(r'\d+\.\d+', 'N', msg)
                normalized = re.sub(r'\b\d+\b', 'N', normalized)
                key = (level, normalized[:100])
                p = patterns[key]
                p.count += 1
                if not p.example:
                    p.example = f"{node}: {msg[:120]}"

            sorted_patterns = sorted(patterns.items(), key=lambda x: x[1].count, reverse=True)
            for (level, prefix), info in sorted_patterns[:20]:
                print(f"  [{level}] x{info.count:,}  {prefix}")
                print(f"         예시: {info.example}")
            print()

    def export_csv(self, filepath):
        """분석 결과를 CSV 파일로 내보냅니다. (하위 호환용)"""
        self.export_csv_reports(output_dir=None, single_file=filepath)

    def export_csv_reports(self, output_dir, single_file=None):
        """실행 날짜 기준 CSV 보고서를 단일 파일로 생성합니다.

        섹션 구분:
          [시간대별 통계]  - 시간대별 로그 볼륨/레벨 분포
          [노드별 통계]    - 노드별 통계
          [에러 목록]      - ERROR/WARN/FATAL 메시지 목록
        """
        now_str = datetime.now().strftime('%Y-%m-%d_%H%M%S')

        if single_file:
            filepath = single_file
        else:
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, f"log_report_{now_str}.csv")

        sorted_buckets = sorted(self.bucket_total.keys())

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # --- 섹션 1: 시간대별 통계 ---
            writer.writerow(['[시간대별 통계]'])
            writer.writerow([
                'time_bucket', 'total', 'fatal', 'error', 'warn', 'info', 'debug',
                'top_node_1', 'top_node_1_count',
                'top_node_2', 'top_node_2_count',
                'top_node_3', 'top_node_3_count',
            ])
            for bk in sorted_buckets:
                label = bucket_label(bk, self.interval_sec)
                levels = self.bucket_levels[bk]
                nodes = self.bucket_nodes[bk]
                top3 = sorted(nodes.items(), key=lambda x: (-x[1], x[0]))[:3]
                row = [
                    label,
                    self.bucket_total[bk],
                    levels.get('FATAL', 0),
                    levels.get('ERROR', 0),
                    levels.get('WARN', 0),
                    levels.get('INFO', 0),
                    levels.get('DEBUG', 0),
                ]
                for i in range(3):
                    if i < len(top3):
                        row.extend([top3[i][0], top3[i][1]])
                    else:
                        row.extend(['', 0])
                writer.writerow(row)

            # --- 섹션 2: 노드별 통계 ---
            sorted_nodes = sorted(self.node_totals.items(), key=lambda x: (-x[1], x[0]))
            if sorted_nodes:
                writer.writerow([])
                writer.writerow(['[노드별 통계]'])
                writer.writerow(['rank', 'node', 'total_count', 'percent',
                                 'fatal', 'error', 'warn', 'info', 'debug'])
                for rank, (node, cnt) in enumerate(sorted_nodes, 1):
                    pct = f"{cnt / self.matched_lines * 100:.2f}" if self.matched_lines > 0 else "0"
                    nl = self.node_level_totals.get(node, {})
                    writer.writerow([
                        rank, node, cnt, pct,
                        nl.get('FATAL', 0),
                        nl.get('ERROR', 0),
                        nl.get('WARN', 0),
                        nl.get('INFO', 0),
                        nl.get('DEBUG', 0),
                    ])

            # --- 섹션 3: 에러 목록 ---
            if self.error_messages:
                writer.writerow([])
                writer.writerow(['[에러 목록]'])
                writer.writerow(['timestamp', 'level', 'node', 'message'])
                for dt_str, node, level, msg in self.error_messages:
                    clean = RE_ANSI.sub('', msg)
                    writer.writerow([dt_str, level, node, clean])

        print(f"  CSV 저장: {filepath}")
        print()


def format_interval(sec):
    if sec >= 3600:
        return f"{sec // 3600}시간"
    elif sec >= 60:
        return f"{sec // 60}분"
    else:
        return f"{sec}초"


# =============================================================================
#  체크포인트 (증분 분석)
# =============================================================================

def _get_report_dir():
    """현재 작업 디렉토리 기준 reports/ 경로를 반환합니다."""
    return os.path.join(os.getcwd(), 'reports')


def _checkpoint_path(logfile):
    """로그 파일에 대응하는 체크포인트 파일 경로를 반환합니다."""
    return os.path.join(_get_report_dir(), '.checkpoint.json')


def load_checkpoint(logfile):
    """저장된 체크포인트를 로드합니다. 없으면 None 반환."""
    cp_path = _checkpoint_path(logfile)
    if not os.path.exists(cp_path):
        return None
    try:
        with open(cp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        abs_log = os.path.abspath(logfile)
        cp = data.get(abs_log)
        if not isinstance(cp, dict):
            return None

        offset = cp.get('offset')
        file_size = cp.get('file_size')
        if not isinstance(offset, int) or not isinstance(file_size, int):
            return None
        if offset < 0 or file_size < 0:
            return None
        return cp
    except (json.JSONDecodeError, OSError):
        return None


def save_checkpoint(logfile, offset, total_lines, last_ts, file_size):
    """분석 완료 후 체크포인트를 저장합니다."""
    cp_path = _checkpoint_path(logfile)
    os.makedirs(os.path.dirname(cp_path), exist_ok=True)

    # 기존 체크포인트 로드
    data = {}
    if os.path.exists(cp_path):
        try:
            with open(cp_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {}

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 메타데이터 (레거시 호환: 기존 엔트리들은 그대로 유지)
    if isinstance(data, dict):
        data.setdefault('schema_version', 2)
        data.setdefault('created_at', now_str)
        data['updated_at'] = now_str

    abs_log = os.path.abspath(logfile)
    data[abs_log] = {
        'offset': offset,
        'total_lines': total_lines,
        'last_ts': last_ts,
        'file_size': file_size,
        'analyzed_at': now_str,
    }

    with open(cp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def clear_checkpoint(logfile):
    """체크포인트를 삭제합니다."""
    cp_path = _checkpoint_path(logfile)
    if not os.path.exists(cp_path):
        return
    try:
        with open(cp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        abs_log = os.path.abspath(logfile)
        if abs_log in data:
            del data[abs_log]
            with open(cp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
    except (json.JSONDecodeError, OSError):
        pass


def _rewind_to_line_start_if_needed(fb, start_offset, cap_bytes=1024 * 1024):
    """Rewind to a safe line boundary for a binary file handle.

    - If start_offset is already on a line boundary (previous byte is '\n'), keep it.
    - If start_offset is in the middle of a line, scan backwards (up to cap_bytes)
      and return the offset right after the previous '\n'.
    - If no '\n' is found within the cap, fall back to 0.
    """

    if start_offset <= 0:
        return 0

    try:
        fb.seek(start_offset - 1)
        prev = fb.read(1)
    except OSError:
        return 0

    if prev == b'\n':
        return start_offset

    to_scan = min(int(cap_bytes), int(start_offset))
    scan_start = start_offset - to_scan
    try:
        fb.seek(scan_start)
        buf = fb.read(to_scan)
    except OSError:
        return 0

    idx = buf.rfind(b'\n')
    if idx < 0:
        return 0
    return scan_start + idx + 1


def _effective_scan_end(fb, file_size, cap_bytes=1024 * 1024):
    """Return the byte offset up to which we can safely process full lines.

    If the file does not end with a newline, we exclude the trailing partial line
    and return the offset right after the last '\n' (or 0 if none found within cap).
    """

    if file_size <= 0:
        return 0

    try:
        fb.seek(file_size - 1)
        last = fb.read(1)
    except OSError:
        return 0

    if last == b'\n':
        return file_size

    to_scan = min(int(cap_bytes), int(file_size))
    scan_start = file_size - to_scan
    try:
        fb.seek(scan_start)
        buf = fb.read(to_scan)
    except OSError:
        return 0

    idx = buf.rfind(b'\n')
    if idx < 0:
        return 0
    return scan_start + idx + 1


# =============================================================================
#  실시간 모니터링 (--follow / -f)
# =============================================================================

# ANSI 터미널 제어 코드
TERM_CLEAR = '\033[2J'
TERM_HOME = '\033[H'
TERM_BOLD = '\033[1m'
TERM_RESET = '\033[0m'
TERM_RED = '\033[91m'
TERM_YELLOW = '\033[93m'
TERM_GREEN = '\033[92m'
TERM_CYAN = '\033[96m'
TERM_DIM = '\033[2m'
TERM_HIDE_CURSOR = '\033[?25l'
TERM_SHOW_CURSOR = '\033[?25h'


def get_terminal_size():
    try:
        cols, rows = os.get_terminal_size()
        return rows, cols
    except OSError:
        return 40, 120


class LiveMonitor:
    """실시간 로그 모니터링 대시보드"""

    def __init__(self, interval_sec, window_sec, node_filter=None, errors_only=False, focus_query=None):
        self.interval_sec = interval_sec
        self.window_sec = window_sec
        self.node_filter = node_filter
        self.errors_only = errors_only
        self.focus_query = focus_query

        # 롤링 윈도우: (timestamp, node, level) 튜플을 저장
        self.events = deque()

        # 누적 통계
        self.total_lines = 0
        self.parsed_lines = 0
        self.matched_lines = 0
        self.total_levels = defaultdict(int)
        self.total_nodes = defaultdict(int)
        self.node_level_totals = defaultdict(lambda: defaultdict(int))  # node -> {level: count}

        # 초당 처리율 계산용 (최근 N초를 초 단위로 집계)
        self.rate_window = deque()  # (sec, count)
        self.rate_window_secs = 10

        # 최근 에러/경고 메시지 (화면에 표시할 최근 N개)
        self.recent_alerts = deque(maxlen=20)

        # 포커스 노드(부분 매칭) - window + cumulative, bounded
        self.focus_events = deque()  # (timestamp, level) only for focus-matched events
        self.focus_win_total = 0
        self.focus_win_levels = defaultdict(int)
        self.focus_total = 0
        self.focus_total_levels = defaultdict(int)
        self.focus_recent_alerts = deque(maxlen=20)  # (dt,node,level,msg)

        # 시작 시간
        self.start_wall = time.time()
        self.start_ts = None
        self.latest_ts = None

    def process_line(self, line):
        self.total_lines += 1

        result = parse_line(line)
        if result is None:
            return None

        ts, node, level = result
        self.parsed_lines += 1

        if self.start_ts is None:
            self.start_ts = ts

        # 필터 적용
        if self.node_filter and self.node_filter not in node:
            return None
        if self.errors_only and level not in ('ERROR', 'WARN', 'FATAL'):
            return None

        self.matched_lines += 1

        is_focus = bool(self.focus_query) and (self.focus_query in node)

        # accepted 이벤트의 최신 타임스탬프는 단조 증가로 유지
        if self.latest_ts is None or ts > self.latest_ts:
            self.latest_ts = ts

        # 롤링 윈도우에 추가
        self.events.append((ts, node, level))

        # 윈도우 만료는 렌더링 주기와 무관하게 여기서 수행
        cutoff = ts - self.window_sec
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

        if self.focus_query:
            if is_focus:
                self.focus_events.append((ts, level))
                self.focus_win_total += 1
                self.focus_win_levels[level] += 1
                self.focus_total += 1
                self.focus_total_levels[level] += 1

            while self.focus_events and self.focus_events[0][0] < cutoff:
                _, old_level = self.focus_events.popleft()
                self.focus_win_total -= 1
                self.focus_win_levels[old_level] -= 1
                if self.focus_win_levels[old_level] <= 0:
                    del self.focus_win_levels[old_level]

        # 누적 통계
        self.total_levels[level] += 1
        self.total_nodes[node] += 1
        self.node_level_totals[node][level] += 1

        # 초당 처리율 (초 단위로 집계, bounded)
        now = time.time()
        sec = int(now)
        if self.rate_window and self.rate_window[-1][0] == sec:
            prev_sec, prev_cnt = self.rate_window[-1]
            self.rate_window[-1] = (prev_sec, prev_cnt + 1)
        else:
            self.rate_window.append((sec, 1))

        cutoff_sec = sec - self.rate_window_secs
        while self.rate_window and self.rate_window[0][0] < cutoff_sec:
            self.rate_window.popleft()

        # 에러/경고 알림 수집
        if level in ('ERROR', 'WARN', 'FATAL'):
            dt_str = datetime.fromtimestamp(ts).strftime('%H:%M:%S')
            clean_msg = RE_ANSI.sub('', line.strip())
            # 노드명 뒤 메시지 부분 추출
            after_node = clean_msg.find(']', clean_msg.find(node))
            if after_node >= 0:
                msg_part = clean_msg[after_node + 1:].strip()
            else:
                msg_part = clean_msg[-150:]
            self.recent_alerts.append((dt_str, node, level, msg_part[:120]))
            if is_focus:
                self.focus_recent_alerts.append((dt_str, node, level, msg_part[:120]))

        return (ts, node, level)

    def _calc_rate(self):
        """초당 로그 처리율 계산"""
        if not self.rate_window:
            return 0.0
        total = sum(c for _, c in self.rate_window)
        span = max(1, self.rate_window[-1][0] - self.rate_window[0][0] + 1)
        return total / span

    def render_dashboard(self, latest_ts):
        """현재 상태를 터미널 대시보드로 렌더링"""
        rows, cols = get_terminal_size()
        # window/rate 만료는 process_line에서 처리 (render cadence 비의존)

        lines = []
        w = min(cols, 130)
        now_dt = datetime.fromtimestamp(latest_ts) if latest_ts else datetime.now()
        wall_elapsed = time.time() - self.start_wall

        # --- 헤더 ---
        lines.append(f"{TERM_BOLD}{TERM_CYAN}{'═' * w}{TERM_RESET}")
        title = "ROS2 AGV 실시간 로그 모니터"
        quit_hint = "Ctrl+C: 종료"
        pad = w - len(title) - len(quit_hint) - 4
        lines.append(f"{TERM_BOLD}{TERM_CYAN}  {title}{' ' * max(pad, 1)}{TERM_DIM}{quit_hint}{TERM_RESET}")
        lines.append(f"{TERM_BOLD}{TERM_CYAN}{'═' * w}{TERM_RESET}")

        # --- 상태 바 ---
        rate = self._calc_rate()
        win_count = len(self.events)
        elapsed_str = f"{int(wall_elapsed // 60)}분 {int(wall_elapsed % 60)}초"

        filters = []
        if self.node_filter:
            filters.append(f"노드={self.node_filter}")
        if self.focus_query:
            filters.append(f"포커스={self.focus_query}")
        if self.errors_only:
            filters.append("에러만")
        filter_str = f"  필터: {', '.join(filters)}" if filters else ""

        lines.append(
            f"  로그 시각: {TERM_BOLD}{now_dt.strftime('%Y-%m-%d %H:%M:%S')}{TERM_RESET}"
            f"  |  윈도우: {format_interval(self.window_sec)}"
            f"  |  경과: {elapsed_str}"
            f"  |  처리율: {TERM_GREEN}{rate:,.0f}{TERM_RESET} lines/s"
            f"{filter_str}"
        )
        lines.append("")

        # --- 윈도우 내 로그 레벨 요약 ---
        win_levels = defaultdict(int)
        win_nodes = defaultdict(int)
        win_buckets = defaultdict(lambda: defaultdict(int))

        for ts, node, level in self.events:
            win_levels[level] += 1
            win_nodes[node] += 1
            bk = bucket_key(ts, self.interval_sec)
            win_buckets[bk][level] += 1

        total_in_win = sum(win_levels.values())

        lines.append(f"  {TERM_BOLD}[윈도우 내 통계]  총 {total_in_win:,}건  (최근 {format_interval(self.window_sec)}){TERM_RESET}")
        lines.append(f"  {'─' * (w - 4)}")

        level_parts = []
        for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
            cnt = win_levels.get(lvl, 0)
            if cnt > 0:
                level_parts.append(f"{color}{lvl}: {cnt:,}{TERM_RESET}")
        lines.append(f"  레벨:  {'  |  '.join(level_parts) if level_parts else '(없음)'}")

        # 누적 통계 한 줄
        cum_parts = []
        for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
            cnt = self.total_levels.get(lvl, 0)
            if cnt > 0:
                cum_parts.append(f"{color}{lvl}: {cnt:,}{TERM_RESET}")
        lines.append(f"  누적:  {'  |  '.join(cum_parts)}  {TERM_DIM}(전체 {self.parsed_lines:,}줄){TERM_RESET}")
        lines.append("")

        # --- 윈도우 내 시간대별 바 차트 ---
        if win_buckets:
            sorted_bks = sorted(win_buckets.keys())
            # 최근 버킷들만 (화면에 맞게)
            max_bars = min(rows - 25, 15)  # 대시보드 다른 요소 공간 확보
            if len(sorted_bks) > max_bars:
                sorted_bks = sorted_bks[-max_bars:]

            lines.append(f"  {TERM_BOLD}[시간대별 로그 볼륨]{TERM_RESET}  (단위: {format_interval(self.interval_sec)})")
            lines.append(f"  {'─' * (w - 4)}")

            bucket_totals = {bk: sum(win_buckets[bk].values()) for bk in sorted_bks}
            max_bt = max(bucket_totals.values()) if bucket_totals else 1

            for bk in sorted_bks:
                bt = bucket_totals[bk]
                label = bucket_label(bk, self.interval_sec)
                errors = win_buckets[bk].get('ERROR', 0) + win_buckets[bk].get('FATAL', 0)
                warns = win_buckets[bk].get('WARN', 0)

                bar_space = w - 45
                bar_len = int(bt / max_bt * bar_space) if max_bt > 0 else 0
                bar = "█" * bar_len

                err_str = ""
                if errors > 0 or warns > 0:
                    parts = []
                    if errors > 0:
                        parts.append(f"{TERM_RED}E:{errors}{TERM_RESET}")
                    if warns > 0:
                        parts.append(f"{TERM_YELLOW}W:{warns}{TERM_RESET}")
                    err_str = f" [{', '.join(parts)}]"

                lines.append(f"  {label}  {bt:>8,}{err_str:>20} {TERM_GREEN}{bar}{TERM_RESET}")
            lines.append("")

        # --- 윈도우 내 상위 노드 ---
        if win_nodes:
            sorted_wn = sorted(win_nodes.items(), key=lambda x: (-x[1], x[0]))[:7]
            max_wn = sorted_wn[0][1] if sorted_wn else 1

            lines.append(f"  {TERM_BOLD}[활동 노드 Top {min(7, len(sorted_wn))}]{TERM_RESET}")
            lines.append(f"  {'─' * (w - 4)}")

            for node, cnt in sorted_wn:
                bar_space = w - 65
                bar_len = int(cnt / max_wn * bar_space) if max_wn > 0 else 0
                bar = "▓" * bar_len
                pct = cnt / total_in_win * 100 if total_in_win > 0 else 0
                lines.append(f"  {node:<40} {cnt:>8,}  ({pct:>5.1f}%)  {TERM_CYAN}{bar}{TERM_RESET}")
            lines.append("")

        # --- 포커스 패널 ---
        if self.focus_query:
            lines.append(f"  {TERM_BOLD}[포커스 노드]{TERM_RESET}  쿼리='{self.focus_query}'")
            lines.append(f"  {'─' * (w - 4)}")
            if self.focus_total <= 0:
                lines.append("  (아직 매칭 로그 없음)")
                lines.append("")
            else:
                win_parts = []
                for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
                    cnt = self.focus_win_levels.get(lvl, 0)
                    if cnt > 0:
                        win_parts.append(f"{color}{lvl}:{cnt:,}{TERM_RESET}")
                lines.append(
                    f"  윈도우: 총 {self.focus_win_total:,}건"
                    f"  |  {'  '.join(win_parts) if win_parts else '(없음)'}"
                )

                cum_parts = []
                for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
                    cnt = self.focus_total_levels.get(lvl, 0)
                    if cnt > 0:
                        cum_parts.append(f"{color}{lvl}:{cnt:,}{TERM_RESET}")
                lines.append(
                    f"  누적  : 총 {self.focus_total:,}건"
                    f"  |  {'  '.join(cum_parts) if cum_parts else '(없음)'}"
                )

                if self.focus_recent_alerts:
                    available_rows = max(rows - len(lines) - 3, 0)
                    alert_count = min(8, len(self.focus_recent_alerts), available_rows)
                    if alert_count > 0:
                        lines.append(f"  {TERM_BOLD}[포커스 최근 에러/경고]{TERM_RESET}")
                        recent = list(self.focus_recent_alerts)[-alert_count:]
                        for dt_str, node, level, msg in recent:
                            if level == 'FATAL':
                                color = TERM_RED + TERM_BOLD
                            elif level == 'ERROR':
                                color = TERM_RED
                            else:
                                color = TERM_YELLOW

                            max_msg_len = w - len(dt_str) - len(node) - len(level) - 18
                            truncated = msg[:max_msg_len] + "…" if len(msg) > max_msg_len else msg
                            lines.append(f"  {TERM_DIM}{dt_str}{TERM_RESET} {color}[{level}]{TERM_RESET} {TERM_BOLD}{node}{TERM_RESET} {truncated}")
                lines.append("")

        # --- 최근 에러/경고 알림 ---
        if self.recent_alerts:
            available_rows = max(rows - len(lines) - 3, 3)
            alert_count = min(20, len(self.recent_alerts), available_rows)
            lines.append(f"  {TERM_BOLD}[최근 에러/경고]{TERM_RESET}")
            lines.append(f"  {'─' * (w - 4)}")

            recent = list(self.recent_alerts)[-alert_count:]
            for dt_str, node, level, msg in recent:
                if level == 'FATAL':
                    color = TERM_RED + TERM_BOLD
                elif level == 'ERROR':
                    color = TERM_RED
                else:
                    color = TERM_YELLOW

                max_msg_len = w - len(dt_str) - len(node) - len(level) - 14
                truncated = msg[:max_msg_len] + "…" if len(msg) > max_msg_len else msg
                lines.append(f"  {TERM_DIM}{dt_str}{TERM_RESET} {color}[{level}]{TERM_RESET} {TERM_BOLD}{node}{TERM_RESET} {truncated}")

        # --- 푸터 ---
        lines.append("")
        lines.append(f"{TERM_DIM}{'─' * w}{TERM_RESET}")

        return '\n'.join(lines)


def tail_seek(filepath, num_lines):
    """파일 끝에서 num_lines 줄 앞 위치(byte offset)를 반환합니다."""
    with open(filepath, 'rb') as f:
        f.seek(0, 2)  # EOF
        file_size = f.tell()

        if file_size == 0:
            return 0

        # 끝에서부터 역방향 탐색
        block_size = 8192
        found_lines = 0
        pos = file_size

        while pos > 0 and found_lines <= num_lines:
            read_size = min(block_size, pos)
            pos -= read_size
            f.seek(pos)
            block = f.read(read_size)
            found_lines += block.count(b'\n')

        if found_lines <= num_lines:
            return 0

        # 정확한 위치 찾기: pos부터 읽어서 num_lines 줄 전까지 스킵
        f.seek(pos)
        skip = found_lines - num_lines
        for _ in range(skip):
            f.readline()
        return f.tell()


def run_follow_mode(args, interval_sec, window_sec):
    """실시간 모니터링 모드 실행"""
    monitor = LiveMonitor(
        interval_sec=interval_sec,
        window_sec=window_sec,
        node_filter=args.node,
        errors_only=args.errors_only,
        focus_query=args.focus_node,
    )

    filepath = args.logfile
    tail_lines = args.tail
    refresh_sec = args.refresh

    # 터미널 준비
    sys.stdout.write(TERM_HIDE_CURSOR)
    sys.stdout.flush()

    # Ctrl+C 시 커서 복원
    original_sigint = signal.getsignal(signal.SIGINT)

    def cleanup_handler(signum, frame):
        sys.stdout.write(TERM_SHOW_CURSOR + '\n')
        sys.stdout.flush()
        signal.signal(signal.SIGINT, original_sigint)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, cleanup_handler)

    # 시작 위치 결정
    def _open_follow_file():
        fb = open(filepath, 'rb')
        st = os.fstat(fb.fileno())
        return fb, st

    def _seek_follow_start(fb):
        if tail_lines > 0:
            start_offset = tail_seek(filepath, tail_lines)
        else:
            # 기본: 파일 끝에서 시작
            start_offset = os.path.getsize(filepath)

        safe_start = _rewind_to_line_start_if_needed(fb, start_offset)
        fb.seek(safe_start)
        # 시작 지점이 줄 중간일 때만 첫 줄 버림
        if safe_start < start_offset:
            fb.readline()

    last_render = 0
    last_eof_check = 0
    eof_check_interval = 1.0

    try:
        fb, fst = _open_follow_file()
        try:
            _seek_follow_start(fb)

            while True:
                pos_before = fb.tell()
                raw = fb.readline()

                if raw:
                    # 정상 라인만 처리 (부분 라인은 다음 read에서 이어서 처리)
                    if not raw.endswith(b'\n'):
                        fb.seek(pos_before)
                        time.sleep(0.05)
                    else:
                        line = raw.decode('utf-8', errors='replace')
                        monitor.process_line(line)

                    # 대시보드 갱신 (refresh 간격마다)
                    now = time.time()
                    if now - last_render >= refresh_sec:
                        dashboard = monitor.render_dashboard(monitor.latest_ts or time.time())
                        sys.stdout.write(TERM_CLEAR + TERM_HOME + dashboard + '\n')
                        sys.stdout.flush()
                        last_render = now
                    continue

                # EOF
                time.sleep(0.2)
                now = time.time()

                # 주기적으로 truncation/rotation 체크
                if now - last_eof_check >= eof_check_interval:
                    last_eof_check = now
                    try:
                        st = os.stat(filepath)
                    except OSError:
                        st = None

                    reopened = False

                    # inode/dev 변경(로테이션) 감지
                    if st is not None and (st.st_ino != fst.st_ino or st.st_dev != fst.st_dev):
                        fb.close()
                        fb, fst = _open_follow_file()
                        _seek_follow_start(fb)
                        reopened = True

                    # truncation 감지: 파일 크기가 현재 위치보다 작아짐
                    if not reopened:
                        try:
                            cur_pos = fb.tell()
                            cur_size = os.path.getsize(filepath)
                        except OSError:
                            cur_pos = None
                            cur_size = None

                        if cur_pos is not None and cur_size is not None and cur_size < cur_pos:
                            if tail_lines > 0:
                                _seek_follow_start(fb)
                            else:
                                fb.seek(0)

                # 대기 중에도 대시보드 갱신
                if now - last_render >= refresh_sec:
                    dashboard = monitor.render_dashboard(monitor.latest_ts or time.time())
                    sys.stdout.write(TERM_CLEAR + TERM_HOME + dashboard + '\n')
                    sys.stdout.flush()
                    last_render = now

        finally:
            try:
                fb.close()
            except Exception:
                pass

    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write(TERM_SHOW_CURSOR)
        sys.stdout.flush()

    # 종료 시 최종 요약 출력
    print(f"\n\n  실시간 모니터링 종료")
    print(f"  총 처리 라인: {monitor.total_lines:,}")
    print(f"  파싱 성공: {monitor.parsed_lines:,}")
    duration = time.time() - monitor.start_wall
    print(f"  모니터링 시간: {int(duration // 60)}분 {int(duration % 60)}초")

    if monitor.total_levels:
        print(f"\n  누적 로그 레벨:")
        for lvl in ['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG']:
            cnt = monitor.total_levels.get(lvl, 0)
            if cnt > 0:
                print(f"    {lvl:<6} : {cnt:>12,}")

    # 실시간 모니터링 결과도 CSV로 저장
    if monitor.parsed_lines > 0:
        report_dir = _get_report_dir()
        os.makedirs(report_dir, exist_ok=True)

        now_str = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        filepath = os.path.join(report_dir, f"live_report_{now_str}.csv")

        # 윈도우 내 이벤트를 버킷으로 집계
        bk_total = defaultdict(int)
        bk_levels = defaultdict(lambda: defaultdict(int))
        bk_nodes = defaultdict(lambda: defaultdict(int))
        for ts, node, level in monitor.events:
            bk = bucket_key(ts, monitor.interval_sec)
            bk_total[bk] += 1
            bk_levels[bk][level] += 1
            bk_nodes[bk][node] += 1

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # --- 섹션 1: 시간대별 통계 ---
            writer.writerow(['[시간대별 통계]'])
            writer.writerow([
                'time_bucket', 'total', 'fatal', 'error', 'warn', 'info', 'debug',
                'top_node_1', 'top_node_1_count',
                'top_node_2', 'top_node_2_count',
                'top_node_3', 'top_node_3_count',
            ])
            for bk in sorted(bk_total.keys()):
                label = bucket_label(bk, monitor.interval_sec)
                lvls = bk_levels[bk]
                top3 = sorted(bk_nodes[bk].items(), key=lambda x: (-x[1], x[0]))[:3]
                row = [label, bk_total[bk],
                       lvls.get('FATAL', 0), lvls.get('ERROR', 0),
                       lvls.get('WARN', 0), lvls.get('INFO', 0), lvls.get('DEBUG', 0)]
                for i in range(3):
                    if i < len(top3):
                        row.extend([top3[i][0], top3[i][1]])
                    else:
                        row.extend(['', 0])
                writer.writerow(row)

            # --- 섹션 2: 노드별 통계 ---
            sorted_nodes = sorted(monitor.total_nodes.items(), key=lambda x: (-x[1], x[0]))
            if sorted_nodes:
                writer.writerow([])
                writer.writerow(['[노드별 통계]'])
                writer.writerow(['rank', 'node', 'total_count', 'percent',
                                 'fatal', 'error', 'warn', 'info', 'debug'])
                for rank, (node, cnt) in enumerate(sorted_nodes, 1):
                    pct = f"{cnt / monitor.matched_lines * 100:.2f}" if monitor.matched_lines > 0 else "0"
                    nl = monitor.node_level_totals.get(node, {})
                    writer.writerow([
                        rank, node, cnt, pct,
                        nl.get('FATAL', 0), nl.get('ERROR', 0),
                        nl.get('WARN', 0), nl.get('INFO', 0), nl.get('DEBUG', 0)
                    ])

            # --- 섹션 3: 에러 목록 ---
            if monitor.recent_alerts:
                writer.writerow([])
                writer.writerow(['[에러 목록]'])
                writer.writerow(['timestamp', 'level', 'node', 'message'])
                for dt_str, node, level, msg in monitor.recent_alerts:
                    writer.writerow([dt_str, level, node, RE_ANSI.sub('', msg)])

        print(f"\n  CSV 저장: {filepath}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description='ROS2 AGV launch.log 시간대별 분석기',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시 (정적 분석 - 증분):
  python3 analyze_log.py launch.log                     # 증분 분석 (새 내용만)
  python3 analyze_log.py launch.log --full              # 전체 재분석
  python3 analyze_log.py launch.log --interval 10m      # 10분 단위
  python3 analyze_log.py launch.log --top-nodes 10      # 상위 10개 노드
  python3 analyze_log.py launch.log --errors-only       # 에러/경고만
  python3 analyze_log.py launch.log --node motor_driver  # 특정 노드 필터
  python3 analyze_log.py launch.log --csv report.csv    # CSV 내보내기

예시 (시간 범위 지정):
  python3 analyze_log.py launch.log --from "2026-01-27" --to "2026-01-28"
  python3 analyze_log.py launch.log --from "2026-01-27 09:00" --to "2026-01-27 18:00"
  python3 analyze_log.py launch.log --from "09:00" --to "12:00"    # 오늘 기준
  python3 analyze_log.py launch.log --from "2026-01-27" -i 10m -e  # 조합

예시 (실시간 모니터링):
  python3 analyze_log.py launch.log -f                  # 실시간 대시보드
  python3 analyze_log.py launch.log -f --window 10m     # 최근 10분 윈도우
  python3 analyze_log.py launch.log -f --refresh 1      # 1초마다 갱신
  python3 analyze_log.py launch.log -f --tail 5000      # 최근 5000줄부터
  python3 analyze_log.py launch.log -f -e               # 에러만 실시간 추적
  python3 analyze_log.py launch.log -f -n motor_driver  # 특정 노드 실시간
        """
    )
    parser.add_argument('logfile', nargs='?', help='분석할 로그 파일 경로')
    parser.add_argument('--interactive', '-I', action='store_true',
                        help='인터랙티브 마법사로 옵션 선택 (TTY에서 logfile 미지정시 자동)')
    parser.add_argument('--interval', '-i', default='1h',
                        help='분석 시간 단위 (예: 1h, 30m, 10m, 1m, 30s) [기본: 1h]')
    parser.add_argument('--top-nodes', '-t', type=int, default=10,
                        help='상위 노드 표시 개수 [기본: 10]')
    parser.add_argument('--errors-only', '-e', action='store_true',
                        help='ERROR/WARN/FATAL 로그만 분석')
    parser.add_argument('--node', '-n', default=None,
                        help='특정 노드만 필터링 (부분 매칭)')
    parser.add_argument('--focus-node', default=None,
                        help='포커스 노드 상세 분석/모니터링 (노드명 부분 매칭, 기존 필터 적용 후)')
    parser.add_argument('--csv', '-c', default=None,
                        help='CSV 파일로 결과 내보내기')

    # 시간 범위 필터
    parser.add_argument('--from', dest='time_from', default=None,
                        help='분석 시작 시각 (예: "2026-01-27", "2026-01-27 09:00", "09:00")')
    parser.add_argument('--to', dest='time_to', default=None,
                        help='분석 종료 시각 (예: "2026-01-28", "2026-01-27 18:00", "18:00")')

    # 증분/전체 분석 옵션
    parser.add_argument('--full', action='store_true',
                        help='전체 재분석 (체크포인트 무시)')

    # 실시간 모니터링 옵션
    parser.add_argument('--follow', '-f', action='store_true',
                        help='실시간 모니터링 모드 (tail -f 방식)')
    parser.add_argument('--window', '-w', default='5m',
                        help='실시간 모드 롤링 윈도우 크기 [기본: 5m]')
    parser.add_argument('--refresh', '-r', type=float, default=2.0,
                        help='실시간 모드 대시보드 갱신 주기(초) [기본: 2]')
    parser.add_argument('--tail', type=int, default=5000,
                        help='실시간 모드 시작 시 읽어올 마지막 줄 수 [기본: 5000, 0=파일 끝부터]')

    args = parser.parse_args()

    def _is_tty() -> bool:
        try:
            return sys.stdin.isatty() and sys.stdout.isatty()
        except Exception:
            return False

    def _prompt_line(prompt: str, default=None) -> str:
        if default is None or default == '':
            suffix = ''
        else:
            suffix = f" [{default}]"
        s = input(f"{prompt}{suffix}: ").strip()
        if s == '':
            return '' if default is None else str(default)
        return s

    def _prompt_bool(prompt: str, default: bool = False) -> bool:
        guide = 'Y/n' if default else 'y/N'
        while True:
            s = input(f"{prompt} ({guide}): ").strip().lower()
            if s == '':
                return default
            if s in ('y', 'yes', '1', 'true', 't'):
                return True
            if s in ('n', 'no', '0', 'false', 'f'):
                return False
            print("  y/n 로 입력하세요")

    def _prompt_int(prompt: str, default=None, min_value=None):
        while True:
            s = _prompt_line(prompt, default=default)
            if s == '' and default is None:
                return None
            try:
                v = int(s)
            except ValueError:
                print("  숫자를 입력하세요")
                continue
            if min_value is not None and v < min_value:
                print(f"  {min_value} 이상으로 입력하세요")
                continue
            return v

    def _prompt_float(prompt: str, default=None, min_value=None):
        while True:
            s = _prompt_line(prompt, default=default)
            if s == '' and default is None:
                return None
            try:
                v = float(s)
            except ValueError:
                print("  숫자를 입력하세요")
                continue
            if min_value is not None and v < min_value:
                print(f"  {min_value} 이상으로 입력하세요")
                continue
            return v

    def _prompt_interval(prompt: str, default: str) -> str:
        while True:
            s = _prompt_line(prompt, default=default).strip()
            if s == '':
                return default
            try:
                parse_interval(s)
                return s
            except ValueError as e:
                print(f"  {e}")

    def _prompt_datetime_optional(prompt: str, default=None):
        while True:
            s = _prompt_line(prompt, default=default).strip()
            if s == '':
                return None
            try:
                parse_datetime_arg(s)
                return s
            except ValueError as e:
                print(f"  {e}")

    def _prompt_existing_file(prompt: str, default=None) -> str:
        while True:
            s = _prompt_line(prompt, default=default).strip()
            if s == '':
                if default:
                    s = str(default)
                else:
                    print("  파일 경로를 입력하세요")
                    continue
            p = os.path.expanduser(s)
            if os.path.isfile(p):
                return p
            print(f"  파일을 찾을 수 없습니다: {p}")

    def _logfile_candidates(default=None):
        """Shallow logfile discovery: cwd + ./log only."""
        out = []

        def _add(p):
            if not p:
                return
            p = os.path.expanduser(str(p))
            if os.path.isfile(p) and p not in out:
                out.append(p)

        _add(default)
        _add('launch.log')
        for p in sorted(glob.glob('*.log')):
            _add(p)
        for p in sorted(glob.glob(os.path.join('log', '*.log'))):
            _add(p)
        return out

    def _logfile_meta_brief(p: str) -> str:
        try:
            st = os.stat(p)
            size_mb = st.st_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M')
            return f"{size_mb:6.1f}MB {mtime}"
        except OSError:
            return ""

    def _pick_logfile_curses(candidates):
        """Return selected path; None => manual input/fallback."""
        if not _is_tty():
            return None
        if os.environ.get('TERM', '').lower() in ('', 'dumb'):
            return None

        try:
            import curses  # stdlib; may be missing on some platforms
        except Exception:
            return None

        def _safe_addnstr(stdscr, y, x, s, n):
            try:
                stdscr.addnstr(y, x, s, n)
            except Exception:
                return

        def _browse_files(stdscr, start_dir: str):
            # Returns selected file path or None (manual input)
            cur_dir = os.path.abspath(start_dir or '.')
            idx = 0
            top = 0
            msg = ""

            def _list_dir(d: str):
                nonlocal msg
                try:
                    names = os.listdir(d)
                except OSError as e:
                    msg = f"디렉터리를 열 수 없습니다: {e}"
                    return [], [], []

                dirs = []
                log_files = []
                other_files = []
                for name in names:
                    if name in ('.', '..'):
                        continue
                    full = os.path.join(d, name)
                    try:
                        if os.path.isdir(full):
                            dirs.append(name)
                        elif name.lower().endswith('.log'):
                            log_files.append(name)
                        else:
                            other_files.append(name)
                    except OSError:
                        # If we cannot stat, treat as other file-like entry.
                        other_files.append(name)

                key = lambda s: s.lower()
                dirs.sort(key=key)
                log_files.sort(key=key)
                other_files.sort(key=key)
                msg = ""
                return dirs, log_files, other_files

            while True:
                dirs, log_files, other_files = _list_dir(cur_dir)

                entries = []
                entries.append(('dir', '..'))
                for name in dirs:
                    entries.append(('dir', name))
                for name in log_files:
                    entries.append(('file', name))
                for name in other_files:
                    entries.append(('file', name))
                entries.append(('manual', None))

                try:
                    h, w = stdscr.getmaxyx()
                    stdscr.erase()

                    if h < 6 or w < 24:
                        _safe_addnstr(stdscr, 0, 0, "logfile 탐색", max(0, w - 1))
                        _safe_addnstr(stdscr, 2, 0, "터미널이 너무 작습니다", max(0, w - 1))
                        _safe_addnstr(stdscr, 3, 0, "q=직접입력", max(0, w - 1))
                        stdscr.refresh()
                    else:
                        header0 = "logfile 탐색 (↑/↓, Enter=열기/선택, Backspace=상위, g=./log, q=직접입력)"
                        _safe_addnstr(stdscr, 0, 0, header0, max(0, w - 1))
                        _safe_addnstr(stdscr, 1, 0, f"{cur_dir}", max(0, w - 1))
                        if msg:
                            _safe_addnstr(stdscr, 2, 0, msg, max(0, w - 1))
                            list_top_y = 3
                        else:
                            list_top_y = 2

                        visible_h = max(1, h - list_top_y)
                        if idx >= len(entries):
                            idx = max(0, len(entries) - 1)
                            top = 0
                        if idx < top:
                            top = idx
                        if idx >= top + visible_h:
                            top = idx - visible_h + 1

                        for row in range(visible_h):
                            i = top + row
                            if i >= len(entries):
                                break
                            y = list_top_y + row
                            kind, name = entries[i]
                            prefix = "> " if i == idx else "  "
                            if kind == 'dir':
                                label = f"[D] {name}"
                            elif kind == 'manual':
                                label = "직접 입력..."
                            else:
                                label = f"    {name}"

                            _safe_addnstr(stdscr, y, 0, prefix + label, max(0, w - 1))

                        stdscr.refresh()

                except Exception:
                    return None

                try:
                    ch = stdscr.getch()
                except Exception:
                    return None

                if ch in (ord('q'), ord('Q'), 27):
                    return None
                if ch in (curses.KEY_UP, ord('k')):
                    idx = (idx - 1) % len(entries)
                    continue
                if ch in (curses.KEY_DOWN, ord('j')):
                    idx = (idx + 1) % len(entries)
                    continue
                if ch in (curses.KEY_RESIZE,):
                    continue
                if ch in (curses.KEY_BACKSPACE, 127, 8):
                    parent = os.path.dirname(cur_dir)
                    if parent and parent != cur_dir:
                        cur_dir = parent
                        idx = 0
                        top = 0
                    continue
                if ch in (ord('g'), ord('G')):
                    if os.path.isdir('log'):
                        cur_dir = os.path.abspath('log')
                        idx = 0
                        top = 0
                        msg = ""
                    else:
                        msg = "./log 디렉터리가 없습니다"
                    continue
                if ch in (curses.KEY_ENTER, 10, 13):
                    kind, name = entries[idx]
                    if kind == 'manual':
                        return None
                    if kind == 'dir':
                        if name == '..':
                            parent = os.path.dirname(cur_dir)
                            if parent and parent != cur_dir:
                                cur_dir = parent
                        else:
                            next_dir = os.path.join(cur_dir, name)
                            if os.path.isdir(next_dir):
                                cur_dir = next_dir
                            else:
                                msg = f"디렉터리를 찾을 수 없습니다: {next_dir}"
                        idx = 0
                        top = 0
                        continue
                    # file
                    p = os.path.join(cur_dir, name)
                    if os.path.isfile(p):
                        return p
                    msg = f"파일을 찾을 수 없습니다: {p}"

        def _run(stdscr):
            # Main menu: quick candidates + browse + manual input
            items = []
            for p in (candidates or []):
                items.append(('file', str(p)))

            items.append(('browse', '.'))
            if os.path.isdir('log'):
                items.append(('browse_log', 'log'))
            items.append(('manual', None))

            idx = 0
            top = 0
            try:
                stdscr.keypad(True)
            except Exception:
                pass
            try:
                curses.curs_set(0)
            except Exception:
                pass

            while True:
                try:
                    h, w = stdscr.getmaxyx()
                    stdscr.erase()

                    title = "logfile 선택 (↑/↓, Enter, q=직접입력)"
                    _safe_addnstr(stdscr, 0, 0, title, max(0, w - 1))

                    if h < 6 or w < 24:
                        _safe_addnstr(stdscr, 2, 0, "터미널이 너무 작습니다", max(0, w - 1))
                        _safe_addnstr(stdscr, 3, 0, "q=직접입력", max(0, w - 1))
                        stdscr.refresh()
                    else:
                        visible_h = h - 2
                        if idx < top:
                            top = idx
                        if idx >= top + visible_h:
                            top = idx - visible_h + 1

                        for row in range(visible_h):
                            i = top + row
                            if i >= len(items):
                                break
                            y = 1 + row

                            kind, val = items[i]
                            if kind == 'file':
                                left = str(val)
                                right = _logfile_meta_brief(str(val))
                            elif kind == 'browse':
                                left = "탐색... (현재 디렉터리)"
                                right = ""
                            elif kind == 'browse_log':
                                left = "탐색... (./log)"
                                right = ""
                            else:
                                left = "직접 입력..."
                                right = ""

                            prefix = "> " if i == idx else "  "
                            if right:
                                min_gap = 2
                                right_w = min(len(right), max(0, w - 1 - min_gap))
                                left_w = max(0, w - 1 - len(prefix) - min_gap - right_w)
                                _safe_addnstr(stdscr, y, 0, prefix + left, len(prefix) + left_w)
                                if right_w > 0:
                                    _safe_addnstr(stdscr, y, w - right_w - 1, right[-right_w:], right_w)
                            else:
                                max_left = max(0, w - 1 - len(prefix))
                                _safe_addnstr(stdscr, y, 0, prefix + left, max_left)

                        stdscr.refresh()

                except Exception:
                    return None

                try:
                    ch = stdscr.getch()
                except Exception:
                    return None

                if ch in (ord('q'), ord('Q'), 27):
                    return None
                if ch in (curses.KEY_UP, ord('k')):
                    idx = (idx - 1) % len(items)
                    continue
                if ch in (curses.KEY_DOWN, ord('j')):
                    idx = (idx + 1) % len(items)
                    continue
                if ch in (curses.KEY_RESIZE,):
                    continue
                if ch in (curses.KEY_ENTER, 10, 13):
                    kind, val = items[idx]
                    if kind == 'manual':
                        return None
                    if kind == 'browse':
                        return _browse_files(stdscr, '.')
                    if kind == 'browse_log':
                        return _browse_files(stdscr, 'log')
                    return str(val)

        try:
            return curses.wrapper(_run)
        except Exception:
            return None

    def _pick_logfile_numeric(candidates):
        if not candidates:
            return None

        print("\nlogfile 선택:")
        for i, p in enumerate(candidates, 1):
            meta = _logfile_meta_brief(p)
            if meta:
                print(f"  {i:2d}) {p}  ({meta})")
            else:
                print(f"  {i:2d}) {p}")
        print("   0) 직접 입력")

        while True:
            try:
                v = _prompt_int('선택', default=1, min_value=0)
            except (EOFError, KeyboardInterrupt):
                return None
            if v is None:
                return None
            if v == 0:
                return None
            if 1 <= v <= len(candidates):
                return candidates[v - 1]
            print(f"  0~{len(candidates)} 범위로 입력하세요")

    def _pick_logfile_for_wizard(default=None):
        candidates = _logfile_candidates(default=default)
        # In a normal TTY with curses available, always show the arrow-key UI
        # even if there are no shallow candidates.
        p = _pick_logfile_curses(candidates)
        if p:
            return p
        # curses unavailable/failed or user chose manual -> numeric menu fallback
        if candidates:
            return _pick_logfile_numeric(candidates)
        return None

    def _run_interactive_wizard(existing_args):
        print("\n[interactive] 옵션을 선택하세요 (Enter=기본값)")
        print(
            """[examples] 입력 예시
  follow(-f)      : y / n
  full(--full)    : y / n
  interval(-i)    : 1h, 10m, 30s, 2(=2h)   (h/m/s 지원, 숫자만=시간)
  top-nodes(-t)   : 10, 20
  errors-only(-e) : y / n
  node(-n)        : motor_driver   (부분 매칭, 빈칸=없음)
  focus-node      : tsd            (부분 매칭, 빈칸=없음)
  csv(-c)         : reports/my_report.csv  (빈칸=자동)
  --from / --to   : 2026-01-27 | 2026-01-27 09:00 | 09:00  (오늘)
"""
        )
        w = argparse.Namespace(**vars(existing_args))

        picked = _pick_logfile_for_wizard(default=w.logfile)
        if picked:
            w.logfile = picked
        else:
            w.logfile = _prompt_existing_file('logfile', default=w.logfile)
        w.follow = _prompt_bool('follow 모드(-f) 사용', default=bool(w.follow))

        w.full = _prompt_bool('전체 재분석(--full)', default=bool(w.full))
        w.interval = _prompt_interval('interval(-i) (예: 1h, 10m, 30s)', default=str(w.interval))
        w.top_nodes = _prompt_int('top-nodes(-t)', default=w.top_nodes, min_value=1)
        w.errors_only = _prompt_bool('errors-only(-e)', default=bool(w.errors_only))

        node_default = w.node if w.node is not None else ''
        s = _prompt_line('node(-n) (부분 매칭, 빈칸=없음)', default=node_default).strip()
        w.node = s or None

        focus_default = w.focus_node if w.focus_node is not None else ''
        s = _prompt_line('focus-node (부분 매칭, 빈칸=없음)', default=focus_default).strip()
        w.focus_node = s or None

        csv_default = w.csv if w.csv is not None else ''
        s = _prompt_line('csv(-c) (파일 경로, 빈칸=자동)', default=csv_default).strip()
        w.csv = s or None

        tf_default = w.time_from if w.time_from is not None else ''
        w.time_from = _prompt_datetime_optional('--from (빈칸=없음)', default=tf_default)
        tt_default = w.time_to if w.time_to is not None else ''
        w.time_to = _prompt_datetime_optional('--to (빈칸=없음)', default=tt_default)

        if w.follow:
            w.window = _prompt_interval('window(-w) (예: 5m, 10m)', default=str(w.window))
            w.refresh = _prompt_float('refresh(-r) (초)', default=w.refresh, min_value=0.1)
            w.tail = _prompt_int('tail (0=끝부터)', default=w.tail, min_value=0)

        return w

    def _build_equivalent_cli(a) -> str:
        argv = ['python3', os.path.basename(__file__), a.logfile]

        argv += ['--interval', str(a.interval)]
        argv += ['--top-nodes', str(a.top_nodes)]
        if a.errors_only:
            argv += ['--errors-only']
        if a.node:
            argv += ['--node', str(a.node)]
        if a.focus_node:
            argv += ['--focus-node', str(a.focus_node)]
        if a.csv:
            argv += ['--csv', str(a.csv)]
        if a.time_from:
            argv += ['--from', str(a.time_from)]
        if a.time_to:
            argv += ['--to', str(a.time_to)]
        if a.full:
            argv += ['--full']

        if a.follow:
            argv += ['--follow']
            argv += ['--window', str(a.window)]
            argv += ['--refresh', str(a.refresh)]
            argv += ['--tail', str(a.tail)]

        return ' '.join(shlex.quote(x) for x in argv)

    interactive = bool(args.interactive)
    if not interactive and args.logfile is None and _is_tty():
        interactive = True

    if interactive and not _is_tty():
        parser.error('--interactive 는 TTY에서만 사용할 수 있습니다')

    if interactive:
        try:
            args = _run_interactive_wizard(args)
        except KeyboardInterrupt:
            print("\n취소됨")
            return

        print("\n선택한 옵션:")
        print(f"  logfile     : {args.logfile}")
        print(f"  follow      : {args.follow}")
        print(f"  full        : {args.full}")
        print(f"  interval    : {args.interval}")
        print(f"  top-nodes   : {args.top_nodes}")
        print(f"  errors-only : {args.errors_only}")
        print(f"  node        : {args.node}")
        print(f"  focus-node  : {args.focus_node}")
        print(f"  csv         : {args.csv}")
        print(f"  from/to     : {args.time_from} ~ {args.time_to}")
        if args.follow:
            print(f"  window      : {args.window}")
            print(f"  refresh     : {args.refresh}")
            print(f"  tail        : {args.tail}")

        print("\nCLI 힌트:")
        print(f"  {_build_equivalent_cli(args)}")
        print()

    if args.logfile is None:
        parser.error('logfile 인자가 필요합니다 (또는 --interactive 사용)')

    # 실시간 모니터링 모드
    if args.follow:
        # follow 모드에서는 interval 기본값을 1분으로 조정
        if args.interval == '1h':
            args.interval = '1m'
        try:
            interval_sec = parse_interval(args.interval)
            window_sec = parse_interval(args.window)
        except ValueError as e:
            parser.error(str(e))
        run_follow_mode(args, interval_sec, window_sec)
        return

    try:
        interval_sec = parse_interval(args.interval)
    except ValueError as e:
        parser.error(str(e))

    # 시간 범위 파싱
    ts_from = None
    ts_to = None
    if args.time_from:
        try:
            ts_from = parse_datetime_arg(args.time_from)
        except ValueError as e:
            parser.error(str(e))
    if args.time_to:
        try:
            ts_to = parse_datetime_arg(args.time_to)
        except ValueError as e:
            parser.error(str(e))

    analyzer = LogAnalyzer(
        interval_sec=interval_sec,
        node_filter=args.node,
        errors_only=args.errors_only,
        focus_query=args.focus_node,
        ts_from=ts_from,
        ts_to=ts_to,
    )

    file_size = os.path.getsize(args.logfile)

    # --- 증분 분석 결정 ---
    start_offset = 0
    incremental = False
    cp = None

    if not args.full:
        cp = load_checkpoint(args.logfile)

    if cp and not args.full:
        prev_offset = cp.get('offset')
        prev_size = cp.get('file_size')

        # 체크포인트 필드 검증: 이상하면 체크포인트 무시
        if (
            not isinstance(prev_offset, int)
            or not isinstance(prev_size, int)
            or prev_offset < 0
            or prev_size < 0
        ):
            cp = None
        else:
            prev_lines = cp.get('total_lines', 0)
            prev_at = cp.get('analyzed_at', '?')
            prev_ts = cp.get('last_ts')

            if file_size < prev_size:
                # 파일이 작아짐 → 로그 로테이션 감지 → 전체 분석
                print(f"\n  로그 로테이션 감지 (이전 {prev_size:,} → 현재 {file_size:,} bytes)")
                print(f"  전체 재분석을 수행합니다.")
                clear_checkpoint(args.logfile)
                start_offset = 0
            elif file_size <= prev_size:
                # 변경 없음
                print(f"\n  마지막 분석: {prev_at}")
                print(f"  새로 추가된 로그가 없습니다. (파일 크기: {file_size:,} bytes)")
                print(f"  전체 재분석을 하려면 --full 옵션을 사용하세요.\n")
                return
            else:
                # 증분 분석
                incremental = True
                start_offset = prev_offset
                new_bytes = file_size - prev_offset
                print(f"\n  *** 증분 분석 모드 ***")
                print(f"  마지막 분석   : {prev_at}")
                if prev_ts:
                    print(f"  마지막 로그   : {datetime.fromtimestamp(prev_ts).strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  이전 위치     : {prev_offset:,} bytes ({prev_lines:,} 줄)")
                print(f"  새 데이터     : {new_bytes / (1024**2):.1f} MB")
    else:
        if args.full and cp:
            print(f"\n  --full: 체크포인트를 무시하고 전체 재분석합니다.")
            clear_checkpoint(args.logfile)

    # --- 분석 대상 크기 ---
    analyze_bytes = file_size - start_offset

    print(f"\n  로그 파일: {args.logfile}")
    print(f"  전체 크기: {file_size / (1024**3):.2f} GB")
    if incremental:
        print(f"  분석 범위: {start_offset:,} ~ {file_size:,} bytes ({analyze_bytes / (1024**2):.1f} MB)")
    print(f"  분석 단위: {format_interval(interval_sec)}")
    if args.node:
        print(f"  노드 필터: {args.node}")
    if args.errors_only:
        print(f"  모드: ERROR/WARN/FATAL만 분석")
    if ts_from is not None or ts_to is not None:
        from_str = datetime.fromtimestamp(ts_from).strftime('%Y-%m-%d %H:%M:%S') if ts_from else '(처음)'
        to_str = datetime.fromtimestamp(ts_to).strftime('%Y-%m-%d %H:%M:%S') if ts_to else '(끝)'
        print(f"  시간 범위: {from_str} ~ {to_str}")
    print()

    start_time = time.time()
    bytes_read = 0
    last_progress = -1
    final_offset = start_offset

    try:
        with open(args.logfile, 'rb') as fb:
            # EOF에 부분 라인이 있으면 제외하고 처리 (다음 실행에서 재처리)
            effective_end = _effective_scan_end(fb, file_size)
            analyzable_bytes = max(0, effective_end - start_offset)

            read_offset = start_offset
            if read_offset > effective_end:
                read_offset = effective_end

            if read_offset > 0:
                safe_start = _rewind_to_line_start_if_needed(fb, read_offset)
                fb.seek(safe_start)
                # 체크포인트가 줄 중간이었다면 해당 줄은 버리고, 다음 줄부터 처리
                if safe_start < read_offset:
                    fb.readline()
            else:
                fb.seek(0)

            final_offset = fb.tell()

            while fb.tell() < effective_end:
                line_start = fb.tell()
                bline = fb.readline()
                if not bline:
                    break

                # 안전장치: effective_end 이후의 trailing partial line은 처리하지 않음
                if line_start >= effective_end:
                    break

                # 정상 라인만 처리 (\n로 끝나야 함)
                if not bline.endswith(b'\n'):
                    final_offset = line_start
                    break

                line = bline.decode('utf-8', errors='replace')
                analyzer.process_line(line)
                final_offset = fb.tell()

                # 진행률/속도 계산은 byte 기준
                bytes_read = max(0, final_offset - start_offset)

                if analyzable_bytes > 0:
                    progress = int(bytes_read / analyzable_bytes * 100)
                    if progress > 100:
                        progress = 100
                else:
                    progress = 100

                if progress > last_progress:
                    last_progress = progress
                    elapsed = time.time() - start_time
                    speed = bytes_read / (1024**2) / elapsed if elapsed > 0 else 0
                    remaining = max(0, analyzable_bytes - bytes_read)
                    eta = remaining / (bytes_read / elapsed) if bytes_read > 0 and elapsed > 0 else 0
                    eta_min = int(eta // 60)
                    eta_sec = int(eta % 60)
                    mode_tag = "[증분] " if incremental else ""
                    sys.stderr.write(
                        f"\r  {mode_tag}진행: {progress:>3}%  |  "
                        f"{analyzer.total_lines:>12,} 줄  |  "
                        f"{speed:>6.1f} MB/s  |  "
                        f"남은 시간: {eta_min}분 {eta_sec}초   "
                    )
                    sys.stderr.flush()

    except KeyboardInterrupt:
        print(f"\n\n  [중단됨] {analyzer.total_lines:,} 줄까지 분석된 결과를 표시합니다.\n")

    elapsed = time.time() - start_time
    mode_tag = "[증분] " if incremental else ""
    sys.stderr.write(f"\r  {mode_tag}완료: {analyzer.total_lines:,} 줄 처리 ({elapsed:.1f}초)\n\n")
    sys.stderr.flush()

    analyzer.print_report(top_nodes=args.top_nodes)

    # 체크포인트 저장
    last_ts = analyzer.ts_max if analyzer.ts_max != float('-inf') else None
    cumulative_lines = analyzer.total_lines
    if incremental and cp:
        cumulative_lines += cp.get('total_lines', 0)
    save_checkpoint(args.logfile, final_offset, cumulative_lines, last_ts, file_size)
    print(f"  체크포인트 저장 완료 (offset: {final_offset:,} bytes, {cumulative_lines:,} 줄)")

    # CSV 보고서 자동 생성 (실행 날짜 기준)
    report_dir = _get_report_dir()

    if args.csv:
        # 사용자 지정 경로로 내보내기
        analyzer.export_csv_reports(output_dir=None, single_file=args.csv)
    else:
        # 자동: reports/ 디렉토리에 날짜별 CSV 생성
        analyzer.export_csv_reports(output_dir=report_dir)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n취소됨")
        raise SystemExit(130)
