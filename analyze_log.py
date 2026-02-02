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

from log_parser import (
    RE_ANSI, bucket_key, bucket_label, parse_datetime_arg, parse_interval,
    parse_line, parse_line_full, extract_comm_content,
)
from i18n import set_lang, t


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
            msg_part = _extract_msg_part(clean_msg, node, max_len=200)
            self.error_messages.append((dt_str, node, level, msg_part))

    def print_report(self, top_nodes=5):
        if self.parsed_lines == 0:
            print(t("파싱된 로그 라인이 없습니다.", "No parsed log lines found."))
            return
        if self.matched_lines == 0:
            print(t("필터에 매칭되는 로그 라인이 없습니다.", "No log lines matching the filter."))
            return

        sorted_buckets = sorted(self.bucket_total.keys())

        # --- 전체 요약 ---
        print("=" * 90)
        print(f"  {t('ROS2 AGV 로그 시간대별 분석 보고서', 'ROS2 AGV Time-Based Log Analysis Report')}")
        print("=" * 90)
        print()
        print(f"  {t('파일 분석 완료', 'File analysis complete')}")
        print(f"  {t('총 라인 수', 'Total lines'):<15}: {self.total_lines:>15,}")
        print(f"  {t('파싱 성공', 'Parsed OK'):<15}: {self.parsed_lines:>15,}")
        print(f"  {t('파싱 실패', 'Parse failed'):<15}: {self.unparsed_lines:>15,}")
        print(f"  {t('시간 범위', 'Time range'):<15}: {datetime.fromtimestamp(self.ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"                   ~ {datetime.fromtimestamp(self.ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
        duration = self.ts_max - self.ts_min
        days = int(duration // 86400)
        hours = int((duration % 86400) // 3600)
        mins = int((duration % 3600) // 60)
        print(f"  {t('총 기간', 'Duration'):<15}: {t(f'{days}일 {hours}시간 {mins}분', f'{days}d {hours}h {mins}m')}")
        print(f"  {t('분석 단위', 'Interval'):<15}: {format_interval(self.interval_sec)}")
        print()

        # --- 로그 레벨별 전체 통계 ---
        print("-" * 90)
        print(f"  [{t('로그 레벨별 전체 통계', 'Log Level Statistics')}]")
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
        print(f"  [{t('노드별 전체 통계', 'Node Statistics')} - {t('상위', 'Top')} {top_nodes}{t('개', '')}]")
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
            print(f"  {t('(기타 ' + str(len(sorted_nodes) - top_nodes) + '개 노드)', '(other ' + str(len(sorted_nodes) - top_nodes) + ' nodes)'):<40} {others:>12,}  ({pct:>5.1f}%)")
        print()

        # --- 시간대별 로그 볼륨 ---
        print("-" * 90)
        print(f"  [{t('시간대별 로그 볼륨', 'Log Volume by Time')}]")
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
        print(f"  [{t('시간대별 로그 레벨 분포', 'Log Level Distribution by Time')}]")
        print("-" * 90)
        header = f"  {t('시간대', 'Time'):<20} {'FATAL':>8} {'ERROR':>8} {'WARN':>8} {'INFO':>10} {'DEBUG':>8} {t('합계', 'Total'):>10}"
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
        print(f"  [{t('시간대별 상위 활동 노드 (Top 3)', 'Top Active Nodes by Time (Top 3)')}]")
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
            print(f"  [{t(f'ERROR/WARN/FATAL 메시지 샘플 (최대 {self.max_error_msgs}개)', f'ERROR/WARN/FATAL Message Samples (max {self.max_error_msgs})')}]")
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
                        print(f"         {t('예시', 'Example')}: {short_msg[:120]}")
                        break
                print()

        # --- 포커스 노드 상세 ---
        if self.focus_query is not None:
            print("-" * 90)
            print(f"  [{t('포커스 노드 상세', 'Focus Node Details')}]")
            print("-" * 90)
            print(f"  {t('포커스 쿼리', 'Focus query')}: {self.focus_query if self.focus_query else t('(빈 문자열)', '(empty string)')}")

            if not self.focus_query:
                print(t("  포커스 쿼리가 비어 있어 상세 집계를 생략합니다.", "  Focus query is empty, skipping detailed aggregation."))
                print()
                return

            focus_nodes = [(n, c) for n, c in self.node_totals.items() if self.focus_query in n]
            focus_nodes.sort(key=lambda x: (-x[1], x[0]))
            print(f"  {t('매칭 노드 수', 'Matched nodes')}: {len(focus_nodes):,}{t('개', '')}")
            if focus_nodes:
                top_list = ", ".join(f"{n}({c:,})" for n, c in focus_nodes[:10])
                print(f"  {t('매칭 노드 Top10', 'Matched nodes Top10')}: {top_list}")

            if self.focus_total == 0:
                print("\n" + t("  포커스 노드에 매칭되는 로그가 없습니다.", "  No logs matching the focus node.") + "\n")
                return

            # 기본 통계
            err_cnt = self.focus_levels.get('ERROR', 0) + self.focus_levels.get('FATAL', 0)
            warn_cnt = self.focus_levels.get('WARN', 0)
            duration = self.focus_ts_max - self.focus_ts_min

            print()
            print(f"  {t('총 로그 수', 'Total logs'):<13}: {self.focus_total:>12,}")
            print(f"  {t('시작 시각', 'Start time'):<13}: {datetime.fromtimestamp(self.focus_ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  {t('마지막 시각', 'Last time'):<13}: {datetime.fromtimestamp(self.focus_ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
            if duration >= 60:
                print(f"  {t('활동 기간', 'Duration'):<13}: {int(duration // 3600)}{t('시간', 'h')} {int((duration % 3600) // 60)}{t('분', 'm')} {int(duration % 60)}{t('초', 's')}")
            else:
                print(f"  {t('활동 기간', 'Duration'):<13}: {duration:.1f}{t('초', 's')}")
            if duration > 0:
                print(f"  {t('평균 처리율', 'Avg rate'):<13}: {self.focus_total / duration:.1f} lines/sec")
            print()

            # 레벨 분포
            print(f"  [{t('포커스 로그 레벨 분포', 'Focus Log Level Distribution')}]")
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

                print(f"  [{t('포커스 시간대별 활동 타임라인', 'Focus Activity Timeline')}]  ({t('단위', 'Unit')}: {format_interval(self.interval_sec)})")
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
                    print(f"  {t(f'포커스 피크 구간 (평균 {avg:,.0f}의 2배 이상):', f'Focus peak intervals (>2x avg {avg:,.0f}):')}")
                    for bk, c in peaks[:5]:
                        label = bucket_label(bk, self.interval_sec)
                        print(f"    {label}  →  {c:,} ({c / avg:.1f}x)")
                    print()
                else:
                    print(f"  {t(f'포커스 피크 구간: 없음 (평균 {avg:,.0f})', f'Focus peak intervals: none (avg {avg:,.0f})')}")
                    print()

            # 에러/경고 패턴 요약 (샘플 기반, bounded)
            print(f"  [{t('포커스 에러/경고 메시지 패턴', 'Focus Error/Warning Message Patterns')}]  (ERROR/FATAL {err_cnt:,} + WARN {warn_cnt:,} {t('중 샘플', 'samples')} {len(self.focus_error_samples)}{t('건', '')})")
            if not self.focus_error_samples:
                print(t("  (샘플 없음)", "  (no samples)"))
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
                print(f"         {t('예시', 'Example')}: {info.example}")
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

        print(f"  {t('CSV 저장', 'CSV saved')}: {filepath}")
        print()


def format_interval(sec):
    if sec >= 3600:
        return f"{sec // 3600}{t('시간', 'h')}"
    elif sec >= 60:
        return f"{sec // 60}{t('분', 'm')}"
    else:
        return f"{sec}{t('초', 's')}"


# =============================================================================
#  통신 데이터 분석 (--comm 모드)
# =============================================================================

class CommAnalyzer:
    """Node communication content analyzer for --comm mode."""

    MAX_TOPICS_PER_NODE = 200
    MAX_ANOMALIES = 200
    MAX_VALUE_TREND = 500

    def __init__(self, interval_sec, node_filter=None, errors_only=False,
                 ts_from=None, ts_to=None,
                 track_topics=None, track_values=None, gap_threshold=10.0):
        self.interval_sec = interval_sec
        self.node_filter = node_filter
        self.errors_only = errors_only
        self.ts_from = ts_from
        self.ts_to = ts_to
        self.track_topics = track_topics or []  # partial-match topic filters
        self.track_values = set(track_values or [])  # key names to track
        self.gap_threshold = gap_threshold

        # Counters
        self.total_lines = 0
        self.comm_lines = 0

        # node -> set of topics (bounded per node)
        self.node_topics = defaultdict(set)
        # topic -> {count, publish, subscribe, error}
        self.topic_stats = defaultdict(lambda: {'count': 0, 'publish': 0, 'subscribe': 0, 'error': 0})
        # node -> topic -> action -> count
        self.node_topic_action = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        # bucket -> topic -> count
        self.bucket_topic_counts = defaultdict(lambda: defaultdict(int))
        # (topic, key) -> {count, sum, min, max}
        self.kv_summary = {}
        # (topic, key) -> deque of (ts, value)
        self.value_trends = {}
        # anomalies deque
        self.anomalies = deque(maxlen=self.MAX_ANOMALIES)
        # topic -> last timestamp (for gap detection)
        self._topic_last_ts = {}

        # Time range
        self.ts_min = float('inf')
        self.ts_max = float('-inf')

    def _matches_track_topic(self, topic):
        """Check if topic matches any --track-topic filter (partial match)."""
        if not self.track_topics:
            return True
        return any(t_filter in topic for t_filter in self.track_topics)

    def process_line(self, line):
        self.total_lines += 1

        result = parse_line_full(line)
        if result is None:
            return

        ts, node, level, msg = result

        # Time range filter
        if self.ts_from is not None and ts < self.ts_from:
            return
        if self.ts_to is not None and ts > self.ts_to:
            return

        # Node filter
        if self.node_filter and self.node_filter not in node:
            return
        if self.errors_only and level not in ('ERROR', 'WARN', 'FATAL'):
            return

        # Extract communication content
        comm = extract_comm_content(msg)
        if comm is None:
            return

        self.comm_lines += 1

        # Update time range
        if ts < self.ts_min:
            self.ts_min = ts
        if ts > self.ts_max:
            self.ts_max = ts

        topics = comm['topics']
        action = comm['action']
        kv_pairs = comm['kv_pairs']

        bk = bucket_key(ts, self.interval_sec)

        for topic in topics:
            # Bounded node-topic mapping
            node_set = self.node_topics[node]
            if len(node_set) < self.MAX_TOPICS_PER_NODE:
                node_set.add(topic)

            # Topic stats
            stats = self.topic_stats[topic]
            stats['count'] += 1
            if action:
                stats[action] += 1

            # Node-topic-action
            self.node_topic_action[node][topic][action or 'unknown'] += 1

            # Bucket topic counts
            self.bucket_topic_counts[bk][topic] += 1

            # Gap detection
            if topic in self._topic_last_ts:
                gap = ts - self._topic_last_ts[topic]
                if gap > self.gap_threshold:
                    self.anomalies.append({
                        'type': 'gap',
                        'ts': ts,
                        'node': node,
                        'topic': topic,
                        'detail': f'{gap:.1f}s gap (threshold: {self.gap_threshold}s)',
                    })
            self._topic_last_ts[topic] = ts

            # KV summary + value trends
            for key, val in kv_pairs:
                kv_key = (topic, key)
                if kv_key not in self.kv_summary:
                    self.kv_summary[kv_key] = {'count': 0, 'sum': 0.0, 'min': val, 'max': val}
                s = self.kv_summary[kv_key]
                s['count'] += 1
                s['sum'] += val
                if val < s['min']:
                    s['min'] = val
                if val > s['max']:
                    s['max'] = val

                # Value trends for tracked keys
                if key in self.track_values:
                    if kv_key not in self.value_trends:
                        self.value_trends[kv_key] = deque(maxlen=self.MAX_VALUE_TREND)
                    self.value_trends[kv_key].append((ts, val))

        # Error action anomaly
        if action == 'error':
            self.anomalies.append({
                'type': 'comm_error',
                'ts': ts,
                'node': node,
                'topic': topics[0] if topics else '(unknown)',
                'detail': msg[:150],
            })

    def print_report(self):
        if self.comm_lines == 0:
            print(t("  통신 관련 로그 라인이 없습니다.", "  No communication-related log lines found."))
            print()
            return

        # --- Section 1: Communication Summary ---
        print("-" * 90)
        print(f"  [{t('통신 분석 요약', 'Communication Analysis Summary')}]")
        print("-" * 90)
        unique_pairs = sum(len(topics) for topics in self.node_topics.values())
        print(f"  {t('통신 라인 수', 'Comm lines'):<20}: {self.comm_lines:>12,}")
        print(f"  {t('고유 토픽 수', 'Unique topics'):<20}: {len(self.topic_stats):>12,}")
        print(f"  {t('고유 노드-토픽 쌍', 'Node-topic pairs'):<20}: {unique_pairs:>12,}")
        if self.ts_min != float('inf'):
            print(f"  {t('시간 범위', 'Time range'):<20}: {datetime.fromtimestamp(self.ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  {'':20}  ~ {datetime.fromtimestamp(self.ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # --- Section 2: Topic Statistics ---
        print("-" * 90)
        print(f"  [{t('토픽별 통계', 'Topic Statistics')}]")
        print("-" * 90)
        sorted_topics = sorted(self.topic_stats.items(), key=lambda x: -x[1]['count'])
        max_topic_count = sorted_topics[0][1]['count'] if sorted_topics else 1
        header = f"  {'Topic':<40} {t('합계','Total'):>8} {'Pub':>8} {'Sub':>8} {'Err':>6}"
        print(header)
        print("  " + "-" * 76)
        for topic, stats in sorted_topics[:30]:
            bar_len = int(stats['count'] / max_topic_count * 20)
            bar = "█" * bar_len
            print(f"  {topic:<40} {stats['count']:>8,} {stats['publish']:>8,} {stats['subscribe']:>8,} {stats['error']:>6,}  {bar}")
        if len(sorted_topics) > 30:
            print(f"  ... {t(f'외 {len(sorted_topics) - 30}개 토픽', f'and {len(sorted_topics) - 30} more topics')}")
        print()

        # --- Section 3: Node-Topic Mapping ---
        print("-" * 90)
        print(f"  [{t('노드-토픽 매핑', 'Node-Topic Mapping')}]")
        print("-" * 90)
        sorted_nodes = sorted(self.node_topic_action.keys())
        for node in sorted_nodes[:20]:
            topics_map = self.node_topic_action[node]
            total_for_node = sum(
                sum(acts.values()) for acts in topics_map.values()
            )
            print(f"  {node} ({total_for_node:,} {t('건', 'msgs')})")
            sorted_node_topics = sorted(topics_map.items(),
                                        key=lambda x: -sum(x[1].values()))
            for topic, acts in sorted_node_topics[:10]:
                act_parts = []
                for a in ('publish', 'subscribe', 'error', 'unknown'):
                    cnt = acts.get(a, 0)
                    if cnt > 0:
                        act_parts.append(f"{a}:{cnt:,}")
                print(f"    {topic:<36} {', '.join(act_parts)}")
            if len(sorted_node_topics) > 10:
                print(f"    ... {t(f'외 {len(sorted_node_topics) - 10}개 토픽', f'and {len(sorted_node_topics) - 10} more topics')}")
            print()

        # --- Section 4: Topic Activity by Time ---
        if self.bucket_topic_counts:
            print("-" * 90)
            print(f"  [{t('시간대별 토픽 활동', 'Topic Activity by Time')}]")
            print("-" * 90)
            sorted_bks = sorted(self.bucket_topic_counts.keys())
            for bk in sorted_bks:
                label = bucket_label(bk, self.interval_sec)
                topic_counts = self.bucket_topic_counts[bk]
                top5 = sorted(topic_counts.items(), key=lambda x: -x[1])[:5]
                total_bk = sum(topic_counts.values())
                top_str = ", ".join(f"{t_name}({c:,})" for t_name, c in top5)
                print(f"  {label}  {total_bk:>8,}  {top_str}")
            print()

        # --- Section 5: Key-Value Data Summary ---
        if self.kv_summary:
            print("-" * 90)
            print(f"  [{t('키-값 데이터 요약', 'Key-Value Data Summary')}]")
            print("-" * 90)
            header = f"  {'Topic':<30} {'Key':<15} {t('횟수','Count'):>8} {'Avg':>10} {'Min':>10} {'Max':>10}"
            print(header)
            print("  " + "-" * 88)
            sorted_kv = sorted(self.kv_summary.items(), key=lambda x: -x[1]['count'])
            for (topic, key), s in sorted_kv[:40]:
                avg = s['sum'] / s['count'] if s['count'] > 0 else 0
                print(f"  {topic:<30} {key:<15} {s['count']:>8,} {avg:>10.3f} {s['min']:>10.3f} {s['max']:>10.3f}")
            if len(sorted_kv) > 40:
                print(f"  ... {t(f'외 {len(sorted_kv) - 40}개 항목', f'and {len(sorted_kv) - 40} more entries')}")
            print()

        # --- Section 6: Value Trends ---
        if self.value_trends:
            print("-" * 90)
            print(f"  [{t('값 변화 추적', 'Value Change Tracking')}]")
            print("-" * 90)
            for (topic, key), trend in sorted(self.value_trends.items()):
                if not trend:
                    continue
                vals = [v for _, v in trend]
                print(f"  {topic} / {key}  ({len(trend)} {t('샘플', 'samples')})")
                print(f"    {t('최근 10개', 'Last 10')}: {', '.join(f'{v:.3f}' for v in vals[-10:])}")
                # Simple trend indicator
                if len(vals) >= 2:
                    first_half = sum(vals[:len(vals)//2]) / max(1, len(vals)//2)
                    second_half = sum(vals[len(vals)//2:]) / max(1, len(vals) - len(vals)//2)
                    if second_half > first_half * 1.1:
                        trend_str = t('↑ 상승 추세', '↑ increasing')
                    elif second_half < first_half * 0.9:
                        trend_str = t('↓ 하락 추세', '↓ decreasing')
                    else:
                        trend_str = t('→ 안정', '→ stable')
                    print(f"    {t('추세', 'Trend')}: {trend_str}")
                print()

        # --- Section 7: Communication Anomalies ---
        if self.anomalies:
            print("-" * 90)
            print(f"  [{t('통신 이상', 'Communication Anomalies')}]  ({len(self.anomalies)} {t('건', 'events')})")
            print("-" * 90)
            for anom in list(self.anomalies)[-30:]:
                dt_str = datetime.fromtimestamp(anom['ts']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"  [{anom['type']:<12}] {dt_str}  {anom['node']:<30} {anom['topic']}")
                print(f"    {anom['detail'][:120]}")
            print()

    def export_csv_sections(self, writer):
        """Write comm analysis sections to an existing CSV writer."""
        if self.comm_lines == 0:
            return

        # Section: Topic Statistics
        writer.writerow([])
        writer.writerow([t('[토픽 통계]', '[Topic Statistics]')])
        writer.writerow(['topic', 'count', 'publish', 'subscribe', 'error'])
        for topic, stats in sorted(self.topic_stats.items(), key=lambda x: -x[1]['count']):
            writer.writerow([topic, stats['count'], stats['publish'], stats['subscribe'], stats['error']])

        # Section: Node-Topic Mapping
        writer.writerow([])
        writer.writerow([t('[노드-토픽 매핑]', '[Node-Topic Mapping]')])
        writer.writerow(['node', 'topic', 'action', 'count'])
        for node in sorted(self.node_topic_action.keys()):
            for topic in sorted(self.node_topic_action[node].keys()):
                for action, cnt in sorted(self.node_topic_action[node][topic].items(), key=lambda x: -x[1]):
                    writer.writerow([node, topic, action, cnt])

        # Section: KV Summary
        if self.kv_summary:
            writer.writerow([])
            writer.writerow([t('[키-값 요약]', '[Key-Value Summary]')])
            writer.writerow(['topic', 'key', 'count', 'avg', 'min', 'max'])
            for (topic, key), s in sorted(self.kv_summary.items(), key=lambda x: -x[1]['count']):
                avg = s['sum'] / s['count'] if s['count'] > 0 else 0
                writer.writerow([topic, key, s['count'], f"{avg:.6f}", f"{s['min']:.6f}", f"{s['max']:.6f}"])

        # Section: Anomalies
        if self.anomalies:
            writer.writerow([])
            writer.writerow([t('[통신 이상]', '[Communication Anomalies]')])
            writer.writerow(['type', 'timestamp', 'node', 'topic', 'detail'])
            for anom in self.anomalies:
                dt_str = datetime.fromtimestamp(anom['ts']).strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([anom['type'], dt_str, anom['node'], anom['topic'], anom['detail']])


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

    def __init__(self, interval_sec, window_sec, node_filter=None, errors_only=False, focus_query=None,
                 comm_mode=False, track_topics=None, track_values=None, gap_threshold=10.0):
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

        # Communication mode fields
        self.comm_mode = comm_mode
        if comm_mode:
            self.comm_track_topics = track_topics or []
            self.comm_track_values = set(track_values or [])
            self.comm_gap_threshold = gap_threshold
            self.comm_events = deque()  # (ts, node, topic, action) rolling window
            self.comm_topic_counts = defaultdict(int)  # cumulative topic counts
            self.comm_node_topics = defaultdict(set)  # node -> set of topics (max 200/node)
            self.comm_recent_values = {}  # (topic,key) -> deque of (ts, val), maxlen=20
            self.comm_anomalies = deque(maxlen=10)
            self._comm_topic_last_ts = {}

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

        # Communication mode processing
        if self.comm_mode:
            full = parse_line_full(line)
            if full is not None:
                _, _, _, msg = full
                comm = extract_comm_content(msg)
                if comm is not None:
                    topics = comm['topics']
                    action = comm['action']
                    kv_pairs = comm['kv_pairs']
                    for topic in topics:
                        self.comm_events.append((ts, node, topic, action))
                        self.comm_topic_counts[topic] += 1
                        node_set = self.comm_node_topics[node]
                        if len(node_set) < 200:
                            node_set.add(topic)

                        # Gap detection
                        if topic in self._comm_topic_last_ts:
                            gap = ts - self._comm_topic_last_ts[topic]
                            if gap > self.comm_gap_threshold:
                                self.comm_anomalies.append((
                                    ts, 'gap', node, topic,
                                    f'{gap:.1f}s gap',
                                ))
                        self._comm_topic_last_ts[topic] = ts

                        # KV tracking
                        for key, val in kv_pairs:
                            if key in self.comm_track_values:
                                kv_key = (topic, key)
                                if kv_key not in self.comm_recent_values:
                                    self.comm_recent_values[kv_key] = deque(maxlen=20)
                                self.comm_recent_values[kv_key].append((ts, val))

                    if action == 'error':
                        self.comm_anomalies.append((
                            ts, 'comm_error', node,
                            topics[0] if topics else '(unknown)',
                            msg[:100],
                        ))

                    # Expire old comm events from window
                    while self.comm_events and self.comm_events[0][0] < cutoff:
                        self.comm_events.popleft()

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
        title = t("ROS2 AGV 실시간 로그 모니터", "ROS2 AGV Real-time Log Monitor")
        quit_hint = t("Ctrl+C: 종료", "Ctrl+C: Quit")
        pad = w - len(title) - len(quit_hint) - 4
        lines.append(f"{TERM_BOLD}{TERM_CYAN}  {title}{' ' * max(pad, 1)}{TERM_DIM}{quit_hint}{TERM_RESET}")
        lines.append(f"{TERM_BOLD}{TERM_CYAN}{'═' * w}{TERM_RESET}")

        # --- 상태 바 ---
        rate = self._calc_rate()
        win_count = len(self.events)
        elapsed_str = f"{int(wall_elapsed // 60)}{t('분', 'm')} {int(wall_elapsed % 60)}{t('초', 's')}"

        filters = []
        if self.node_filter:
            filters.append(f"{t('노드', 'Node')}={self.node_filter}")
        if self.focus_query:
            filters.append(f"{t('포커스', 'Focus')}={self.focus_query}")
        if self.errors_only:
            filters.append(t("에러만", "errors only"))
        filter_str = f"  {t('필터', 'Filter')}: {', '.join(filters)}" if filters else ""

        lines.append(
            f"  {t('로그 시각', 'Log time')}: {TERM_BOLD}{now_dt.strftime('%Y-%m-%d %H:%M:%S')}{TERM_RESET}"
            f"  |  {t('윈도우', 'Window')}: {format_interval(self.window_sec)}"
            f"  |  {t('경과', 'Elapsed')}: {elapsed_str}"
            f"  |  {t('처리율', 'Rate')}: {TERM_GREEN}{rate:,.0f}{TERM_RESET} lines/s"
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

        lines.append(f"  {TERM_BOLD}{t('[윈도우 내 통계]', '[Window Statistics]')}  {t('총', 'Total')} {total_in_win:,}{t('건', '')}  ({t('최근', 'Last')} {format_interval(self.window_sec)}){TERM_RESET}")
        lines.append(f"  {'─' * (w - 4)}")

        level_parts = []
        for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
            cnt = win_levels.get(lvl, 0)
            if cnt > 0:
                level_parts.append(f"{color}{lvl}: {cnt:,}{TERM_RESET}")
        lines.append(f"  {t('레벨', 'Level')}:  {'  |  '.join(level_parts) if level_parts else t('(없음)', '(none)')}")

        # 누적 통계 한 줄
        cum_parts = []
        for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
            cnt = self.total_levels.get(lvl, 0)
            if cnt > 0:
                cum_parts.append(f"{color}{lvl}: {cnt:,}{TERM_RESET}")
        lines.append(f"  {t('누적', 'Cumul.')}:  {'  |  '.join(cum_parts)}  {TERM_DIM}({t('전체', 'Total')} {self.parsed_lines:,}{t('줄', 'lines')}){TERM_RESET}")
        lines.append("")

        # --- 윈도우 내 시간대별 바 차트 ---
        if win_buckets:
            sorted_bks = sorted(win_buckets.keys())
            # 최근 버킷들만 (화면에 맞게)
            max_bars = min(rows - 25, 15)  # 대시보드 다른 요소 공간 확보
            if len(sorted_bks) > max_bars:
                sorted_bks = sorted_bks[-max_bars:]

            lines.append(f"  {TERM_BOLD}{t('[시간대별 로그 볼륨]', '[Log Volume by Time]')}{TERM_RESET}  ({t('단위', 'Unit')}: {format_interval(self.interval_sec)})")
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

            lines.append(f"  {TERM_BOLD}[{t('활동 노드', 'Active Nodes')} Top {min(7, len(sorted_wn))}]{TERM_RESET}")
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
            lines.append(f"  {TERM_BOLD}{t('[포커스 노드]', '[Focus Node]')}{TERM_RESET}  {t('쿼리', 'query')}='{self.focus_query}'")
            lines.append(f"  {'─' * (w - 4)}")
            if self.focus_total <= 0:
                lines.append(f"  {t('(아직 매칭 로그 없음)', '(no matching logs yet)')}")
                lines.append("")
            else:
                win_parts = []
                for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
                    cnt = self.focus_win_levels.get(lvl, 0)
                    if cnt > 0:
                        win_parts.append(f"{color}{lvl}:{cnt:,}{TERM_RESET}")
                lines.append(
                    f"  {t('윈도우', 'Window')}: {t('총', 'Total')} {self.focus_win_total:,}{t('건', '')}"
                    f"  |  {'  '.join(win_parts) if win_parts else t('(없음)', '(none)')}"
                )

                cum_parts = []
                for lvl, color in [('FATAL', TERM_RED), ('ERROR', TERM_RED), ('WARN', TERM_YELLOW), ('INFO', TERM_RESET), ('DEBUG', TERM_DIM)]:
                    cnt = self.focus_total_levels.get(lvl, 0)
                    if cnt > 0:
                        cum_parts.append(f"{color}{lvl}:{cnt:,}{TERM_RESET}")
                lines.append(
                    f"  {t('누적', 'Cumul.')}  : {t('총', 'Total')} {self.focus_total:,}{t('건', '')}"
                    f"  |  {'  '.join(cum_parts) if cum_parts else t('(없음)', '(none)')}"
                )

                if self.focus_recent_alerts:
                    available_rows = max(rows - len(lines) - 3, 0)
                    alert_count = min(8, len(self.focus_recent_alerts), available_rows)
                    if alert_count > 0:
                        lines.append(f"  {TERM_BOLD}{t('[포커스 최근 에러/경고]', '[Focus Recent Errors/Warnings]')}{TERM_RESET}")
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
            lines.append(f"  {TERM_BOLD}{t('[최근 에러/경고]', '[Recent Errors/Warnings]')}{TERM_RESET}")
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

        # --- 통신 흐름 섹션 (comm_mode) ---
        if self.comm_mode and self.comm_events:
            lines.append(f"  {TERM_BOLD}{t('[통신 흐름]', '[Communication Flow]')}{TERM_RESET}  ({len(self.comm_events):,} {t('건 (윈도우)', 'in window')})")
            lines.append(f"  {'─' * (w - 4)}")

            # Top 5 node->topic flows in window
            flow_counts = defaultdict(int)
            win_topic_counts = defaultdict(lambda: defaultdict(int))
            for _, nd, tp, act in self.comm_events:
                flow_counts[(nd, tp)] += 1
                win_topic_counts[tp][act or 'unknown'] += 1

            top_flows = sorted(flow_counts.items(), key=lambda x: -x[1])[:5]
            for (nd, tp), cnt in top_flows:
                lines.append(f"  {nd:<30} → {tp:<25} {cnt:>6,}")

            lines.append("")

            # Top 5 topics with action ratio
            lines.append(f"  {TERM_BOLD}{t('[토픽 활동]', '[Topic Activity]')}{TERM_RESET}")
            lines.append(f"  {'─' * (w - 4)}")
            sorted_win_topics = sorted(win_topic_counts.items(),
                                       key=lambda x: -sum(x[1].values()))[:5]
            for tp, acts in sorted_win_topics:
                total_tp = sum(acts.values())
                act_strs = []
                for a in ('publish', 'subscribe', 'error', 'unknown'):
                    c = acts.get(a, 0)
                    if c > 0:
                        if a == 'error':
                            act_strs.append(f"{TERM_RED}{a}:{c}{TERM_RESET}")
                        else:
                            act_strs.append(f"{a}:{c}")
                lines.append(f"  {tp:<40} {total_tp:>6,}  {', '.join(act_strs)}")
            lines.append("")

            # Tracked values
            if self.comm_recent_values:
                lines.append(f"  {TERM_BOLD}{t('[추적 값]', '[Tracked Values]')}{TERM_RESET}")
                lines.append(f"  {'─' * (w - 4)}")
                for (tp, key), vals in sorted(self.comm_recent_values.items()):
                    if not vals:
                        continue
                    recent = list(vals)[-5:]
                    val_str = ", ".join(f"{v:.3f}" for _, v in recent)
                    lines.append(f"  {tp}/{key}: {val_str}")
                lines.append("")

            # Anomalies
            if self.comm_anomalies:
                lines.append(f"  {TERM_BOLD}{TERM_RED}{t('[통신 이상]', '[Comm Anomalies]')}{TERM_RESET}")
                recent_anoms = list(self.comm_anomalies)[-3:]
                for ts_a, atype, nd, tp, detail in recent_anoms:
                    dt_a = datetime.fromtimestamp(ts_a).strftime('%H:%M:%S')
                    lines.append(f"  {TERM_RED}{dt_a} [{atype}]{TERM_RESET} {nd} {tp} {detail[:60]}")
                lines.append("")

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
        comm_mode=args.comm,
        track_topics=args.track_topics,
        track_values=args.track_values,
        gap_threshold=args.gap_threshold,
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
    print(f"\n\n  {t('실시간 모니터링 종료', 'Real-time monitoring ended')}")
    print(f"  {t('총 처리 라인', 'Total processed lines')}: {monitor.total_lines:,}")
    print(f"  {t('파싱 성공', 'Parsed OK')}: {monitor.parsed_lines:,}")
    duration = time.time() - monitor.start_wall
    print(f"  {t('모니터링 시간', 'Monitoring duration')}: {int(duration // 60)}{t('분', 'm')} {int(duration % 60)}{t('초', 's')}")

    if monitor.total_levels:
        print(f"\n  {t('누적 로그 레벨', 'Cumulative log levels')}:")
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

        print(f"\n  {t('CSV 저장', 'CSV saved')}: {filepath}")
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

예시 (통신 분석):
  python3 analyze_log.py launch.log --comm --full         # 전체 통신 분석
  python3 analyze_log.py launch.log --comm --track-topic /cmd_vel  # 특정 토픽 추적
  python3 analyze_log.py launch.log --comm --track-value linear_x  # 값 변화 추적
  python3 analyze_log.py launch.log --comm --gap-threshold 5       # 갭 감지 5초
  python3 analyze_log.py launch.log -f --comm              # 실시간 통신 모니터링
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
    # 통신 분석 옵션
    parser.add_argument('--comm', action='store_true',
                        help=t('통신 데이터 내용 분석 모드', 'Communication data content analysis mode'))
    parser.add_argument('--track-topic', action='append', default=None, dest='track_topics',
                        help=t('상세 추적할 토픽 (반복 가능, 부분 매칭)',
                               'Topic to track in detail (repeatable, partial match)'))
    parser.add_argument('--track-value', action='append', default=None, dest='track_values',
                        help=t('추적할 데이터 값 키 이름 (반복 가능)',
                               'Data value key name to track (repeatable)'))
    parser.add_argument('--gap-threshold', type=float, default=10.0,
                        help=t('통신 갭 감지 임계값(초) [기본: 10.0]',
                               'Communication gap detection threshold in seconds [default: 10.0]'))

    parser.add_argument('--lang', '-L', choices=['ko', 'en'], default='en',
                        help='Output language / 출력 언어 (ko: 한국어, en: English) [default: en]')

    args = parser.parse_args()
    set_lang(args.lang)

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
            print(t("  y/n 로 입력하세요", "  Please enter y/n"))

    def _prompt_int(prompt: str, default=None, min_value=None):
        while True:
            s = _prompt_line(prompt, default=default)
            if s == '' and default is None:
                return None
            try:
                v = int(s)
            except ValueError:
                print(t("  숫자를 입력하세요", "  Please enter a number"))
                continue
            if min_value is not None and v < min_value:
                print(t(f"  {min_value} 이상으로 입력하세요", f"  Please enter {min_value} or higher"))
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
                print(t("  숫자를 입력하세요", "  Please enter a number"))
                continue
            if min_value is not None and v < min_value:
                print(t(f"  {min_value} 이상으로 입력하세요", f"  Please enter {min_value} or higher"))
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
                    print(t("  파일 경로를 입력하세요", "  Please enter a file path"))
                    continue
            p = os.path.expanduser(s)
            if os.path.isfile(p):
                return p
            print(t(f"  파일을 찾을 수 없습니다: {p}", f"  File not found: {p}"))

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
                    msg = t(f"디렉터리를 열 수 없습니다: {e}", f"Cannot open directory: {e}")
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
                        _safe_addnstr(stdscr, 0, 0, t("logfile 탐색", "Browse logfile"), max(0, w - 1))
                        _safe_addnstr(stdscr, 2, 0, t("터미널이 너무 작습니다", "Terminal too small"), max(0, w - 1))
                        _safe_addnstr(stdscr, 3, 0, t("q=직접입력", "q=manual input"), max(0, w - 1))
                        stdscr.refresh()
                    else:
                        header0 = t("logfile 탐색 (↑/↓, Enter=열기/선택, Backspace=상위, g=./log, q=직접입력)", "Browse logfile (↑/↓, Enter=open/select, Backspace=parent, g=./log, q=manual)")
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
                                label = t("직접 입력...", "Manual input...")
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
                        msg = t("./log 디렉터리가 없습니다", "./log directory not found")
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
                                msg = t(f"디렉터리를 찾을 수 없습니다: {next_dir}", f"Directory not found: {next_dir}")
                        idx = 0
                        top = 0
                        continue
                    # file
                    p = os.path.join(cur_dir, name)
                    if os.path.isfile(p):
                        return p
                    msg = t(f"파일을 찾을 수 없습니다: {p}", f"File not found: {p}")

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

                    title = t("logfile 선택 (↑/↓, Enter, q=직접입력)", "Select logfile (↑/↓, Enter, q=manual)")
                    _safe_addnstr(stdscr, 0, 0, title, max(0, w - 1))

                    if h < 6 or w < 24:
                        _safe_addnstr(stdscr, 2, 0, t("터미널이 너무 작습니다", "Terminal too small"), max(0, w - 1))
                        _safe_addnstr(stdscr, 3, 0, t("q=직접입력", "q=manual input"), max(0, w - 1))
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
                                left = t("탐색... (현재 디렉터리)", "Browse... (current dir)")
                                right = ""
                            elif kind == 'browse_log':
                                left = t("탐색... (./log)", "Browse... (./log)")
                                right = ""
                            else:
                                left = t("직접 입력...", "Manual input...")
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

        print(f"\n{t('logfile 선택:', 'Select logfile:')}")
        for i, p in enumerate(candidates, 1):
            meta = _logfile_meta_brief(p)
            if meta:
                print(f"  {i:2d}) {p}  ({meta})")
            else:
                print(f"  {i:2d}) {p}")
        print(f"   0) {t('직접 입력', 'Manual input')}")

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
            print(t(f"  0~{len(candidates)} 범위로 입력하세요", f"  Please enter 0~{len(candidates)}"))

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
        print(t("\n[interactive] 옵션을 선택하세요 (Enter=기본값)", "\n[interactive] Select options (Enter=default)"))
        print(t(
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
  comm(--comm)    : y / n          (통신 데이터 분석)
  track-topic     : /cmd_vel       (부분 매칭, 빈칸=없음, 쉼표 구분)
  track-value     : linear_x       (키 이름, 빈칸=없음, 쉼표 구분)
  gap-threshold   : 10.0           (초 단위)
""",
            """[examples] Input examples
  follow(-f)      : y / n
  full(--full)    : y / n
  interval(-i)    : 1h, 10m, 30s, 2(=2h)   (h/m/s supported, number only=hours)
  top-nodes(-t)   : 10, 20
  errors-only(-e) : y / n
  node(-n)        : motor_driver   (partial match, empty=none)
  focus-node      : tsd            (partial match, empty=none)
  csv(-c)         : reports/my_report.csv  (empty=auto)
  --from / --to   : 2026-01-27 | 2026-01-27 09:00 | 09:00  (today)
  comm(--comm)    : y / n          (communication data analysis)
  track-topic     : /cmd_vel       (partial match, empty=none, comma separated)
  track-value     : linear_x       (key name, empty=none, comma separated)
  gap-threshold   : 10.0           (seconds)
"""
        ))
        w = argparse.Namespace(**vars(existing_args))

        picked = _pick_logfile_for_wizard(default=w.logfile)
        if picked:
            w.logfile = picked
        else:
            w.logfile = _prompt_existing_file('logfile', default=w.logfile)
        w.follow = _prompt_bool(t('follow 모드(-f) 사용', 'Use follow mode (-f)'), default=bool(w.follow))

        w.full = _prompt_bool(t('전체 재분석(--full)', 'Full re-analysis (--full)'), default=bool(w.full))
        w.interval = _prompt_interval('interval(-i) (예: 1h, 10m, 30s)', default=str(w.interval))
        w.top_nodes = _prompt_int('top-nodes(-t)', default=w.top_nodes, min_value=1)
        w.errors_only = _prompt_bool('errors-only(-e)', default=bool(w.errors_only))

        node_default = w.node if w.node is not None else ''
        s = _prompt_line(t('node(-n) (부분 매칭, 빈칸=없음)', 'node(-n) (partial match, empty=none)'), default=node_default).strip()
        w.node = s or None

        focus_default = w.focus_node if w.focus_node is not None else ''
        s = _prompt_line(t('focus-node (부분 매칭, 빈칸=없음)', 'focus-node (partial match, empty=none)'), default=focus_default).strip()
        w.focus_node = s or None

        csv_default = w.csv if w.csv is not None else ''
        s = _prompt_line(t('csv(-c) (파일 경로, 빈칸=자동)', 'csv(-c) (file path, empty=auto)'), default=csv_default).strip()
        w.csv = s or None

        tf_default = w.time_from if w.time_from is not None else ''
        w.time_from = _prompt_datetime_optional(t('--from (빈칸=없음)', '--from (empty=none)'), default=tf_default)
        tt_default = w.time_to if w.time_to is not None else ''
        w.time_to = _prompt_datetime_optional(t('--to (빈칸=없음)', '--to (empty=none)'), default=tt_default)

        w.comm = _prompt_bool(t('통신 분석(--comm)', 'Communication analysis (--comm)'), default=bool(w.comm))
        if w.comm:
            tt_str = _prompt_line(
                t('track-topic (쉼표 구분, 빈칸=전체)', 'track-topic (comma separated, empty=all)'),
                default=','.join(w.track_topics) if w.track_topics else '',
            ).strip()
            w.track_topics = [s.strip() for s in tt_str.split(',') if s.strip()] or None
            tv_str = _prompt_line(
                t('track-value (쉼표 구분, 빈칸=없음)', 'track-value (comma separated, empty=none)'),
                default=','.join(w.track_values) if w.track_values else '',
            ).strip()
            w.track_values = [s.strip() for s in tv_str.split(',') if s.strip()] or None
            w.gap_threshold = _prompt_float(
                t('gap-threshold (초)', 'gap-threshold (sec)'), default=w.gap_threshold, min_value=0.1,
            )

        if w.follow:
            w.window = _prompt_interval(t('window(-w) (예: 5m, 10m)', 'window(-w) (e.g. 5m, 10m)'), default=str(w.window))
            w.refresh = _prompt_float(t('refresh(-r) (초)', 'refresh(-r) (sec)'), default=w.refresh, min_value=0.1)
            w.tail = _prompt_int(t('tail (0=끝부터)', 'tail (0=from end)'), default=w.tail, min_value=0)

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

        if a.comm:
            argv += ['--comm']
            if a.track_topics:
                for tp in a.track_topics:
                    argv += ['--track-topic', tp]
            if a.track_values:
                for tv in a.track_values:
                    argv += ['--track-value', tv]
            if a.gap_threshold != 10.0:
                argv += ['--gap-threshold', str(a.gap_threshold)]

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
            print(f"\n{t('취소됨', 'Cancelled')}")
            return

        print(f"\n{t('선택한 옵션:', 'Selected options:')}")
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
        if args.comm:
            print(f"  comm        : {args.comm}")
            print(f"  track-topic : {args.track_topics}")
            print(f"  track-value : {args.track_values}")
            print(f"  gap-threshold: {args.gap_threshold}")
        if args.follow:
            print(f"  window      : {args.window}")
            print(f"  refresh     : {args.refresh}")
            print(f"  tail        : {args.tail}")

        print(f"\n{t('CLI 힌트:', 'CLI hint:')}")
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

    comm_analyzer = None
    if args.comm:
        comm_analyzer = CommAnalyzer(
            interval_sec=interval_sec,
            node_filter=args.node,
            errors_only=args.errors_only,
            ts_from=ts_from,
            ts_to=ts_to,
            track_topics=args.track_topics,
            track_values=args.track_values,
            gap_threshold=args.gap_threshold,
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
                print(f"\n  {t(f'로그 로테이션 감지 (이전 {prev_size:,} → 현재 {file_size:,} bytes)', f'Log rotation detected (prev {prev_size:,} → current {file_size:,} bytes)')}")
                print(t("  전체 재분석을 수행합니다.", "  Performing full re-analysis."))
                clear_checkpoint(args.logfile)
                start_offset = 0
            elif file_size <= prev_size:
                # 변경 없음
                print(f"\n  {t('마지막 분석', 'Last analysis')}: {prev_at}")
                print(t(f"  새로 추가된 로그가 없습니다. (파일 크기: {file_size:,} bytes)", f"  No new logs added. (file size: {file_size:,} bytes)"))
                print(t("  전체 재분석을 하려면 --full 옵션을 사용하세요.\n", "  Use --full option for full re-analysis.\n"))
                return
            else:
                # 증분 분석
                incremental = True
                start_offset = prev_offset
                new_bytes = file_size - prev_offset
                print(t("\n  *** 증분 분석 모드 ***", "\n  *** Incremental analysis mode ***"))
                print(f"  {t('마지막 분석', 'Last analysis'):<13}: {prev_at}")
                if prev_ts:
                    print(f"  {t('마지막 로그', 'Last log'):<13}: {datetime.fromtimestamp(prev_ts).strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  {t('이전 위치', 'Previous offset'):<13}: {prev_offset:,} bytes ({prev_lines:,} {t('줄', 'lines')})")
                print(f"  {t('새 데이터', 'New data'):<13}: {new_bytes / (1024**2):.1f} MB")
    else:
        if args.full and cp:
            print(t("\n  --full: 체크포인트를 무시하고 전체 재분석합니다.", "\n  --full: Ignoring checkpoint, performing full re-analysis."))
            clear_checkpoint(args.logfile)

    # --- 분석 대상 크기 ---
    analyze_bytes = file_size - start_offset

    print(f"\n  {t('로그 파일', 'Log file')}: {args.logfile}")
    print(f"  {t('전체 크기', 'Total size')}: {file_size / (1024**3):.2f} GB")
    if incremental:
        print(f"  {t('분석 범위', 'Analysis range')}: {start_offset:,} ~ {file_size:,} bytes ({analyze_bytes / (1024**2):.1f} MB)")
    print(f"  {t('분석 단위', 'Interval')}: {format_interval(interval_sec)}")
    if args.node:
        print(f"  {t('노드 필터', 'Node filter')}: {args.node}")
    if args.errors_only:
        print(t("  모드: ERROR/WARN/FATAL만 분석", "  Mode: ERROR/WARN/FATAL only"))
    if args.comm:
        print(t("  모드: 통신 데이터 분석 활성화", "  Mode: Communication data analysis enabled"))
        if args.track_topics:
            print(f"  {t('추적 토픽', 'Track topics')}: {', '.join(args.track_topics)}")
        if args.track_values:
            print(f"  {t('추적 값', 'Track values')}: {', '.join(args.track_values)}")
        print(f"  {t('갭 임계값', 'Gap threshold')}: {args.gap_threshold}s")
    if ts_from is not None or ts_to is not None:
        from_str = datetime.fromtimestamp(ts_from).strftime('%Y-%m-%d %H:%M:%S') if ts_from else t('(처음)', '(start)')
        to_str = datetime.fromtimestamp(ts_to).strftime('%Y-%m-%d %H:%M:%S') if ts_to else t('(끝)', '(end)')
        print(f"  {t('시간 범위', 'Time range')}: {from_str} ~ {to_str}")
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
                if comm_analyzer is not None:
                    comm_analyzer.process_line(line)
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
                    mode_tag = t("[증분] ", "[INCR] ") if incremental else ""
                    sys.stderr.write(
                        f"\r  {mode_tag}{t('진행', 'Progress')}: {progress:>3}%  |  "
                        f"{analyzer.total_lines:>12,} {t('줄', 'lines')}  |  "
                        f"{speed:>6.1f} MB/s  |  "
                        f"{t('남은 시간', 'ETA')}: {eta_min}{t('분', 'm')} {eta_sec}{t('초', 's')}   "
                    )
                    sys.stderr.flush()

    except KeyboardInterrupt:
        print(f"\n\n  {t('[중단됨]', '[Interrupted]')} {t(f'{analyzer.total_lines:,} 줄까지 분석된 결과를 표시합니다.', f'Showing results for {analyzer.total_lines:,} lines analyzed.')}\n")

    elapsed = time.time() - start_time
    mode_tag = t("[증분] ", "[INCR] ") if incremental else ""
    sys.stderr.write(f"\r  {mode_tag}{t('완료', 'Done')}: {analyzer.total_lines:,} {t('줄 처리', 'lines processed')} ({elapsed:.1f}{t('초', 's')})\n\n")
    sys.stderr.flush()

    analyzer.print_report(top_nodes=args.top_nodes)

    # Communication analysis report
    if comm_analyzer is not None:
        comm_analyzer.print_report()

    # 체크포인트 저장
    last_ts = analyzer.ts_max if analyzer.ts_max != float('-inf') else None
    cumulative_lines = analyzer.total_lines
    if incremental and cp:
        cumulative_lines += cp.get('total_lines', 0)
    save_checkpoint(args.logfile, final_offset, cumulative_lines, last_ts, file_size)
    print(f"  {t('체크포인트 저장 완료', 'Checkpoint saved')} (offset: {final_offset:,} bytes, {cumulative_lines:,} {t('줄', 'lines')})")

    # CSV 보고서 자동 생성 (실행 날짜 기준)
    report_dir = _get_report_dir()

    if args.csv:
        # 사용자 지정 경로로 내보내기
        analyzer.export_csv_reports(output_dir=None, single_file=args.csv)
    else:
        # 자동: reports/ 디렉토리에 날짜별 CSV 생성
        analyzer.export_csv_reports(output_dir=report_dir)

    # Append comm sections to the CSV if comm mode active
    if comm_analyzer is not None and comm_analyzer.comm_lines > 0:
        # Determine CSV path that was just written
        if args.csv:
            csv_path = args.csv
        else:
            # Find the most recent CSV in reports/
            report_files = sorted(
                [f for f in os.listdir(report_dir) if f.startswith('log_report_') and f.endswith('.csv')],
                reverse=True,
            )
            csv_path = os.path.join(report_dir, report_files[0]) if report_files else None

        if csv_path and os.path.isfile(csv_path):
            with open(csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                comm_analyzer.export_csv_sections(writer)
            print(f"  {t('통신 분석 CSV 추가 완료', 'Comm analysis appended to CSV')}: {csv_path}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{t('취소됨', 'Cancelled')}")
        raise SystemExit(130)
