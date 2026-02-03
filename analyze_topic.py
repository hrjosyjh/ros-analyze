#!/usr/bin/env python3
"""
ROS2 Bag 토픽 통신 분석기

ROS2 bag 파일(.db3)을 읽어 토픽별 통신 통계를 분석합니다.
메시지 역직렬화 없이 메타데이터 기반으로 분석하므로 추가 의존성이 필요없습니다.

Usage:
    python3 analyze_topic.py recording.db3
    python3 analyze_topic.py *.db3 --topic /cmd_vel
    python3 analyze_topic.py recording.db3 --interval 10s --csv report.csv
    python3 analyze_topic.py recording.db3 --from "09:00" --to "10:00"
    python3 analyze_topic.py recording.db3 --gap-threshold 5.0 --lang ko
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime

from log_parser import parse_datetime_arg, parse_interval, bucket_key, bucket_label
from i18n import set_lang, t


# =============================================================================
#  Constants
# =============================================================================

RE_QOS_RELIABILITY = re.compile(r'reliability:\s*(\d+)')
RE_QOS_DURABILITY = re.compile(r'durability:\s*(\d+)')
RE_QOS_DEPTH = re.compile(r'depth:\s*(\d+)')

RELIABILITY_MAP = {'1': 'reliable', '2': 'best_effort'}
DURABILITY_MAP = {'1': 'transient_local', '2': 'volatile'}

MAX_GAPS_PER_TOPIC = 50


# =============================================================================
#  Helpers
# =============================================================================

def format_interval(sec):
    """Format an interval in seconds to a human-readable string."""
    if sec >= 3600:
        return f"{sec // 3600}{t('시간', 'h')}"
    elif sec >= 60:
        return f"{sec // 60}{t('분', 'm')}"
    else:
        return f"{sec}{t('초', 's')}"


def format_duration(sec):
    """Format a duration in seconds to Xd Xh Xm string."""
    days = int(sec // 86400)
    hours = int((sec % 86400) // 3600)
    mins = int((sec % 3600) // 60)
    return t(f'{days}일 {hours}시간 {mins}분', f'{days}d {hours}h {mins}m')


def format_size(nbytes):
    """Format byte size to human-readable string."""
    if nbytes >= 1024 * 1024:
        return f"{nbytes / (1024 * 1024):.1f} MB"
    elif nbytes >= 1024:
        return f"{nbytes / 1024:.1f} KB"
    else:
        return f"{nbytes} B"


def parse_qos_profiles(qos_text):
    """Parse QoS profile text from the topics table.

    Returns dict with keys: reliability, durability, history_depth.
    Values are human-readable strings or 'N/A'.
    """
    result = {'reliability': 'N/A', 'durability': 'N/A', 'history_depth': 'N/A'}
    if not qos_text:
        return result

    m = RE_QOS_RELIABILITY.search(qos_text)
    if m:
        result['reliability'] = RELIABILITY_MAP.get(m.group(1), m.group(1))

    m = RE_QOS_DURABILITY.search(qos_text)
    if m:
        result['durability'] = DURABILITY_MAP.get(m.group(1), m.group(1))

    m = RE_QOS_DEPTH.search(qos_text)
    if m:
        result['history_depth'] = m.group(1)

    return result


# =============================================================================
#  TopicAnalyzer
# =============================================================================

class TopicAnalyzer:
    """ROS2 bag (.db3) topic communication analyzer."""

    def __init__(self, interval_sec, topic_filter=None, gap_threshold=2.0,
                 ts_from=None, ts_to=None):
        self.interval_sec = interval_sec
        self.topic_filter = topic_filter
        self.gap_threshold = gap_threshold
        self.ts_from = ts_from
        self.ts_to = ts_to

        # Per-topic message count
        self.topic_count = defaultdict(int)

        # Per-topic running size stats (O(1) memory)
        self.topic_size_min = {}    # topic_name -> int
        self.topic_size_max = {}    # topic_name -> int
        self.topic_size_sum = defaultdict(int)

        # Per-topic time range
        self.topic_first_ts = {}    # topic_name -> float (seconds)
        self.topic_last_ts = {}     # topic_name -> float (seconds)

        # Per-topic communication gaps (bounded to top MAX_GAPS_PER_TOPIC per topic)
        # Each entry: (gap_duration, gap_start, gap_end)
        self.topic_gaps = defaultdict(list)

        # Time-bucketed counts: bucket_ts -> {topic_name: count}
        self.bucket_counts = defaultdict(lambda: defaultdict(int))

        # Topic metadata: topic_id -> {name, type, serialization_format, qos_profiles}
        self.topic_meta = {}

        # Global counters
        self.total_messages = 0
        self.ts_min = float('inf')
        self.ts_max = float('-inf')
        self.db_files_count = 0

        # Previous timestamp per topic for gap detection
        self._prev_ts = {}

    def process_db(self, db_path):
        """Open a .db3 file, read topics and iterate messages."""
        if not os.path.isfile(db_path):
            print(t(f"  파일을 찾을 수 없습니다: {db_path}",
                     f"  File not found: {db_path}"), file=sys.stderr)
            return

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.Error as e:
            print(t(f"  SQLite 열기 실패: {db_path} ({e})",
                     f"  Failed to open SQLite: {db_path} ({e})"), file=sys.stderr)
            return

        try:
            # Verify tables exist
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('topics', 'messages')"
            )
            tables = {row[0] for row in cursor}
            if 'topics' not in tables or 'messages' not in tables:
                print(t(f"  유효한 ROS2 bag 파일이 아닙니다: {db_path}",
                         f"  Not a valid ROS2 bag file: {db_path}"), file=sys.stderr)
                return

            # Read topic metadata
            id_to_name = {}
            cursor = conn.execute(
                "SELECT id, name, type, serialization_format, offered_qos_profiles FROM topics"
            )
            for row in cursor:
                topic_id, name, msg_type, ser_fmt, qos_text = row
                id_to_name[topic_id] = name
                if name not in self.topic_meta:
                    qos = parse_qos_profiles(qos_text)
                    self.topic_meta[name] = {
                        'type': msg_type or '',
                        'serialization_format': ser_fmt or '',
                        'qos': qos,
                    }

            # Apply topic filter to relevant IDs
            if self.topic_filter:
                filtered_ids = {tid for tid, name in id_to_name.items()
                                if self.topic_filter in name}
            else:
                filtered_ids = None  # all topics

            # Iterate messages — O(1) memory via cursor iteration
            cursor = conn.execute(
                "SELECT topic_id, timestamp, length(data) FROM messages ORDER BY timestamp"
            )

            msg_count = 0
            for topic_id, ts_ns, data_size in cursor:
                if filtered_ids is not None and topic_id not in filtered_ids:
                    continue

                topic_name = id_to_name.get(topic_id)
                if topic_name is None:
                    continue

                self.process_message(topic_name, ts_ns, data_size or 0)
                msg_count += 1

                if msg_count % 500000 == 0:
                    sys.stderr.write(
                        f"\r  {t('처리 중', 'Processing')} {db_path}: "
                        f"{msg_count:,} {t('메시지...', 'messages...')}"
                    )
                    sys.stderr.flush()

            if msg_count > 0:
                sys.stderr.write(
                    f"\r  {db_path}: {msg_count:,} {t('메시지 처리 완료', 'messages processed')}       \n"
                )
                sys.stderr.flush()

            self.db_files_count += 1

        except sqlite3.Error as e:
            print(t(f"  DB 읽기 오류: {db_path} ({e})",
                     f"  DB read error: {db_path} ({e})"), file=sys.stderr)
        finally:
            conn.close()

    def process_message(self, topic_name, ts_ns, data_size):
        """Update all running stats for a single message."""
        ts_sec = ts_ns / 1e9

        # Time range filter
        if self.ts_from is not None and ts_sec < self.ts_from:
            return
        if self.ts_to is not None and ts_sec > self.ts_to:
            return

        self.total_messages += 1

        # Global time range
        if ts_sec < self.ts_min:
            self.ts_min = ts_sec
        if ts_sec > self.ts_max:
            self.ts_max = ts_sec

        # Per-topic count
        self.topic_count[topic_name] += 1

        # Per-topic size stats
        if topic_name not in self.topic_size_min:
            self.topic_size_min[topic_name] = data_size
            self.topic_size_max[topic_name] = data_size
        else:
            if data_size < self.topic_size_min[topic_name]:
                self.topic_size_min[topic_name] = data_size
            if data_size > self.topic_size_max[topic_name]:
                self.topic_size_max[topic_name] = data_size
        self.topic_size_sum[topic_name] += data_size

        # Per-topic time range
        if topic_name not in self.topic_first_ts:
            self.topic_first_ts[topic_name] = ts_sec
        self.topic_last_ts[topic_name] = ts_sec

        # Gap detection
        if topic_name in self._prev_ts:
            gap = ts_sec - self._prev_ts[topic_name]
            if gap >= self.gap_threshold:
                gap_entry = (gap, self._prev_ts[topic_name], ts_sec)
                gaps = self.topic_gaps[topic_name]
                if len(gaps) < MAX_GAPS_PER_TOPIC:
                    gaps.append(gap_entry)
                else:
                    # Replace the smallest gap if current is larger
                    min_idx = min(range(len(gaps)), key=lambda i: gaps[i][0])
                    if gap > gaps[min_idx][0]:
                        gaps[min_idx] = gap_entry
        self._prev_ts[topic_name] = ts_sec

        # Time bucket
        bk = bucket_key(ts_sec, self.interval_sec)
        self.bucket_counts[bk][topic_name] += 1

    # =========================================================================
    #  Terminal Report
    # =========================================================================

    def print_report(self, top_topics=20):
        """Print the full terminal report with 6 sections."""
        if self.total_messages == 0:
            print(t("  메시지가 없습니다.", "  No messages found."))
            return

        duration = self.ts_max - self.ts_min
        sorted_topics = sorted(self.topic_count.items(), key=lambda x: (-x[1], x[0]))

        # --- Section 1: Overall Summary ---
        print("=" * 90)
        print(f"  {t('ROS2 Bag 토픽 통신 분석 보고서', 'ROS2 Bag Topic Communication Analysis Report')}")
        print("=" * 90)
        print()
        print(f"  {t('분석 Bag 파일 수', 'Bag files analyzed'):<22}: {self.db_files_count:>15,}")
        print(f"  {t('총 토픽 수', 'Total topics'):<22}: {len(self.topic_count):>15,}")
        print(f"  {t('총 메시지 수', 'Total messages'):<22}: {self.total_messages:>15,}")
        print(f"  {t('시간 범위', 'Time range'):<22}: {datetime.fromtimestamp(self.ts_min).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  {'':22}  ~ {datetime.fromtimestamp(self.ts_max).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  {t('총 기간', 'Duration'):<22}: {format_duration(duration)}")
        print(f"  {t('분석 단위', 'Analysis interval'):<22}: {format_interval(self.interval_sec)}")
        print()

        # --- Section 2: Per-Topic Statistics ---
        print("-" * 90)
        print(f"  [{t('토픽별 통계', 'Topic Statistics')} - {t('상위', 'Top')} {top_topics}{t('개', '')}]")
        print("-" * 90)

        header = (f"  {'Rank':>4}  {t('토픽', 'Topic'):<35} {t('타입', 'Type'):<28} "
                  f"{t('수량', 'Count'):>10}  {t('주파수', 'Hz'):>7}  {t('평균크기', 'Avg Size'):>10}")
        print(header)
        print("  " + "-" * 86)

        max_count = sorted_topics[0][1] if sorted_topics else 1

        for rank, (topic_name, count) in enumerate(sorted_topics[:top_topics], 1):
            msg_type = self.topic_meta.get(topic_name, {}).get('type', '')
            # Shorten type for display
            type_short = msg_type if len(msg_type) <= 27 else msg_type[:24] + '...'

            first_ts = self.topic_first_ts.get(topic_name, 0)
            last_ts = self.topic_last_ts.get(topic_name, 0)
            topic_duration = last_ts - first_ts
            hz = count / topic_duration if topic_duration > 0 else 0.0

            avg_size = self.topic_size_sum[topic_name] // count if count > 0 else 0

            bar_len = int(count / max_count * 25)
            bar = "\u2588" * bar_len

            print(f"  {rank:>4}  {topic_name:<35} {type_short:<28} "
                  f"{count:>10,}  {hz:>7.1f}  {avg_size:>10,}")
            print(f"        {bar}")

        if len(sorted_topics) > top_topics:
            others_count = sum(c for _, c in sorted_topics[top_topics:])
            others_num = len(sorted_topics) - top_topics
            pct = others_count / self.total_messages * 100 if self.total_messages > 0 else 0
            label = t(f'(기타 {others_num}개 토픽)', f'(other {others_num} topics)')
            print(f"        {label:<35} {'':28} {others_count:>10,}  ({pct:>5.1f}%)")
        print()

        # --- Section 3: Time-Bucketed Message Frequency ---
        print("-" * 90)
        print(f"  [{t('시간대별 메시지 빈도', 'Message Frequency by Time')}]")
        print("-" * 90)

        sorted_buckets = sorted(self.bucket_counts.keys())
        if sorted_buckets:
            bucket_totals = {}
            for bk in sorted_buckets:
                bucket_totals[bk] = sum(self.bucket_counts[bk].values())

            max_bt = max(bucket_totals.values()) if bucket_totals else 1
            avg_bt = sum(bucket_totals.values()) / len(bucket_totals) if bucket_totals else 0

            for bk in sorted_buckets:
                total = bucket_totals[bk]
                label = bucket_label(bk, self.interval_sec)
                bar_len = int(total / max_bt * 40) if max_bt > 0 else 0
                bar = "\u2588" * bar_len

                spike = ""
                if avg_bt > 0 and total > avg_bt * 2:
                    spike = t(" !! SPIKE", " !! SPIKE")

                print(f"  {label}  {total:>12,}  {bar}{spike}")
        print()

        # --- Section 4: Filtered Topic Timeline ---
        if self.topic_filter:
            matched_topics = [name for name in self.topic_count if self.topic_filter in name]
            if matched_topics:
                print("-" * 90)
                print(f"  [{t('필터링된 토픽 타임라인', 'Filtered Topic Timeline')} "
                      f"({t('필터', 'Filter')}: '{self.topic_filter}')]")
                print("-" * 90)

                for topic_name in sorted(matched_topics):
                    print(f"\n  {t('토픽', 'Topic')}: {topic_name}")
                    print(f"  {t('수량', 'Count')}: {self.topic_count[topic_name]:,}")
                    print()

                    # Per-bucket detail for this topic
                    topic_buckets = []
                    for bk in sorted_buckets:
                        cnt = self.bucket_counts[bk].get(topic_name, 0)
                        if cnt > 0:
                            topic_buckets.append((bk, cnt))

                    if topic_buckets:
                        max_tc = max(c for _, c in topic_buckets)
                        for bk, cnt in topic_buckets:
                            label = bucket_label(bk, self.interval_sec)
                            bar_len = int(cnt / max_tc * 35) if max_tc > 0 else 0
                            bar = "\u2588" * bar_len
                            print(f"    {label}  {cnt:>10,}  {bar}")
                    print()

        # --- Section 5: Communication Gap Detection ---
        all_gaps = []
        for topic_name, gaps in self.topic_gaps.items():
            for gap_dur, gap_start, gap_end in gaps:
                all_gaps.append((topic_name, gap_dur, gap_start, gap_end))

        all_gaps.sort(key=lambda x: -x[1])  # Sort by duration descending

        print("-" * 90)
        print(f"  [{t('통신 갭 감지', 'Communication Gaps')} (> {self.gap_threshold:.1f}s)]")
        print("-" * 90)

        if all_gaps:
            header = (f"  {t('토픽', 'Topic'):<35} {t('갭 시작', 'Gap Start'):<24} "
                      f"{t('갭 종료', 'Gap End'):<24} {t('지속시간', 'Duration'):>10}")
            print(header)
            print("  " + "-" * 86)

            for topic_name, gap_dur, gap_start, gap_end in all_gaps[:50]:
                start_str = datetime.fromtimestamp(gap_start).strftime('%Y-%m-%d %H:%M:%S.') + \
                            f"{int((gap_start % 1) * 1000):03d}"
                end_str = datetime.fromtimestamp(gap_end).strftime('%Y-%m-%d %H:%M:%S.') + \
                          f"{int((gap_end % 1) * 1000):03d}"
                topic_short = topic_name if len(topic_name) <= 34 else topic_name[:31] + '...'
                print(f"  {topic_short:<35} {start_str:<24} {end_str:<24} {gap_dur:>9.2f}s")
        else:
            print(t("  감지된 통신 갭이 없습니다.",
                     "  No communication gaps detected."))
        print()

        # --- Section 6: QoS Profiles ---
        print("-" * 90)
        print(f"  [{t('QoS 프로파일', 'QoS Profiles')}]")
        print("-" * 90)

        header = (f"  {t('토픽', 'Topic'):<35} {t('타입', 'Type'):<28} "
                  f"{t('신뢰성', 'Reliability'):<14} {t('내구성', 'Durability'):<16} "
                  f"{t('이력', 'History'):>7}")
        print(header)
        print("  " + "-" * 86)

        for topic_name, _ in sorted_topics:
            meta = self.topic_meta.get(topic_name, {})
            msg_type = meta.get('type', '')
            type_short = msg_type if len(msg_type) <= 27 else msg_type[:24] + '...'
            qos = meta.get('qos', {})
            reliability = qos.get('reliability', 'N/A')
            durability = qos.get('durability', 'N/A')
            history_depth = qos.get('history_depth', 'N/A')
            print(f"  {topic_name:<35} {type_short:<28} "
                  f"{reliability:<14} {durability:<16} {history_depth:>7}")
        print()

    # =========================================================================
    #  CSV Export
    # =========================================================================

    def export_csv(self, filepath):
        """Export analysis results to a CSV file with 4 sections."""
        sorted_topics = sorted(self.topic_count.items(), key=lambda x: (-x[1], x[0]))
        sorted_buckets = sorted(self.bucket_counts.keys())

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # --- Section 1: Topic Summary ---
            writer.writerow(['[Topic Summary]'])
            writer.writerow([
                'rank', 'topic_name', 'message_type', 'count',
                'frequency_hz', 'avg_size', 'min_size', 'max_size'
            ])
            for rank, (topic_name, count) in enumerate(sorted_topics, 1):
                meta = self.topic_meta.get(topic_name, {})
                msg_type = meta.get('type', '')
                first_ts = self.topic_first_ts.get(topic_name, 0)
                last_ts = self.topic_last_ts.get(topic_name, 0)
                topic_duration = last_ts - first_ts
                hz = f"{count / topic_duration:.1f}" if topic_duration > 0 else "0.0"
                avg_size = self.topic_size_sum[topic_name] // count if count > 0 else 0
                min_size = self.topic_size_min.get(topic_name, 0)
                max_size = self.topic_size_max.get(topic_name, 0)
                writer.writerow([rank, topic_name, msg_type, count,
                                 hz, avg_size, min_size, max_size])

            # --- Section 2: Message Frequency by Time ---
            if sorted_buckets:
                writer.writerow([])
                writer.writerow(['[Message Frequency by Time]'])
                writer.writerow([
                    'time_bucket', 'total',
                    'top1_name', 'top1_count',
                    'top2_name', 'top2_count',
                    'top3_name', 'top3_count'
                ])
                for bk in sorted_buckets:
                    label = bucket_label(bk, self.interval_sec)
                    topics_in_bk = self.bucket_counts[bk]
                    total = sum(topics_in_bk.values())
                    top3 = sorted(topics_in_bk.items(), key=lambda x: (-x[1], x[0]))[:3]
                    row = [label, total]
                    for i in range(3):
                        if i < len(top3):
                            row.extend([top3[i][0], top3[i][1]])
                        else:
                            row.extend(['', 0])
                    writer.writerow(row)

            # --- Section 3: Communication Gaps ---
            all_gaps = []
            for topic_name, gaps in self.topic_gaps.items():
                for gap_dur, gap_start, gap_end in gaps:
                    all_gaps.append((topic_name, gap_dur, gap_start, gap_end))
            all_gaps.sort(key=lambda x: -x[1])

            if all_gaps:
                writer.writerow([])
                writer.writerow(['[Communication Gaps]'])
                writer.writerow(['topic_name', 'gap_start', 'gap_end', 'gap_duration_sec'])
                for topic_name, gap_dur, gap_start, gap_end in all_gaps:
                    start_str = datetime.fromtimestamp(gap_start).strftime('%Y-%m-%d %H:%M:%S.') + \
                                f"{int((gap_start % 1) * 1000):03d}"
                    end_str = datetime.fromtimestamp(gap_end).strftime('%Y-%m-%d %H:%M:%S.') + \
                              f"{int((gap_end % 1) * 1000):03d}"
                    writer.writerow([topic_name, start_str, end_str, f"{gap_dur:.3f}"])

            # --- Section 4: QoS Profiles ---
            writer.writerow([])
            writer.writerow(['[QoS Profiles]'])
            writer.writerow(['topic_name', 'message_type', 'reliability',
                             'durability', 'history_depth'])
            for topic_name, _ in sorted_topics:
                meta = self.topic_meta.get(topic_name, {})
                msg_type = meta.get('type', '')
                qos = meta.get('qos', {})
                writer.writerow([
                    topic_name, msg_type,
                    qos.get('reliability', 'N/A'),
                    qos.get('durability', 'N/A'),
                    qos.get('history_depth', 'N/A'),
                ])

        print(f"  {t('CSV 저장', 'CSV saved')}: {filepath}")
        print()


# =============================================================================
#  Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description=t('ROS2 Bag 토픽 통신 분석기', 'ROS2 Bag Topic Communication Analyzer'),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 analyze_topic.py recording.db3
  python3 analyze_topic.py *.db3 --topic /cmd_vel
  python3 analyze_topic.py recording.db3 --interval 10s --csv report.csv
  python3 analyze_topic.py recording.db3 --from "09:00" --to "10:00"
  python3 analyze_topic.py recording.db3 --gap-threshold 5.0 --lang ko
        """
    )

    parser.add_argument('db_files', nargs='+', metavar='DB_FILE',
                        help=t('.db3 파일 경로 (여러 개 가능)', '.db3 file paths (multiple allowed)'))
    parser.add_argument('--topic', '-T', default=None,
                        help=t('토픽 필터 (부분 매칭)', 'Topic filter (partial match)'))
    parser.add_argument('--interval', '-i', default='10s',
                        help=t('시간 버킷 간격 (예: 10s, 1m, 5m) [기본: 10s]',
                               'Time bucket interval (e.g. 10s, 1m, 5m) [default: 10s]'))
    parser.add_argument('--from', dest='time_from', default=None,
                        help=t('분석 시작 시각 (예: "2026-01-27", "09:00")',
                               'Start time filter (e.g. "2026-01-27", "09:00")'))
    parser.add_argument('--to', dest='time_to', default=None,
                        help=t('분석 종료 시각 (예: "2026-01-28", "18:00")',
                               'End time filter (e.g. "2026-01-28", "18:00")'))
    parser.add_argument('--gap-threshold', '-g', type=float, default=2.0,
                        help=t('통신 갭 감지 임계값 (초) [기본: 2.0]',
                               'Communication gap detection threshold (seconds) [default: 2.0]'))
    parser.add_argument('--top-topics', '-t', type=int, default=20,
                        help=t('표시할 상위 토픽 수 [기본: 20]',
                               'Number of top topics to display [default: 20]'))
    parser.add_argument('--csv', '-c', default=None,
                        help=t('CSV 출력 경로', 'CSV output path'))
    parser.add_argument('--lang', '-L', choices=['ko', 'en'], default='en',
                        help='Output language (ko/en) [default: en]')

    args = parser.parse_args()
    set_lang(args.lang)

    # Parse interval
    try:
        interval_sec = parse_interval(args.interval)
    except ValueError as e:
        parser.error(str(e))

    # Parse time range
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

    analyzer = TopicAnalyzer(
        interval_sec=interval_sec,
        topic_filter=args.topic,
        gap_threshold=args.gap_threshold,
        ts_from=ts_from,
        ts_to=ts_to,
    )

    # Print analysis parameters
    print()
    print(f"  {t('분석 대상', 'Target files')}: {len(args.db_files)} {t('개 파일', 'file(s)')}")
    print(f"  {t('분석 단위', 'Interval')}: {format_interval(interval_sec)}")
    if args.topic:
        print(f"  {t('토픽 필터', 'Topic filter')}: {args.topic}")
    print(f"  {t('갭 임계값', 'Gap threshold')}: {args.gap_threshold:.1f}s")
    if ts_from is not None or ts_to is not None:
        from_str = datetime.fromtimestamp(ts_from).strftime('%Y-%m-%d %H:%M:%S') if ts_from else t('(처음)', '(start)')
        to_str = datetime.fromtimestamp(ts_to).strftime('%Y-%m-%d %H:%M:%S') if ts_to else t('(끝)', '(end)')
        print(f"  {t('시간 범위', 'Time range')}: {from_str} ~ {to_str}")
    print()

    # Process each db file
    try:
        for db_path in args.db_files:
            sys.stderr.write(f"  {t('분석 중', 'Analyzing')}: {db_path}\n")
            sys.stderr.flush()
            analyzer.process_db(db_path)
    except KeyboardInterrupt:
        msg = t('[중단됨] 현재까지 분석된 결과를 표시합니다.',
                '[Interrupted] Showing results analyzed so far.')
        print(f"\n\n  {msg}\n")

    # Print report
    analyzer.print_report(top_topics=args.top_topics)

    # CSV export
    if args.csv:
        analyzer.export_csv(args.csv)
    elif analyzer.total_messages > 0:
        # Auto-generate CSV in reports/ directory
        report_dir = os.path.join(os.getcwd(), 'reports')
        os.makedirs(report_dir, exist_ok=True)
        now_str = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        auto_csv = os.path.join(report_dir, f"topic_report_{now_str}.csv")
        analyzer.export_csv(auto_csv)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{t('취소됨', 'Cancelled')}")
        raise SystemExit(130)
