#!/usr/bin/env python3
""".
ROS 2 Node Log Analyzer
사용법:
    1. 전체 노드 요약:
       python3 analyze_node.py launch.log

    2. 특정 노드 상세 분석:
       python3 analyze_node.py launch.log --node motor_driver

    3. 에러가 발생한 노드만 보기:
       python3 analyze_node.py launch.log --errors-only
"""

import re
import sys
import argparse
from datetime import datetime
from collections import defaultdict, Counter

from log_parser import parse_datetime_arg
from i18n import set_lang, t

# ==============================================================================
#  Log Parsing Logic
# ==============================================================================

# Regex patterns for different ROS 2 log formats
# Format A: [INFO] [timestamp] [node_name]: message
# Format B: 123456.789 [INFO] [node_name]: message (Launch prefix)
# Format C: 123456.789 [node_name] [INFO] ...
RE_LAUNCH_PREFIX = re.compile(r'^(\d+\.\d+)\s+\[(INFO|ERROR|WARN|DEBUG|FATAL)\]\s+\[([^\]]+)\]\s*(.*)$')
RE_ROS2_FORMAT   = re.compile(r'^(\d+\.\d+)\s+\[([^\]]+)\]\s+\[(INFO|ERROR|WARN|DEBUG|FATAL)\]\s*(.*)$')
RE_SIMPLE_NODE   = re.compile(r'^(\d+\.\d+)\s+\[([^\]]+)\]\s*(.*)$')

# ANSI Color codes for level inference (if text level is missing)
ANSI_COLOR_MAP = {
    '31': 'ERROR', '1;31': 'ERROR',  # Red
    '33': 'WARN',  '1;33': 'WARN',   # Yellow
    '32': 'DEBUG', '1;32': 'DEBUG',  # Green
}

def parse_line(line):
    """
    Parses a single log line and returns (timestamp, node_name, level, message).
    """
    line = line.strip()
    
    # Try Format A (Launch Prefix with Level first)
    m = RE_LAUNCH_PREFIX.match(line)
    if m:
        ts, level, node, msg = m.groups()
        return float(ts), node.strip(), level, msg

    # Try Format B (Node first, then Level)
    m = RE_ROS2_FORMAT.match(line)
    if m:
        ts, node, level, msg = m.groups()
        return float(ts), node.strip(), level, msg

    # Try Format C (Simple Node prefix, check for ANSI colors or assume INFO)
    m = RE_SIMPLE_NODE.match(line)
    if m:
        ts, node, content = m.groups()
        level = 'INFO'
        
        # Check for ANSI color codes to infer level
        if '\x1b[' in line:
            for code, mapped_level in ANSI_COLOR_MAP.items():
                if f'\x1b[{code}m' in line:
                    level = mapped_level
                    break
        
        # Clean message (remove ANSI codes for readability)
        msg = re.sub(r'\x1b\[[0-9;]*m', '', content)
        return float(ts), node.strip(), level, msg

    return None

# ==============================================================================
#  Analysis Classes
# ==============================================================================

class NodeStats:
    def __init__(self, name):
        self.name = name
        self.count = 0
        self.levels = defaultdict(int)
        self.first_ts = float('inf')
        self.last_ts = float('-inf')
        self.error_samples = []  # Store unique error messages
        self.activity_timeline = defaultdict(int) # Bucketized by minute

    def add(self, ts, level, msg):
        self.count += 1
        self.levels[level] += 1
        self.first_ts = min(self.first_ts, ts)
        self.last_ts = max(self.last_ts, ts)
        
        # Timeline (1-minute buckets)
        bucket = int(ts // 60)
        self.activity_timeline[bucket] += 1

        # Store Error Samples (Simple deduplication)
        if level in ['ERROR', 'FATAL', 'WARN']:
            # Remove timestamps/numbers from msg to group similar errors
            clean_msg = re.sub(r'\d+', 'N', msg[:100]) 
            if len(self.error_samples) < 50: # Limit memory usage
                self.error_samples.append((ts, level, msg, clean_msg))

    def get_duration(self):
        if self.count == 0: return 0
        return self.last_ts - self.first_ts

    def get_error_count(self):
        return self.levels['ERROR'] + self.levels['FATAL'] + self.levels['WARN']

# ==============================================================================
#  Reporting Functions
# ==============================================================================

def print_global_summary(nodes, errors_only=False):
    print(f"\n{'='*90}")
    print(f" {t('ROS 2 시스템 분석 보고서', 'ROS 2 System Analysis Report')}")
    print(f"{'='*90}")
    print(f" {t('노드명', 'Node Name'):<40} | {t('합계', 'Total'):>8} | {t('에러', 'Errors'):>6} | {t('경고', 'Warn'):>6} | {'FPS':>5} | {t('기간', 'Duration')}")
    print(f"{'-'*90}")

    # Sort by error count (descending) then total count
    sorted_nodes = sorted(nodes.values(), key=lambda x: (x.get_error_count(), x.count), reverse=True)

    for n in sorted_nodes:
        err_count = n.levels['ERROR'] + n.levels['FATAL']
        warn_count = n.levels['WARN']
        
        if errors_only and (err_count + warn_count) == 0:
            continue

        duration = n.get_duration()
        fps = n.count / duration if duration > 0 else 0
        dur_str = f"{duration:.1f}s"
        
        # Highlight high error nodes
        prefix = "🔴" if err_count > 0 else "  "
        
        print(f" {prefix}{n.name:<38} | {n.count:>8,} | {err_count:>6} | {warn_count:>6} | {fps:>5.1f} | {dur_str}")

    print(f"{'='*90}\n")


def print_node_detail(node_stats):
    if not node_stats:
        print(t("노드를 찾을 수 없습니다.", "Node not found."))
        return

    n = node_stats
    duration = n.get_duration()

    print(f"\n{'='*80}")
    print(f" {t('상세 분석', 'Detailed Analysis')}: {n.name}")
    print(f"{'='*80}")
    print(f" - {t('총 로그 수', 'Total Logs')}: {n.count:,}")
    print(f" - {t('첫 로그', 'First Log')} : {datetime.fromtimestamp(n.first_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" - {t('마지막 로그', 'Last Log')}  : {datetime.fromtimestamp(n.last_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" - {t('기간', 'Duration')}  : {duration:.2f} {t('초', 'seconds')}")
    print(f" - {t('로그 처리율', 'Log Rate')}  : {n.count / duration:.1f} lines/sec" if duration > 0 else f" - {t('로그 처리율', 'Log Rate')} : N/A")

    print(f"\n [{t('로그 레벨 분포', 'Log Level Distribution')}]")
    for level, count in n.levels.items():
        bar = "█" * int((count / n.count) * 50)
        print(f"   {level:<5} : {count:>6,} {bar}")

    # Timeline visualization
    print(f"\n [{t('활동 타임라인 (분당 로그 수)', 'Activity Timeline (Logs per Minute)')}]")
    if n.activity_timeline:
        min_bucket = min(n.activity_timeline.keys())
        max_bucket = max(n.activity_timeline.keys())
        
        # Normalize for bar chart
        max_val = max(n.activity_timeline.values())
        
        for b in range(min_bucket, max_bucket + 1):
            val = n.activity_timeline.get(b, 0)
            if val == 0: continue
            
            ts_str = datetime.fromtimestamp(b * 60).strftime('%H:%M')
            bar_len = int((val / max_val) * 40)
            print(f"   {ts_str} : {val:>5} {'#' * bar_len}")

    # Error Analysis
    err_count = n.levels['ERROR'] + n.levels['FATAL'] + n.levels['WARN']
    if err_count > 0:
        print(f"\n [{t('상위 에러/경고 패턴', 'Top Error/Warning Patterns')}]")

        # Group by "cleaned" message
        patterns = Counter([x[3] for x in n.error_samples])

        for clean_msg, count in patterns.most_common(5):
            # Find original message for this pattern
            example = next(x[2] for x in n.error_samples if x[3] == clean_msg)
            print(f"   ({count} {t('회 발생', 'occurrences')})")
            print(f"   └── {example[:120]}...")
            print()
    else:
        print(t("\n ✅ 에러 또는 경고가 없습니다.", "\n ✅ No Errors or Warnings detected."))
    print("\n")


# ==============================================================================
#  Main Execution
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Analyze ROS 2 Log Files")
    parser.add_argument("logfile", help="Path to the log file (e.g., launch.log)")
    parser.add_argument("--node", help="Specific node name to analyze (substring match)")
    parser.add_argument("--errors-only", action="store_true", help="Only show nodes with errors in summary")
    parser.add_argument('--from', dest='time_from', default=None,
                        help='Start time / 분석 시작 시각 (e.g. "2026-01-27", "2026-01-27 09:00", "09:00")')
    parser.add_argument('--to', dest='time_to', default=None,
                        help='End time / 분석 종료 시각 (e.g. "2026-01-28", "2026-01-27 18:00", "18:00")')
    parser.add_argument('--lang', '-L', choices=['ko', 'en'], default='ko',
                        help='Output language / 출력 언어 (ko: 한국어, en: English) [default: ko]')

    args = parser.parse_args()
    set_lang(args.lang)

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

    nodes = {}
    total_lines = 0

    print(t(f"{args.logfile} 분석 중...", f"Analyzing {args.logfile}..."))
    if ts_from is not None or ts_to is not None:
        from_str = datetime.fromtimestamp(ts_from).strftime('%Y-%m-%d %H:%M:%S') if ts_from else t('(처음)', '(start)')
        to_str = datetime.fromtimestamp(ts_to).strftime('%Y-%m-%d %H:%M:%S') if ts_to else t('(끝)', '(end)')
        print(f"  {t('시간 범위', 'Time range')}: {from_str} ~ {to_str}")

    try:
        with open(args.logfile, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                total_lines += 1
                result = parse_line(line)

                if result:
                    ts, node_name, level, msg = result

                    # 시간 범위 필터
                    if ts_from is not None and ts < ts_from:
                        continue
                    if ts_to is not None and ts > ts_to:
                        continue

                    if node_name not in nodes:
                        nodes[node_name] = NodeStats(node_name)

                    nodes[node_name].add(ts, level, msg)

                if total_lines % 100000 == 0:
                    sys.stdout.write(f"\r{t('처리 중', 'Processed')} {total_lines:,} {t('줄...', 'lines...')}")
                    sys.stdout.flush()

    except FileNotFoundError:
        print(t(f"에러: '{args.logfile}' 파일을 찾을 수 없습니다.", f"Error: File '{args.logfile}' not found."))
        sys.exit(1)

    print(f"\r{t('처리 완료', 'Processed')} {total_lines:,} {t('줄. 완료.', 'lines. Complete.')}      ")

    if args.node:
        # Find partial matches
        matched = [n for n in nodes.values() if args.node in n.name]
        if not matched:
            print(t(f"'{args.node}'에 매칭되는 노드가 없습니다.", f"No nodes matching '{args.node}' found."))
        else:
            for n in matched:
                print_node_detail(n)
    else:
        print_global_summary(nodes, args.errors_only)

if __name__ == "__main__":
    main()