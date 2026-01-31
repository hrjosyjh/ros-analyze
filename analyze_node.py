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
    print(f" ROS 2 System Analysis Report")
    print(f"{'='*90}")
    print(f" {'Node Name':<40} | {'Total':>8} | {'Errors':>6} | {'Warn':>6} | {'FPS':>5} | {'Duration'}")
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
        print("Node not found.")
        return

    n = node_stats
    duration = n.get_duration()
    
    print(f"\n{'='*80}")
    print(f" Detailed Analysis: {n.name}")
    print(f"{'='*80}")
    print(f" - Total Logs: {n.count:,}")
    print(f" - First Log : {datetime.fromtimestamp(n.first_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" - Last Log  : {datetime.fromtimestamp(n.last_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" - Duration  : {duration:.2f} seconds")
    print(f" - Log Rate  : {n.count / duration:.1f} lines/sec" if duration > 0 else " - Log Rate : N/A")
    
    print(f"\n [Log Level Distribution]")
    for level, count in n.levels.items():
        bar = "█" * int((count / n.count) * 50)
        print(f"   {level:<5} : {count:>6,} {bar}")

    # Timeline visualization
    print(f"\n [Activity Timeline (Logs per Minute)]")
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
        print(f"\n [Top Error/Warning Patterns]")
        
        # Group by "cleaned" message
        patterns = Counter([x[3] for x in n.error_samples])
        
        for clean_msg, count in patterns.most_common(5):
            # Find original message for this pattern
            example = next(x[2] for x in n.error_samples if x[3] == clean_msg)
            print(f"   ({count} occurrences)")
            print(f"   └── {example[:120]}...")
            print()
    else:
        print("\n ✅ No Errors or Warnings detected.")
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
                        help='분석 시작 시각 (예: "2026-01-27", "2026-01-27 09:00", "09:00")')
    parser.add_argument('--to', dest='time_to', default=None,
                        help='분석 종료 시각 (예: "2026-01-28", "2026-01-27 18:00", "18:00")')

    args = parser.parse_args()

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

    print(f"Analyzing {args.logfile}...")
    if ts_from is not None or ts_to is not None:
        from_str = datetime.fromtimestamp(ts_from).strftime('%Y-%m-%d %H:%M:%S') if ts_from else '(처음)'
        to_str = datetime.fromtimestamp(ts_to).strftime('%Y-%m-%d %H:%M:%S') if ts_to else '(끝)'
        print(f"  시간 범위: {from_str} ~ {to_str}")

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
                    sys.stdout.write(f"\rProcessed {total_lines:,} lines...")
                    sys.stdout.flush()

    except FileNotFoundError:
        print(f"Error: File '{args.logfile}' not found.")
        sys.exit(1)

    print(f"\rProcessed {total_lines:,} lines. Complete.      ")

    if args.node:
        # Find partial matches
        matched = [n for n in nodes.values() if args.node in n.name]
        if not matched:
            print(f"No nodes matching '{args.node}' found.")
        else:
            for n in matched:
                print_node_detail(n)
    else:
        print_global_summary(nodes, args.errors_only)

if __name__ == "__main__":
    main()