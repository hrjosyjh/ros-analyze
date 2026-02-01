"""Shared ROS2 launch.log parsing helpers (stdlib-only).

This module centralizes the hot-path parsing utilities used by:
- analyze_log.py
- analyze_node.py

Behavior must remain identical to the original inline implementations.
"""

import re
from datetime import datetime
from typing import Optional, Tuple

from i18n import t


# =============================================================================
#  Log line parser (regexes/constants)
# =============================================================================

# Pattern 1: "TIMESTAMP [INFO] [node]: message" (ROS2 launch format)
RE_LAUNCH = re.compile(
    r'^(\d+\.\d+)\s+\[(INFO|ERROR|WARN|DEBUG|FATAL)\]\s+\[([^\]]+)\]'
)

# Pattern 2: "TIMESTAMP [node] [LEVEL] [ros_ts] [ros_node]: message" (ROS2 logging)
RE_ROS2LOG = re.compile(
    r'^(\d+\.\d+)\s+\[([^\]]+)\]\s+\[(INFO|ERROR|WARN|DEBUG|FATAL)\]'
)

# Pattern 3: "TIMESTAMP [node] ANSI...[HH:MM:SS] [src(line)]func() msg" (node direct output)
# ANSI color codes are used to infer log level.
RE_NODE_DIRECT = re.compile(
    r'^(\d+\.\d+)\s+\[([^\]]+)\]'
)

# ANSI escape stripping
RE_ANSI = re.compile(r'\x1b\[[0-9;]*m')

# Infer log level from color codes: 1;31m=ERROR(red), 1;33m=WARN(yellow), 1;37m=INFO(white)
RE_COLOR_LEVEL = re.compile(r'\x1b\[1;(\d+)m')

COLOR_TO_LEVEL = {
    '31': 'ERROR',   # red
    '33': 'WARN',    # yellow
    '37': 'INFO',    # white
    '32': 'DEBUG',   # green
    '35': 'FATAL',   # magenta
}


def parse_line(line: str) -> Optional[Tuple[float, str, str]]:
    """Parse a log line into (timestamp, node, level); returns None on failure."""

    # Pattern 1: launch logs
    m = RE_LAUNCH.match(line)
    if m:
        ts = float(m.group(1))
        level = m.group(2)
        node = m.group(3).rstrip(':')
        return ts, node, level

    # Pattern 2: ROS2 logging format
    m = RE_ROS2LOG.match(line)
    if m:
        ts = float(m.group(1))
        node = m.group(2)
        level = m.group(3)
        return ts, node, level

    # Pattern 3: node direct output (infer level from color)
    m = RE_NODE_DIRECT.match(line)
    if m:
        ts = float(m.group(1))
        node = m.group(2)
        cm = RE_COLOR_LEVEL.search(line)
        if cm:
            level = COLOR_TO_LEVEL.get(cm.group(1), 'INFO')
        else:
            level = 'INFO'
        return ts, node, level

    return None


# =============================================================================
#  Interval / bucket helpers
# =============================================================================

def parse_datetime_arg(s: str) -> float:
    """Parse a user-provided datetime string into a Unix timestamp.

    Accepted formats:
      - "2026-01-27"             → start of that day (00:00:00)
      - "2026-01-27 09:00"       → that date at 09:00:00
      - "2026-01-27 09:00:00"    → exact datetime
      - "09:00" or "09:00:00"    → today at that time

    Returns epoch float.  Raises ValueError on parse failure.
    """
    s = s.strip()

    # Full datetime with seconds
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.timestamp()
        except ValueError:
            continue

    # Time-only (today)
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            t = datetime.strptime(s, fmt)
            now = datetime.now()
            dt = now.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
            return dt.timestamp()
        except ValueError:
            continue

    raise ValueError(
        t(f"시간 형식을 인식할 수 없습니다: '{s}'\n"
          f"  지원 형식: 2026-01-27, 2026-01-27 09:00, 2026-01-27 09:00:00, 09:00, 09:00:00",
          f"Unrecognized time format: '{s}'\n"
          f"  Supported formats: 2026-01-27, 2026-01-27 09:00, 2026-01-27 09:00:00, 09:00, 09:00:00")
    )


def parse_interval(interval_str: str) -> int:
    """Convert an interval string into seconds.

    Accepts:
      - "10m", "30s", "1h"
      - "2" (defaults to hours)

    Raises ValueError on invalid or non-positive intervals.
    """

    raw = interval_str
    s = interval_str.strip().lower()
    if not s:
        raise ValueError("interval must be a positive duration like '10m', '30s', or '1h'")

    m = re.fullmatch(r'(\d+)([hms]?)', s)
    if not m:
        raise ValueError(
            f"invalid interval '{raw}'; expected <int>[h|m|s] like '10m', '30s', '1h'"
        )

    value = int(m.group(1))
    if value <= 0:
        raise ValueError(
            f"invalid interval '{raw}'; value must be > 0 (example: '1m')"
        )

    unit = m.group(2) or 'h'
    mult = {'h': 3600, 'm': 60, 's': 1}[unit]
    return value * mult


def bucket_key(ts: float, interval_sec: int) -> int:
    """Bucket a timestamp into an interval-aligned epoch-second key."""

    return int(ts // interval_sec) * interval_sec


def bucket_label(bucket_ts: int, interval_sec: int) -> str:
    """Convert a bucket key to a human-readable label."""

    dt = datetime.fromtimestamp(bucket_ts)
    if interval_sec >= 3600:
        return dt.strftime('%Y-%m-%d %H:00')
    elif interval_sec >= 60:
        return dt.strftime('%Y-%m-%d %H:%M')
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S')
