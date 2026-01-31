#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
ROS 2 Node Monitor - Interactive Explorer (Scrolling + Density Control)
기능:
  - 노드 숫자로 선택
  - Enter/Space: 트리 펼치기/접기
  - +/- 키: 화면 밀도 조절 (폰트 크기 대안)
  - Up/Down: 이동 및 스크롤
"""

import sys
import os
import time
import threading
import argparse
import rclpy
import select
import termios
import tty
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.qos import QoSProfile, ReliabilityPolicy
from rosidl_runtime_py.utilities import get_message

try:
    import argcomplete
except ImportError:
    argcomplete = None

# ==============================================================================
#  Key Input Utility
# ==============================================================================
class KeyPoller:
    def __enter__(self):
        try:
            self.fd = sys.stdin.fileno()
            self.old_settings = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        except:
            self.fd = None
        return self

    def __exit__(self, type, value, traceback):
        if self.fd is not None:
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old_settings)

    def poll(self):
        if self.fd is not None and select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            ch = sys.stdin.read(1)
            if ch == '\x1b':
                seq = sys.stdin.read(2)
                if seq == '[A': return 'UP'
                if seq == '[B': return 'DOWN'
            return ch
        return None

# ==============================================================================
#  Autocompletion & Selection
# ==============================================================================
def get_active_nodes(prefix, parsed_args, **kwargs):
    if not rclpy.ok(): rclpy.init()
    temp = rclpy.create_node('_ac')
    nodes = temp.get_node_names()
    temp.destroy_node()
    return [n for n in nodes if n.startswith(prefix)]

def select_node_by_number(node_list):
    if not node_list: return None
    print("\033c", end="")
    print(f"\n{'='*60}\n Active ROS 2 Nodes\n{'='*60}")
    mid = (len(node_list) + 1) // 2
    for i in range(mid):
        left = f"[{i+1}] {node_list[i]}"
        right = f"[{i+mid+1}] {node_list[i+mid]}" if i + mid < len(node_list) else ""
        print(f"{left:<40} {right}")
    print(f"{ '='*60}")
    while True:
        try:
            choice = input(f"\nSelect (1-{len(node_list)}, 'q' quit): ").strip()
            if choice.lower() == 'q': return None
            idx = int(choice) - 1
            if 0 <= idx < len(node_list): return node_list[idx]
        except (ValueError, KeyboardInterrupt): return None

# ==============================================================================
#  Monitoring & Formatting
# ==============================================================================
class TopicMonitor:
    def __init__(self, name, type_str):
        self.name, self.type_str = name, type_str
        self.hz, self.latency_ms, self.last_msg_full = 0.0, -1.0, ""
        self.last_time, self.count, self.lock = time.time(), 0, threading.Lock()

    def callback(self, msg):
        with self.lock:
            now = time.time()
            self.count += 1
            if now - self.last_time > 1.0:
                self.hz = self.count / (now - self.last_time)
                self.count, self.last_time = 0, now
            if hasattr(msg, 'header'):
                ts = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
                self.latency_ms = (now - ts) * 1000.0
            self.last_msg_full = self._msg_to_str(msg)

    def _msg_to_str(self, obj, indent=0):
        res, prefix = [], "  " * indent
        if hasattr(obj, 'get_fields_and_field_types'):
            for f in obj.get_fields_and_field_types().keys():
                v = getattr(obj, f)
                if hasattr(v, 'get_fields_and_field_types'):
                    res.append(f"{prefix}└─ {f}:")
                    res.append(self._msg_to_str(v, indent + 1))
                elif isinstance(v, (list, tuple)):
                    res.append(f"{prefix}└─ {f}: [{len(v)} items]")
                    for item in v[:3]:
                        res.append(self._msg_to_str(item, indent + 2))
                else: res.append(f"{prefix}└─ {f}: {v}")
        else: res.append(f"{prefix}{obj}")
        return "\n".join(res)

class NodeMonitor(Node):
    def __init__(self, target):
        super().__init__('mon_node')
        self.target, self.monitors, self.subs = target, {}, []

    def start(self):
        pubs = []
        for _ in range(3):
            try: pubs = self.get_publisher_names_and_types_by_node(self.target, '/')
            except: pass
            if pubs: break
            time.sleep(0.5)
        if not pubs: return False
        for n, t in pubs:
            if n in ['/rosout', '/parameter_events']: continue
            m = TopicMonitor(n, t[0])
            self.monitors[n] = m
            self.subs.append(self.create_subscription(get_message(t[0]), n, m.callback, QoSProfile(depth=10, reliability=ReliabilityPolicy.BEST_EFFORT)))
        return True

# ==============================================================================
#  Dashboard Loop
# ==============================================================================
def dashboard_loop(node_monitor, stop_event):
    selected_row, scroll_offset, density = 0, 0, 1 # density 1=Normal, 2=Wide
    expanded = set()
    with KeyPoller() as poller:
        while not stop_event.is_set():
            t_list = sorted(node_monitor.monitors.values(), key=lambda x: x.name)
            v_lines, r_map = [], []
            for m in t_list:
                with m.lock:
                    h, l, d = m.hz, m.latency_ms, m.last_msg_full
                hz_s, lat_s = (f"{h:.1f}" if h>0 else "DEAD"), (f"{l:.1f}ms" if l>=0 else "-")
                v_lines.append(('H', m, f" {m.name[:35]:<35} | {m.type_str[:25]:<25} | {hz_s:<6} | {lat_s:<5}"))
                r_map.append(m)
                for _ in range(density - 1): # Space for density
                    v_lines.append(('S', None, "")); r_map.append(m)
                if m.name in expanded:
                    lines = (d if d else "    (Waiting for data...)").splitlines()
                    for ln in lines:
                        v_lines.append(('D', None, f"    \033[90m{ln}\033[0m"))
                        r_map.append(m)
                    v_lines.append(('S', None, "")); r_map.append(m)

            try: cols, lines = os.get_terminal_size()
            except: cols, lines = 80, 24
            vh = lines - 6
            key = poller.poll()
            if key == 'q': return
            elif key == 'UP': selected_row = max(0, selected_row - 1)
            elif key == 'DOWN': selected_row = min(len(v_lines)-1, selected_row + 1)
            elif key in ['\r', '\n', ' ']: # Fix Enter Key
                tm = r_map[selected_row]
                if tm.name in expanded: expanded.remove(tm.name)
                else: expanded.add(tm.name)
            elif key == '+': density = min(4, density + 1)
            elif key == '-': density = max(1, density - 1)

            if selected_row < scroll_offset: scroll_offset = selected_row
            elif selected_row >= scroll_offset + vh: scroll_offset = selected_row - vh + 1
            
            print("\033c", end="")
            print(f"================================================================================")
            print(f" Node: {node_monitor.target} | Density: {density} (+/- zoom)")
            print(f" Controls: \u2191/\u2193 Move, Enter: Toggle, q: Back")
            print(f"================================================================================")
            print(f"  TOPIC NAME: {'TYPE':<25} | {'HZ':<6} | {'LAT'}\n{'-'*cols}")
            for i in range(vh):
                idx = scroll_offset + i
                if idx >= len(v_lines): break
                lt, obj, txt = v_lines[idx]
                style = "\033[7m" if idx == selected_row else ""
                print(f"{style}{txt[:cols]}\033[0m")
            print(f"{'-'*cols}\n Line: {selected_row+1}/{len(v_lines)} | 'q' to menu")
            time.sleep(0.08)

# ==============================================================================
#  Main
# ==============================================================================
def main():
    if 'TERM' not in os.environ: os.environ['TERM'] = 'xterm'
    parser = argparse.ArgumentParser()
    parser.add_argument("node", nargs="?")
    args = parser.parse_args()
    rclpy.init()
    try:
        if args.node: run_monitor(args.node)
        else:
            while rclpy.ok():
                temp = rclpy.create_node('_nl')
                print("Scanning ROS 2 nodes..."); best = []
                for _ in range(10):
                    curr = sorted(temp.get_node_names())
                    if len(curr) > len(best): best = curr
                    time.sleep(0.1)
                temp.destroy_node()
                target = select_node_by_number(best)
                if not target: break
                run_monitor(target)
    finally:
        if rclpy.ok(): rclpy.shutdown()

def run_monitor(target):
    mon = NodeMonitor(target)
    exc = MultiThreadedExecutor()
    exc.add_node(mon)
    stop = threading.Event()
    t = threading.Thread(target=exc.spin, daemon=True)
    t.start()
    try:
        if mon.start(): dashboard_loop(mon, stop)
        else: print(f"Error: {target} no topics"); time.sleep(1)
    except KeyboardInterrupt: pass
    finally:
        stop.set(); exc.shutdown(); mon.destroy_node(); t.join(1)

if __name__ == '__main__': main()
