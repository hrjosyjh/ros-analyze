# ROS2 AGV Log Analyzer

ROS2 AGV(syswin_abot) 시스템의 `launch.log` 파일을 시간대별로 분석하는 도구입니다.
14GB+ 대용량 로그를 스트리밍 방식으로 처리하며, 증분 분석과 실시간 모니터링을 지원합니다.

## 요구사항

- Python 3.8+
- 추가 패키지 설치 불필요 (표준 라이브러리만 사용)
- `monitor_node.py`만 ROS2 환경 필요 (`rclpy`)

## 지원하는 로그 형식

| 형식 | 예시 |
|------|------|
| ROS2 launch | `1769471864.054 [INFO] [motor_driver_node-11]: process started with pid [2047]` |
| ROS2 logging | `1769471864.857 [tsd_node-25] [ERROR] [1769471864.846] [tsd]: Unable to open port` |
| 노드 직접 출력 | `1769471864.926 [motor_driver_node-11] \033[1;31m[08:57:44] [motor_api.cpp(26)] MOTOR API START` |

노드 직접 출력의 경우 ANSI 색상 코드로 로그 레벨을 추정합니다 (빨강=ERROR, 노랑=WARN, 흰색=INFO).

---

## 도구 목록

| 파일 | 설명 |
|------|------|
| `analyze_log.py` | 시간대별 로그 분석기 (정적 분석 + 실시간 모니터링) |
| `analyze_node.py` | 노드별 로그 분석기 (노드 요약, 상세 분석) |
| `monitor_node.py` | ROS2 실시간 노드 모니터 (인터랙티브 TUI, ROS2 필요) |
| `log_parser.py` | 공용 로그 파서 라이브러리 (다른 스크립트에서 import) |

---

## 1. analyze_log.py - 시간대별 로그 분석기

### 정적 분석 (증분)

기본 동작은 **증분 분석**입니다. 이전 분석 이후 새로 추가된 내용만 처리합니다.

```bash
# 증분 분석 (기본 - 새 내용만, 1시간 단위)
python3 analyze_log.py launch.log

# 전체 재분석 (처음부터)
python3 analyze_log.py launch.log --full

# 시간 단위 변경
python3 analyze_log.py launch.log --interval 10m     # 10분 단위
python3 analyze_log.py launch.log --interval 1m      # 1분 단위
python3 analyze_log.py launch.log --interval 30s     # 30초 단위

# 필터링
python3 analyze_log.py launch.log --errors-only                   # ERROR/WARN/FATAL만
python3 analyze_log.py launch.log --node motor_driver_node-11     # 특정 노드 (부분 매칭)
python3 analyze_log.py launch.log --top-nodes 20                  # 상위 20개 노드 표시

# CSV 경로 직접 지정
python3 analyze_log.py launch.log --csv my_report.csv

# 조합
python3 analyze_log.py launch.log -i 10m -e --node tsd_node --full
```

### 시간 범위 지정 분석

`--from` / `--to` 옵션으로 특정 기간만 분석할 수 있습니다.

```bash
# 날짜 범위
python3 analyze_log.py launch.log --from "2026-01-27" --to "2026-01-28"

# 시간 범위 (날짜 + 시간)
python3 analyze_log.py launch.log --from "2026-01-27 09:00" --to "2026-01-27 18:00"

# 시간만 지정 (오늘 기준)
python3 analyze_log.py launch.log --from "09:00" --to "12:00"

# 시작만 지정 (해당 시각 이후 전체)
python3 analyze_log.py launch.log --from "2026-01-28"

# 다른 옵션과 조합
python3 analyze_log.py launch.log --from "2026-01-27" --to "2026-01-28" -i 10m -e
```

지원 형식: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, `YYYY-MM-DD HH:MM:SS`, `HH:MM`, `HH:MM:SS`

### 실시간 모니터링

`-f` 옵션으로 `tail -f` 방식의 실시간 대시보드를 사용합니다.

```bash
# 기본 실시간 모니터링 (1분 단위, 5분 윈도우, 2초 갱신)
python3 analyze_log.py launch.log -f

# 윈도우/갱신 주기 변경
python3 analyze_log.py launch.log -f --window 10m --refresh 1

# 파일 끝부터 시작 (기존 로그 무시)
python3 analyze_log.py launch.log -f --tail 0

# 에러만 실시간 추적
python3 analyze_log.py launch.log -f -e

# 특정 노드 모니터링
python3 analyze_log.py launch.log -f --node motor_driver
```

`Ctrl+C`로 종료하면 누적 통계 요약과 CSV 파일이 저장됩니다.

### 옵션 전체 목록

| 옵션 | 단축 | 기본값 | 설명 |
|------|------|--------|------|
| `logfile` | | (필수) | 분석할 로그 파일 경로 |
| `--interval` | `-i` | `1h` | 분석 시간 단위 (`1h`, `30m`, `10m`, `1m`, `30s`) |
| `--top-nodes` | `-t` | `10` | 상위 노드 표시 개수 |
| `--errors-only` | `-e` | | ERROR/WARN/FATAL 로그만 분석 |
| `--node` | `-n` | | 특정 노드 필터 (부분 매칭) |
| `--csv` | `-c` | | CSV 파일 경로 직접 지정 |
| `--from` | | | 분석 시작 시각 (`2026-01-27`, `2026-01-27 09:00`, `09:00`) |
| `--to` | | | 분석 종료 시각 (`2026-01-28`, `2026-01-27 18:00`, `18:00`) |
| `--full` | | | 전체 재분석 (체크포인트 무시) |
| `--follow` | `-f` | | 실시간 모니터링 모드 |
| `--window` | `-w` | `5m` | 실시간 모드 롤링 윈도우 크기 |
| `--refresh` | `-r` | `2` | 실시간 모드 대시보드 갱신 주기(초) |
| `--tail` | | `5000` | 실시간 모드 시작 시 읽을 마지막 줄 수 (`0`=끝부터) |

---

## 2. analyze_node.py - 노드별 로그 분석기

노드 관점에서 로그를 분석합니다. 전체 노드 요약 또는 특정 노드 상세 분석을 제공합니다.

```bash
# 전체 노드 요약 (에러 순 정렬)
python3 analyze_node.py launch.log

# 특정 노드 상세 분석 (타임라인, 에러 패턴)
python3 analyze_node.py launch.log --node motor_driver

# 에러 발생 노드만 표시
python3 analyze_node.py launch.log --errors-only

# 시간 범위 지정
python3 analyze_node.py launch.log --from "2026-01-27 09:00" --to "2026-01-27 12:00"

# 조합
python3 analyze_node.py launch.log --node tsd --from "2026-01-27" --errors-only
```

### 옵션

| 옵션 | 설명 |
|------|------|
| `logfile` | 분석할 로그 파일 경로 (필수) |
| `--node` | 특정 노드 필터 (부분 매칭) |
| `--errors-only` | 에러 발생 노드만 표시 |
| `--from` | 분석 시작 시각 |
| `--to` | 분석 종료 시각 |

### 출력 내용

**전체 요약 모드** (기본):
- 노드별 총 로그 수, ERROR/WARN 건수, 처리율(FPS), 활동 기간
- 에러율 기준 정렬

**특정 노드 상세 모드** (`--node` 지정):
- 로그 레벨 분포 (바 차트)
- 분 단위 활동 타임라인
- 에러/경고 메시지 패턴별 그룹핑

---

## 3. monitor_node.py - ROS2 실시간 노드 모니터

ROS2가 실행 중인 환경에서 노드의 토픽을 실시간으로 모니터링하는 인터랙티브 TUI 도구입니다.

**ROS2 환경 필수** (`rclpy`, `rosidl_runtime_py`)

```bash
# 인터랙티브 모드 (노드 목록에서 선택)
python3 monitor_node.py

# 특정 노드 직접 지정
python3 monitor_node.py motor_driver_node
```

### 조작법

| 키 | 기능 |
|----|------|
| 숫자 입력 | 노드 선택 |
| Enter/Space | 트리 펼치기/접기 |
| Up/Down | 이동 및 스크롤 |
| +/- | 화면 밀도 조절 |
| q | 메뉴로 돌아가기 |

---

## 출력 보고서

### 터미널 출력 (analyze_log.py 정적 분석)

1. **전체 요약** - 총 라인 수, 시간 범위, 분석 기간
2. **로그 레벨별 통계** - FATAL/ERROR/WARN/INFO/DEBUG 분포
3. **노드별 통계** - 상위 N개 노드 로그 볼륨 (바 차트)
4. **시간대별 로그 볼륨** - 시간 구간별 건수 + 에러 수 + SPIKE 감지
5. **시간대별 레벨 분포** - 구간별 레벨 테이블
6. **시간대별 상위 노드** - 구간별 Top 3 활동 노드
7. **에러 메시지 요약** - ERROR/WARN/FATAL 메시지 패턴별 그룹핑

### 실시간 대시보드 (analyze_log.py -f)

| 영역 | 내용 |
|------|------|
| 상태 바 | 현재 로그 시각, 윈도우 크기, 경과 시간, 처리율(lines/s) |
| 윈도우 통계 | 최근 N분간 레벨별 건수 + 누적 합계 |
| 시간대별 볼륨 | 롤링 윈도우 내 시간 버킷별 바 차트 |
| 활동 노드 | 윈도우 내 상위 7개 노드 비율 차트 |
| 최근 에러/경고 | ERROR/WARN/FATAL 실시간 알림 (최근 8건) |

### CSV 파일 (자동 생성)

분석 실행 시 `reports/` 디렉토리에 실행 날짜 기준으로 **단일 CSV 파일**이 자동 생성됩니다.
하나의 파일 안에 3개 섹션(`[시간대별 통계]`, `[노드별 통계]`, `[에러 목록]`)이 빈 행으로 구분되어 포함됩니다.

```
reports/
  log_report_2026-01-31_102540.csv        # 정적 분석 통합 보고서
  live_report_2026-01-31_102540.csv       # 실시간 모니터링 통합 보고서
  .checkpoint.json                        # 증분 분석 체크포인트
```

CSV는 `utf-8-sig`(BOM 포함) 인코딩이므로 Excel에서 바로 열어도 한글이 깨지지 않습니다.

---

## 증분 분석

기본 동작이 증분 분석입니다. 체크포인트(`reports/.checkpoint.json`)에 마지막 분석 위치를 저장하고, 다음 실행 시 그 지점부터 새 데이터만 분석합니다.

| 상황 | 동작 |
|------|------|
| 첫 실행 | 전체 분석 후 체크포인트 저장 |
| 재실행 (파일 커짐) | 마지막 위치부터 새 데이터만 분석 |
| 재실행 (변경 없음) | "새로 추가된 로그가 없습니다" 출력 후 종료 |
| 파일 크기 줄어듦 | 로그 로테이션 감지 → 자동 전체 재분석 |
| `--full` 옵션 | 체크포인트 무시, 처음부터 전체 분석 |

---

## 파일 구조

```
.log/
  analyze_log.py          # 시간대별 로그 분석기 (정적 + 실시간)
  analyze_node.py         # 노드별 로그 분석기
  monitor_node.py         # ROS2 실시간 노드 모니터 (TUI)
  log_parser.py           # 공용 로그 파서 라이브러리
  launch.log              # ROS2 AGV launch 로그 (분석 대상)
  README.md               # 이 문서
  reports/                # (자동 생성) CSV 보고서 + 체크포인트
    .checkpoint.json
    log_report_*.csv      # 정적 분석 통합 보고서
    live_report_*.csv     # 실시간 모니터링 통합 보고서
```
