# Agent Notes (Repository Guide)

This repository is a small, stdlib-only Python CLI tool for analyzing large ROS2 `launch.log` files.

## Repo Facts

- Language/runtime: Python 3.8+ (no third-party dependencies).
- Primary entrypoints: `analyze_log.py`, `analyze_node.py`.
- Large-file posture: logs can be 14GB+; code is written to stream, not load everything.
- Generated artifacts: CSV reports and `.checkpoint.json` under `reports/` and `report/`.

## Commands

### Run (static / incremental)

```bash
python3 analyze_log.py launch.log
python3 analyze_log.py launch.log --full
python3 analyze_log.py launch.log --interval 10m
python3 analyze_log.py launch.log --errors-only
python3 analyze_log.py launch.log --node motor_driver
python3 analyze_log.py launch.log --top-nodes 20
python3 analyze_log.py launch.log --csv my_report.csv
```

### Run (live / follow)

```bash
python3 analyze_log.py launch.log -f
python3 analyze_log.py launch.log -f --window 10m --refresh 1
python3 analyze_log.py launch.log -f --tail 0
python3 analyze_log.py launch.log -f -e
python3 analyze_log.py launch.log -f --node motor_driver
```

### Node-focused report

```bash
python3 analyze_node.py launch.log
python3 analyze_node.py launch.log --node motor_driver
python3 analyze_node.py launch.log --node motor --node tsd
python3 analyze_node.py launch.log --interval 1m
python3 analyze_node.py launch.log --errors-only
python3 analyze_node.py launch.log --sort errors
```

### Build / lint / tests

- Build: none (scripts run directly; no packaging config present).
- Lint/format: none configured (no `pyproject.toml`, `ruff/black`, `pre-commit`, etc.).
- Tests: none configured (no `pytest`/`unittest` suite).

Useful smoke checks (stdlib-only):

```bash
python3 -m py_compile analyze_log.py analyze_node.py
python3 analyze_log.py --help
python3 analyze_node.py --help
```

Single-test command: N/A (no test runner in-repo). If you add tests later, prefer `pytest` and document:

```bash
pytest -q path/to/test_file.py -k test_name
```

## Code Style (follow existing patterns)

### Imports & dependencies

- Keep dependencies stdlib-only unless there is a clear justification.
- Group imports: stdlib at top; no third-party section expected.

### Naming

- Functions/variables: `snake_case`.
- Classes: `CamelCase`.
- Constants/regex patterns: `UPPER_SNAKE_CASE` (e.g., `RE_*`, `COLOR_TO_LEVEL`).
- CLI flags: long form plus short aliases where useful (see `README.md`).

### Types

- No type-checker is configured; existing code is largely unannotated.
- If adding annotations, keep them lightweight and avoid introducing a typing-only dependency/toolchain unless you also add config.

### Formatting

- Indentation: 4 spaces.
- PEP8-ish spacing; long functions are acceptable.
- Section dividers are used heavily (e.g., `# =============================================================================`).

### Error handling & resilience

- Parsing functions return `None` on non-fatal failures; callers treat it as “skip line” and continue.
- Use narrow exception handling around IO/JSON; fail safe (e.g., ignore corrupt checkpoint and re-run).
- Treat `KeyboardInterrupt` as expected control flow: exit cleanly and still emit summaries/reports.

### CLI UX conventions

- Use `argparse` (help text is part of the product): keep examples in the epilog and keep flags stable.
- Prefer long options with short aliases where it improves ergonomics (e.g., `--interval` + `-i`).
- Progress/status output may go to stderr (so stdout can remain clean for piping).

### IO, encoding, and locale

- When reading logs: use `encoding='utf-8'` with `errors='replace'` for robustness.
- CSV outputs use `utf-8-sig` (BOM) for Excel compatibility; keep this behavior.
- Output filenames may contain Korean text; ensure any new file writing preserves Unicode paths.

### Performance constraints (important)

- Assume multi-GB log files: stream line-by-line; avoid reading entire files into memory.
- Avoid O(N) memory growth where N is total lines; prefer bounded structures (`deque`) or aggregations (`defaultdict(int)`).
- Prefer regex precompilation and cheap early checks in hot paths.

## Files & directories

- Sources: `analyze_log.py`, `analyze_node.py`.
- Docs: `README.md`.
- Generated: `reports/`, `report/`, `launch.log` (do not treat these as source).
- Incremental state: `reports/.checkpoint.json` and/or `report/.checkpoint.json`.

## Working with checkpoints

- Checkpoints exist to support incremental runs over multi-GB logs; do not rewrite the checkpoint schema casually.
- Treat a corrupt checkpoint as non-fatal: fall back to a full re-scan rather than aborting.

## Performance footguns

- Don’t accumulate raw log lines; store counts/aggregations, or cap history with a `deque(maxlen=...)`.
- Avoid per-line expensive work (e.g., repeated `re.compile` or heavy datetime parsing).
- Prefer early returns and cheap substring checks before regex matches in hot paths.

## Cursor / Copilot rules

- No `.cursor/rules/`, `.cursorrules`, or `.github/copilot-instructions.md` found in this repository.
