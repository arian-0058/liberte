# Phase 1 — Signal Hunter MVP

## 1) Install deps
- Python 3.12, Poetry
- `poetry install`

## 2) Create topics
```bash
poetry run python /mnt/data/scripts/create_topics_phase1.py
```

## 3) Apply DB schema
```bash
psql "postgresql://postgres:postgres@localhost:5432/liberte" -f /mnt/data/sql/phase1.sql
```

## 4) Run dummy WS and collector (Phase 0 baseline)
- Dummy WS: `poetry run python scripts/dummy_ws_server.py`
- Collector: `poetry run python src/collector/collector.py`

## 5) Run scanner & execution stub
- Scanner: `poetry run python /mnt/data/src/scanner/scanner.py`
- Exec Stub: `poetry run python /mnt/data/src/execution/execution_stub.py`

## 6) Metrics
- Scanner: http://localhost:8001/metrics
- Exec:    http://localhost:8002/metrics
