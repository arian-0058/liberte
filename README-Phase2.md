# Liberte Phase 2 (Score Engine + ROI Analyzer)

Paths:
- config/score_engine.yaml
- src/score_engine/{metrics.py, db.py, roi_analyzer.py, score_engine.py, __init__.py}

Quick run:
```bash
poetry add aiokafka asyncpg prometheus-client structlog hvac fastapi uvicorn pyyaml
export POSTGRES_DSN="postgresql://user:pass@postgres:5432/liberte"
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
poetry run python -m score_engine.db
poetry run python -m score_engine.score_engine
```
