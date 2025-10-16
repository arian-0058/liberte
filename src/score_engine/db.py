import os
import json
import asyncio
import asyncpg
import structlog
from contextlib import asynccontextmanager
from .metrics import db_seconds, db_upserts_total

logger = structlog.get_logger(__name__)

_POOL = None

async def create_pool(dsn: str, min_size: int = 2, max_size: int = 10):
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)
    return _POOL

def pool():
    if _POOL is None:
        raise RuntimeError("DB pool not initialized")
    return _POOL

CREATE_TABLES_SQL = """CREATE TABLE IF NOT EXISTS roi_history (
    token TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    roi          DOUBLE PRECISION NOT NULL,
    volatility   DOUBLE PRECISION NOT NULL,
    slope        DOUBLE PRECISION NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token, timeframe, window_end)
);
CREATE INDEX IF NOT EXISTS idx_roi_history_token_tf_end ON roi_history(token, timeframe, window_end DESC);

CREATE TABLE IF NOT EXISTS scores (
    token        TEXT NOT NULL,
    scored_at    TIMESTAMPTZ NOT NULL,
    score        DOUBLE PRECISION NOT NULL,
    features     JSONB NOT NULL,
    weights      JSONB NOT NULL,
    config_ver   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token, scored_at)
);
CREATE INDEX IF NOT EXISTS idx_scores_token_scored_at ON scores(token, scored_at DESC);
"""

UPSERT_ROI_SQL = """INSERT INTO roi_history (token, timeframe, window_start, window_end, roi, volatility, slope)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (token, timeframe, window_end) DO UPDATE SET
  roi=EXCLUDED.roi,
  volatility=EXCLUDED.volatility,
  slope=EXCLUDED.slope;
"""

UPSERT_SCORE_SQL = """INSERT INTO scores (token, scored_at, score, features, weights, config_ver)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (token, scored_at) DO UPDATE SET
  score=EXCLUDED.score,
  features=EXCLUDED.features,
  weights=EXCLUDED.weights,
  config_ver=EXCLUDED.config_ver;
"""

FETCH_LATEST_SCORE_SQL = """SELECT token, scored_at, score, features, weights, config_ver
FROM scores
WHERE token=$1
ORDER BY scored_at DESC
LIMIT 1;
"""

FETCH_LATEST_ROI_SQL = """SELECT DISTINCT ON (timeframe) token, timeframe, window_start, window_end, roi, volatility, slope
FROM roi_history
WHERE token=$1
ORDER BY timeframe, window_end DESC;
"""

async def ensure_tables():
    with db_seconds.time():
        async with pool().acquire() as conn:
            await conn.execute(CREATE_TABLES_SQL)

async def upsert_roi(token, timeframe, window_start, window_end, roi, volatility, slope):
    with db_seconds.time():
        async with pool().acquire() as conn:
            await conn.execute(UPSERT_ROI_SQL, token, timeframe, window_start, window_end, roi, volatility, slope)
            db_upserts_total.inc()

async def upsert_score(token, scored_at, score, features: dict, weights: dict, config_ver: str):
    with db_seconds.time():
        async with pool().acquire() as conn:
            await conn.execute(UPSERT_SCORE_SQL, token, scored_at, score, json.dumps(features), json.dumps(weights), config_ver)
            db_upserts_total.inc()

async def fetch_latest_score(token: str):
    async with pool().acquire() as conn:
        row = await conn.fetchrow(FETCH_LATEST_SCORE_SQL, token)
        if not row:
            return None
        return dict(row)

async def fetch_latest_roi(token: str):
    async with pool().acquire() as conn:
        rows = await conn.fetch(FETCH_LATEST_ROI_SQL, token)
        return [dict(r) for r in rows]

# CLI: init tables
if __name__ == "__main__":
    import asyncio, sys
    import structlog
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
    log = structlog.get_logger("db.init")
    try:
        dsn = os.getenv("POSTGRES_DSN")
        if not dsn:
            log.error("POSTGRES_DSN not set")
            sys.exit(1)
        asyncio.run(create_pool(dsn))
        asyncio.run(ensure_tables())
        log.info("tables_ready", ok=True)
    except Exception as e:
        log.exception("init_failed", error=str(e))
        sys.exit(2)
