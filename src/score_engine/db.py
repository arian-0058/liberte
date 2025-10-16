"""
Database utilities for phase 2
==============================

This module provides a tiny abstraction over PostgreSQL using
``asyncpg``.  When ``asyncpg`` is unavailable, the API falls back to
storing records in memory.  The interface intentionally mirrors only
the operations required by the ROI analyzer and scoring engine so that
tests can execute without a running database.

The two tables created by :func:`init_phase2` correspond exactly to
those defined in the project specifications:

* ``roi_history`` – Holds rolling ROI metrics for each token along with
  volatility and slope.
* ``scores`` – Holds the computed score and confidence for each token.

Both tables use the token as the unique key for the purposes of
upserts.  If more than one entry per token is required in the future,
the table definitions can be adjusted accordingly.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
from typing import Any, Dict, Optional, Tuple

try:
    import asyncpg  # type: ignore
except ImportError:  # pragma: no cover - fallback when asyncpg is absent
    asyncpg = None  # type: ignore


class Database:
    """Lightweight database facade supporting asyncpg or in‑memory mode."""

    def __init__(self, dsn: str) -> None:
        #: Connection string for PostgreSQL when asyncpg is available.
        self.dsn = dsn
        #: Connection pool created on demand.  None when asyncpg is not
        #: installed or when :meth:`connect` has not been called.
        self.pool: Optional[asyncpg.pool.Pool] = None  # type: ignore[name-defined]

        # In‑memory fallbacks used when asyncpg is not available.  These
        # structures map the token to the latest record.
        self._roi_store: Dict[str, Dict[str, Any]] = {}
        self._score_store: Dict[str, Dict[str, Any]] = {}

    async def connect(self) -> None:
        """Establish the connection pool if asyncpg is available.

        When asyncpg is not installed, this method does nothing.  It
        can safely be called multiple times.
        """
        if asyncpg and self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.dsn)

    async def init_phase2(self) -> None:
        """Create the phase 2 tables if they do not already exist.

        The table definitions mirror those in the HLD.  In memory
        stores require no explicit setup.
        """
        if not asyncpg or self.pool is None:
            # In memory mode: nothing to create
            return

        # Compose the SQL once and execute it.  We wrap the operation
        # inside a transaction to ensure the tables are either both
        # created or not created in the unlikely event of an error.
        create_sql = """
        CREATE TABLE IF NOT EXISTS roi_history (
            id SERIAL PRIMARY KEY,
            token TEXT UNIQUE,
            roi_5m DOUBLE PRECISION,
            roi_15m DOUBLE PRECISION,
            roi_1h DOUBLE PRECISION,
            volatility DOUBLE PRECISION,
            roi_slope DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS scores (
            id SERIAL PRIMARY KEY,
            token TEXT UNIQUE,
            avg_roi DOUBLE PRECISION,
            volatility DOUBLE PRECISION,
            trend DOUBLE PRECISION,
            score DOUBLE PRECISION,
            confidence DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
        async with self.pool.acquire() as conn:
            # Use execute with multiple statements – asyncpg handles it
            await conn.execute(create_sql)

    async def upsert_roi_history(
        self,
        token: str,
        roi_5m: float,
        roi_15m: float,
        roi_1h: float,
        volatility: float,
        roi_slope: float,
    ) -> None:
        """Insert or update a token's ROI history record.

        In PostgreSQL mode an UPSERT on the ``token`` column is used
        since ``token`` has a unique constraint.  In in‑memory mode the
        internal dictionary is updated directly.
        """
        now = _dt.datetime.utcnow()
        if asyncpg and self.pool is not None:
            sql = """
                INSERT INTO roi_history
                    (token, roi_5m, roi_15m, roi_1h, volatility, roi_slope, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (token) DO UPDATE
                SET roi_5m = EXCLUDED.roi_5m,
                    roi_15m = EXCLUDED.roi_15m,
                    roi_1h = EXCLUDED.roi_1h,
                    volatility = EXCLUDED.volatility,
                    roi_slope = EXCLUDED.roi_slope,
                    updated_at = EXCLUDED.updated_at;
            """
            async with self.pool.acquire() as conn:
                await conn.execute(sql, token, roi_5m, roi_15m, roi_1h, volatility, roi_slope, now)
        else:
            # In memory: simply overwrite the record
            self._roi_store[token] = {
                "token": token,
                "roi_5m": roi_5m,
                "roi_15m": roi_15m,
                "roi_1h": roi_1h,
                "volatility": volatility,
                "roi_slope": roi_slope,
                "updated_at": now,
            }

    async def upsert_score(
        self,
        token: str,
        avg_roi: float,
        volatility: float,
        trend: float,
        score: float,
        confidence: float,
    ) -> None:
        """Insert or update a token's score record."""
        now = _dt.datetime.utcnow()
        if asyncpg and self.pool is not None:
            sql = """
                INSERT INTO scores
                    (token, avg_roi, volatility, trend, score, confidence, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (token) DO UPDATE
                SET avg_roi = EXCLUDED.avg_roi,
                    volatility = EXCLUDED.volatility,
                    trend = EXCLUDED.trend,
                    score = EXCLUDED.score,
                    confidence = EXCLUDED.confidence,
                    updated_at = EXCLUDED.updated_at;
            """
            async with self.pool.acquire() as conn:
                await conn.execute(sql, token, avg_roi, volatility, trend, score, confidence, now)
        else:
            self._score_store[token] = {
                "token": token,
                "avg_roi": avg_roi,
                "volatility": volatility,
                "trend": trend,
                "score": score,
                "confidence": confidence,
                "updated_at": now,
            }

    async def fetch_roi(self, token: str) -> Optional[Dict[str, Any]]:
        """Return the latest ROI record for ``token`` or ``None``."""
        if asyncpg and self.pool is not None:
            sql = "SELECT token, roi_5m, roi_15m, roi_1h, volatility, roi_slope, updated_at FROM roi_history WHERE token = $1"
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, token)
            if row:
                return dict(row)
            return None
        else:
            return self._roi_store.get(token)

    async def fetch_all_roi(self) -> Dict[str, Dict[str, Any]]:
        """Fetch ROI records for all tokens.

        Returns a dictionary keyed by token.  In PostgreSQL mode a
        single SQL query retrieves all rows and converts them to a
        dictionary.
        """
        if asyncpg and self.pool is not None:
            sql = "SELECT token, roi_5m, roi_15m, roi_1h, volatility, roi_slope, updated_at FROM roi_history"
            records: Dict[str, Dict[str, Any]] = {}
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql)
                for row in rows:
                    records[row["token"]] = dict(row)
            return records
        else:
            # return copy to avoid accidental modification
            return dict(self._roi_store)

    async def fetch_score(self, token: str) -> Optional[Dict[str, Any]]:
        """Return the score record for ``token`` or ``None``."""
        if asyncpg and self.pool is not None:
            sql = "SELECT token, avg_roi, volatility, trend, score, confidence, updated_at FROM scores WHERE token = $1"
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, token)
            if row:
                return dict(row)
            return None
        else:
            return self._score_store.get(token)