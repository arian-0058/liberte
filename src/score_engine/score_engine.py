"""
Scoring engine for phase 2
==========================

The scoring engine reads the latest ROI metrics from the
``roi_history`` table, combines them with configurable weights and
produces an overall score and confidence for each token.  Scores are
persisted to the ``scores`` table and optionally published to the
``scored_tokens`` Kafka topic.

The public API offered by this module consists of a handful of pure
functions that implement the core formula along with a convenience
class :class:`ScoreEngine` which orchestrates database IO, hot
configuration reloading and metric collection.  The pure functions are
entirely free from side effects and are therefore amenable to unit
testing without any mocking.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Dict, Any, Mapping, Optional, Tuple

try:
    # aiokafka is optional – tests may not require it.  Importing
    # these symbols inside the try prevents ImportError from
    # propagating at module import time when aiokafka is missing.
    from aiokafka import AIOKafkaProducer  # type: ignore
    from aiokafka.errors import KafkaError  # type: ignore
except ImportError:  # pragma: no cover
    AIOKafkaProducer = None  # type: ignore
    KafkaError = Exception  # type: ignore

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from .db import Database

try:
    # structlog is optional
    import structlog  # type: ignore
except ImportError:  # pragma: no cover
    structlog = None  # type: ignore

from .metrics import (
    roi_volatility_avg,
    score_confidence_avg,
    score_distribution,
    score_update_latency_seconds,
)


def normalize_volatility(volatility: float, clip: float) -> float:
    """Normalize volatility to the [0, 1] range using a clip value.

    A small epsilon is included in the denominator to avoid division by
    zero.  The result is clipped between 0 and 1 inclusive.
    """
    if clip <= 0:
        raise ValueError("clip must be positive")
    val = volatility / clip if clip else 0.0
    if val < 0:
        return 0.0
    if val > 1:
        return 1.0
    return val


def compute_trend(slope: float) -> float:
    """Apply a hyperbolic tangent to the ROI slope to map it into [-1, 1]."""
    return math.tanh(slope)


def compute_score(
    avg_roi: float,
    vol_norm: float,
    trend: float,
    weights: Mapping[str, float],
) -> Tuple[float, float]:
    """Compute the score and confidence using the configurable formula.

    Parameters
    ----------
    avg_roi:
        The average of the three ROI windows.

    vol_norm:
        Normalised volatility in the range [0, 1].

    trend:
        Hyperbolic tangent of the ROI slope.  Lies in [-1, 1].

    weights:
        A mapping containing three keys: ``roi``, ``volatility`` and
        ``trend``.  Their values should sum to one for the score to
        remain bounded.  If they do not sum to one the formula still
        operates but the resulting scores may exceed the [-1, 1]
        interval.

    Returns
    -------
    Tuple[float, float]
        A tuple ``(score, confidence)``.  ``score`` is the weighted
        combination of the inputs and ``confidence`` is computed as
        ``max(0, 1 - vol_norm)``.  Confidence may be overridden in
        the future with more advanced heuristics.
    """
    w_roi = float(weights.get("roi", 0.0))
    w_vol = float(weights.get("volatility", 0.0))
    w_trend = float(weights.get("trend", 0.0))
    # Compute the base score
    score = w_roi * avg_roi + w_vol * (1.0 - vol_norm) + w_trend * trend
    # Confidence decreases as volatility increases
    confidence = max(0.0, 1.0 - vol_norm)
    return score, confidence


class ScoreEngine:
    """Orchestrate score computation and publication.

    This class periodically retrieves the latest ROI metrics from the
    database, computes scores and persists them.  It also exposes
    optional Kafka integration and hot configuration reloading from a
    YAML file.  The scoring loop runs in the background until the
    ``stop`` method is called.
    """

    def __init__(
        self,
        db: Database,
        config_path: str,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
    ) -> None:
        self.db = db
        self.config_path = config_path
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self._config: Dict[str, Any] = {}
        self._producer: Optional[AIOKafkaProducer] = None  # type: ignore[name-defined]
        self._running = False
        self._logger = structlog.get_logger(__name__) if structlog else None

    async def _load_config(self) -> None:
        """Load or reload the YAML configuration file on disk.

        When the file cannot be read or parsed the existing
        configuration remains unchanged.  The weight keys are
        normalised so that missing keys default to zero.
        """
        try:
            if yaml is None:
                return
            if not os.path.exists(self.config_path):
                return
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            # Flatten nested structure into a dict of weights and a
            # clip value
            weights = data.get("weights", {}) or {}
            score_cfg = data.get("score", {}) or {}
            self._config["weights"] = {
                "roi": float(weights.get("roi", 0.0)),
                "volatility": float(weights.get("volatility", 0.0)),
                "trend": float(weights.get("trend", 0.0)),
            }
            self._config["vol_clip"] = float(score_cfg.get("vol_clip", 1.0))
            self._config["update_interval_sec"] = int(
                data.get("update_interval_sec", 60)
            )
        except Exception as exc:  # pragma: no cover
            if self._logger:
                self._logger.warning("Failed to reload config", exc_info=exc)

    async def _ensure_producer(self) -> None:
        """Initialise the Kafka producer if ``aiokafka`` is available."""
        if AIOKafkaProducer is None or self._producer is not None:
            return
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers
            )  # type: ignore[call-arg]
            await self._producer.start()  # type: ignore[union-attr]
        except Exception as exc:  # pragma: no cover
            if self._logger:
                self._logger.error("Failed to start Kafka producer", exc_info=exc)
            # In case of failure we leave the producer as None

    async def stop(self) -> None:
        """Stop the scoring loop and close resources."""
        self._running = False
        if self._producer is not None:
            try:
                await self._producer.stop()  # type: ignore[union-attr]
            finally:
                self._producer = None

    async def _publish_score(self, token: str, payload: Dict[str, Any]) -> None:
        """Publish the score payload to Kafka if the producer is available."""
        if self._producer is None:
            return
        try:
            await self._producer.send_and_wait(
                self.kafka_topic,
                value=str(payload).encode("utf-8"),
            )  # type: ignore[union-attr]
        except KafkaError as exc:  # pragma: no cover
            if self._logger:
                self._logger.warning(
                    "Failed to publish score", token=token, exc_info=exc
                )

    async def _process(self) -> None:
        """Compute scores for all tokens and persist them."""
        if not self._config:
            # Initial configuration load
            await self._load_config()
        # Load configuration parameters
        weights: Mapping[str, float] = self._config.get(
            "weights", {"roi": 0.4, "volatility": 0.3, "trend": 0.3}
        )
        vol_clip: float = self._config.get("vol_clip", 1.0)
        # Fetch all ROI records
        roi_records = await self.db.fetch_all_roi()
        if not roi_records:
            return
        confidences: list[float] = []
        vols: list[float] = []
        for token, rec in roi_records.items():
            roi_5m = float(rec.get("roi_5m", 0.0))
            roi_15m = float(rec.get("roi_15m", 0.0))
            roi_1h = float(rec.get("roi_1h", 0.0))
            volatility = float(rec.get("volatility", 0.0))
            slope = float(rec.get("roi_slope", 0.0))
            avg_roi = (roi_5m + roi_15m + roi_1h) / 3.0
            vol_norm = normalize_volatility(volatility, vol_clip)
            trend = compute_trend(slope)
            score, confidence = compute_score(avg_roi, vol_norm, trend, weights)
            # Persist score in DB
            await self.db.upsert_score(
                token=token,
                avg_roi=avg_roi,
                volatility=volatility,
                trend=trend,
                score=score,
                confidence=confidence,
            )
            # Publish to Kafka
            payload = {
                "token": token,
                "score": score,
                "roi_5m": roi_5m,
                "roi_15m": roi_15m,
                "roi_1h": roi_1h,
                "volatility": volatility,
                "confidence": confidence,
                "timestamp": int(time.time()),
            }
            await self._publish_score(token, payload)
            confidences.append(confidence)
            vols.append(volatility)
            # Update metrics
            if score_distribution is not None:
                score_distribution.observe(score)
        # Aggregate metrics after loop to avoid repeated gauge
        if confidences and score_confidence_avg is not None:
            score_confidence_avg.set(sum(confidences) / len(confidences))
        if vols and roi_volatility_avg is not None:
            roi_volatility_avg.set(sum(vols) / len(vols))

    async def run(self) -> None:
        """Main loop that periodically updates scores until stopped."""
        self._running = True
        # Ensure DB connection and Kafka producer exist
        await self.db.connect()
        await self.db.init_phase2()
        await self._ensure_producer()
        # Start a timer for metrics
        while self._running:
            start_time = time.perf_counter()
            try:
                await self._load_config()
                await self._process()
            except Exception as exc:  # pragma: no cover
                if self._logger:
                    self._logger.error("Error in scoring loop", exc_info=exc)
            finally:
                duration = time.perf_counter() - start_time
                if score_update_latency_seconds is not None:
                    score_update_latency_seconds.observe(duration)
            interval = self._config.get("update_interval_sec", 60)
            await asyncio.sleep(interval)


async def main() -> None:
    """Entry point for running the scoring engine as a script.

    This function reads the configuration file, initialises the
    database and metrics server, creates a :class:`ScoreEngine`
    instance and enters its run loop.  When run from the command
    line this function is executed via :func:`asyncio.run` in the
    ``__main__`` block below.
    """
    # Determine configuration file path from environment or use default
    config_path = os.environ.get("SCORE_ENGINE_CONFIG", "config/score_engine.yaml")
    # Load YAML to fetch Kafka and DB details
    if yaml is None:
        raise RuntimeError("PyYAML is required to run the score engine as a script")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    kafka_cfg = cfg.get("kafka", {}) or {}
    bootstrap = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topics = kafka_cfg.get("topics", {}) or {}
    scored_topic = topics.get("scored_tokens", "scored_tokens")
    db_cfg = cfg.get("db", {}) or {}
    dsn = db_cfg.get("dsn", "postgresql://postgres:postgres@localhost:5432/liberte")
    prom_cfg = cfg.get("prometheus", {}) or {}
    port = int(prom_cfg.get("port", 8003))
    # Start metrics server if available
    from .metrics import start_metrics_server  # local import to avoid circular

    start_metrics_server(port)
    # Initialise DB and run engine
    db = Database(dsn=dsn)
    engine = ScoreEngine(
        db=db,
        config_path=config_path,
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=scored_topic,
    )
    await engine.run()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())