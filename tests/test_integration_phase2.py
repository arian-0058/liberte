"""
Lightweight integration test for phase 2 components.

This test stitches together the ROI analyzer and the scoring engine
without relying on external services such as Kafka or PostgreSQL.  It
exercises the full data flow from ingesting synthetic trades through
writing ROI metrics to computing and persisting a final score.

Only the happy path is covered here.  Error cases such as invalid
configuration files or missing dependencies are outside the scope of
this integration test.
"""

import asyncio
import os
import unittest

from src.score_engine.db import Database
from src.score_engine.roi_analyzer import RoiAnalyzer
from src.score_engine.score_engine import ScoreEngine
from src.score_engine.metrics import (
    score_confidence_avg,
    roi_volatility_avg,
)


class TestIntegrationPhase2(unittest.TestCase):
    def setUp(self) -> None:
        # Use a temporary config file path for the test.  Copy the
        # default configuration into a test specific location.
        self.config_path = "config/test_score_engine.yaml"
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        if not os.path.exists(self.config_path):
            with open("config/score_engine.yaml", "r", encoding="utf-8") as src:
                with open(self.config_path, "w", encoding="utf-8") as dst:
                    dst.write(src.read())

    def tearDown(self) -> None:
        # Clean up the temporary config file
        try:
            os.remove(self.config_path)
        except FileNotFoundError:
            pass

    def test_end_to_end_scoring(self) -> None:
        async def run_test() -> None:
            # Initialise DB in in‑memory mode (asyncpg unavailable)
            db = Database(dsn="postgresql://postgres:postgres@localhost:5432/liberte")
            await db.connect()
            await db.init_phase2()
            # Create ROI analyzer and ingest synthetic trades
            analyzer = RoiAnalyzer(db)
            token = "0xTEST"
            now = 100.0
            trades = [
                (now - 2 * 60, 0.1),  # within 5m
                (now - 7 * 60, 0.2),  # within 15m
                (now - 20 * 60, 0.3),  # within 1h
            ]
            metrics = await analyzer.analyze_trades(token, trades)
            # Ensure ROI metrics are persisted
            roi_rec = await db.fetch_roi(token)
            self.assertIsNotNone(roi_rec)
            # Create score engine; Kafka settings are irrelevant for the test
            engine = ScoreEngine(
                db=db,
                config_path=self.config_path,
                kafka_bootstrap_servers="localhost:9092",
                kafka_topic="scored_tokens",
            )
            # Run a single processing step
            await engine._load_config()
            await engine._process()
            # Fetch the persisted score
            score_rec = await db.fetch_score(token)
            self.assertIsNotNone(score_rec)
            # Compute expected values manually
            avg_roi = (metrics.roi_5m + metrics.roi_15m + metrics.roi_1h) / 3.0
            # Extract configuration from the loaded file
            weights = engine._config.get("weights", {})
            vol_clip = engine._config.get("vol_clip", 1.0)
            vol_norm = metrics.volatility / vol_clip if vol_clip else 0.0
            if vol_norm > 1:
                vol_norm = 1.0
            trend = engine._config.get("trend_fn", None)
            # Using compute_score from score_engine would duplicate the logic; instead
            # we recompute directly here for clarity
            w_roi = float(weights.get("roi", 0.0))
            w_vol = float(weights.get("volatility", 0.0))
            w_trend = float(weights.get("trend", 0.0))
            expected_score = w_roi * avg_roi + w_vol * (1 - vol_norm) + w_trend * math.tanh(metrics.roi_slope)
            expected_conf = max(0.0, 1 - vol_norm)
            self.assertAlmostEqual(score_rec["score"], expected_score)
            self.assertAlmostEqual(score_rec["confidence"], expected_conf)
            # Metrics: ensure gauges have been updated (if metrics are enabled).  We
            # access the collected samples via the public ``collect`` API rather than
            # private attributes which may change between versions.
            if score_confidence_avg is not None:
                families = list(score_confidence_avg.collect())
                if families:
                    samples = families[0].samples
                    self.assertTrue(samples, "score_confidence_avg has no samples")
                    # The value should be non‑negative
                    self.assertGreaterEqual(samples[0].value, 0.0)
            if roi_volatility_avg is not None:
                families = list(roi_volatility_avg.collect())
                if families:
                    samples = families[0].samples
                    self.assertTrue(samples, "roi_volatility_avg has no samples")
                    self.assertGreaterEqual(samples[0].value, 0.0)

        import math
        asyncio.run(run_test())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()