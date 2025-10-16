"""
Unit tests for the score computation functions.

These tests verify the correctness of the mathematical formulas used to
compute the final score and confidence from ROI metrics, volatility and
trend.  The functions under test are pure and side‑effect free.
"""

import unittest

from src.score_engine.score_engine import (
    normalize_volatility,
    compute_trend,
    compute_score,
)


class TestScoreFormula(unittest.TestCase):
    def test_normalize_volatility(self) -> None:
        # Nominal case
        self.assertAlmostEqual(normalize_volatility(0.5, 1.0), 0.5)
        # Clipping at 1
        self.assertAlmostEqual(normalize_volatility(0.8, 0.4), 1.0)
        # Negative volatility returns 0 (should not happen in practice)
        self.assertEqual(normalize_volatility(-0.1, 1.0), 0.0)

    def test_compute_trend(self) -> None:
        # Tanh of zero is zero
        self.assertEqual(compute_trend(0.0), 0.0)
        # Tanh of a positive number is positive and less than 1
        self.assertAlmostEqual(compute_trend(1.0), 0.76159, places=5)
        # Tanh of a negative number is negative
        self.assertAlmostEqual(compute_trend(-1.0), -0.76159, places=5)

    def test_compute_score_and_confidence(self) -> None:
        # Example calculation using given weights
        weights = {"roi": 0.4, "volatility": 0.3, "trend": 0.3}
        avg_roi = 0.2
        vol_norm = 0.2
        trend = 0.1
        score, confidence = compute_score(avg_roi, vol_norm, trend, weights)
        expected_score = 0.4 * 0.2 + 0.3 * (1 - 0.2) + 0.3 * 0.1
        expected_confidence = 1.0 - 0.2
        self.assertAlmostEqual(score, expected_score)
        self.assertAlmostEqual(confidence, expected_confidence)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()