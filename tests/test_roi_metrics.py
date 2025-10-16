"""
Unit tests for ROI metric calculations.

These tests exercise the ROI window aggregation, volatility calculation
and slope determination functions.  The test cases use simple
synthetic data where the expected outputs can be calculated by hand.
"""

import unittest

from src.score_engine.roi_analyzer import (
    compute_roi_windows,
    calculate_volatility,
    calculate_slope,
)


class TestRoiMetrics(unittest.TestCase):
    def setUp(self) -> None:
        # Fixed current time for reproducibility
        self.now = 100.0
        # Trades: (timestamp, roi)
        # One trade in the last 5 minutes, one additional trade in the
        # last 15 minutes and one more in the last hour.
        self.trades = [
            (self.now - 2 * 60, 0.1),  # within 5m
            (self.now - 7 * 60, 0.2),  # between 5m and 15m
            (self.now - 20 * 60, 0.3),  # between 15m and 1h
        ]

    def test_compute_roi_windows(self) -> None:
        roi_5m, roi_15m, roi_1h = compute_roi_windows(self.trades, now=self.now)
        self.assertAlmostEqual(roi_5m, 0.1)
        self.assertAlmostEqual(roi_15m, (0.1 + 0.2) / 2)
        self.assertAlmostEqual(roi_1h, (0.1 + 0.2 + 0.3) / 3)

    def test_calculate_volatility(self) -> None:
        values = [0.1, 0.15, 0.2]
        vol = calculate_volatility(values)
        # Expected population stddev: sqrt(((−0.05)^2 + 0^2 + 0.05^2)/3)
        expected = ((0.05**2 + 0.05**2) / 3) ** 0.5
        self.assertAlmostEqual(vol, expected)
        # Should be zero for fewer than two points
        self.assertEqual(calculate_volatility([0.1]), 0.0)
        self.assertEqual(calculate_volatility([]), 0.0)

    def test_calculate_slope(self) -> None:
        x = [5, 15, 60]
        y = [0.1, 0.15, 0.2]
        slope = calculate_slope(x, y)
        # Expected slope ~0.0016 (see manual calculation in HLD)
        self.assertAlmostEqual(slope, 0.0016, places=4)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()