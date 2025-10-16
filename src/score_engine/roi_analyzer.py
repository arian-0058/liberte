"""
ROI Analyzer for phase 2
========================

This module is responsible for computing rolling return metrics for
each token based on historical trade data.  The primary function,
:func:`compute_roi_windows`, accepts a sequence of trade tuples and
produces three ROI estimates covering 5 minutes, 15 minutes and 1 hour.
It works without requiring access to external dependencies.

The module also exposes helper functions to compute volatility (as the
population standard deviation) and a simple linear regression to
estimate the ROI trend.  Together these helpers form the core of the
ROI analyzer and can be unit tested in isolation.

While the high level design envisages a background task that
consumes messages from the ``filtered_tokens`` topic and publishes
results to the database, the asynchronous loop itself is left
optional.  Tests are free to call the pure functions directly and
bypass Kafka entirely.  A minimal :class:`RoiAnalyzer` class is
provided as a potential home for a future real consumer implementation
should the need arise.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple, Optional

from .db import Database


def compute_roi_windows(
    trades: Sequence[Tuple[float, float]],
    now: Optional[float] = None,
    windows: Sequence[int] = (5 * 60, 15 * 60, 60 * 60),
) -> Tuple[float, float, float]:
    """Compute ROI values over rolling windows.

    Parameters
    ----------
    trades:
        A sequence of tuples ``(timestamp, roi)``.  The timestamp is a
        UNIX timestamp in seconds, and ``roi`` represents the return
        on investment for that trade relative to some baseline (e.g.,
        the price at the start of the trading day).  The sequence may
        be unsorted; only trades occurring within each window will
        contribute to that window's ROI.

    now:
        The current time as a UNIX timestamp.  If omitted the
        function uses ``time.time()``.

    windows:
        The widths of the rolling windows in seconds.  Exactly three
        windows are expected: 300 seconds (5 minutes), 900 seconds
        (15 minutes) and 3600 seconds (1 hour).  Custom values may be
        supplied for testing or experimentation.

    Returns
    -------
    Tuple[float, float, float]
        A tuple ``(roi_5m, roi_15m, roi_1h)`` corresponding to the
        average ROI of trades whose timestamps lie within the last
        5/15/60 minutes.  If no trades fall within a window the ROI
        value is ``0.0`` for that window.
    """
    if len(windows) != 3:
        raise ValueError("windows must contain exactly three entries")

    # Normalise time reference
    now_ts = float(now) if now is not None else time.time()

    # Prepare return list
    roi_values: List[float] = []
    results: List[float] = []

    # For each window compute the mean ROI of trades that fall into
    # that window.  If no trades meet the criteria we append 0.0.
    for win in windows:
        cutoff = now_ts - win
        selected = [roi for ts, roi in trades if ts >= cutoff]
        if selected:
            results.append(sum(selected) / len(selected))
        else:
            results.append(0.0)

    # Unpack into three values
    return results[0], results[1], results[2]


def calculate_volatility(values: Sequence[float]) -> float:
    """Return the standard deviation of the given values.

    The population standard deviation is used because the values are
    taken from the entire window, not a sample.  When fewer than two
    values are provided the function returns ``0.0``.  A very small
    epsilon is added to the result to prevent division by zero in
    downstream computations.
    """
    if not values or len(values) < 2:
        return 0.0
    try:
        sd = statistics.pstdev(values)
    except statistics.StatisticsError:
        return 0.0
    return float(sd)


def calculate_slope(x: Sequence[float], y: Sequence[float]) -> float:
    """Compute the slope of the regression line for the data points.

    Parameters
    ----------
    x:
        Sequence of x positions.  For the ROI trend calculation this is
        expected to be ``(5, 15, 60)`` corresponding to minutes.

    y:
        Sequence of y values (ROI measurements) corresponding to each
        x position.

    Returns
    -------
    float
        The slope of the least squares regression line.  When fewer
        than two points are provided or the variance of ``x`` is zero
        the function returns ``0.0`` to avoid division by zero.
    """
    if len(x) != len(y) or not x:
        raise ValueError("x and y must have the same length and not be empty")
    n = len(x)
    if n < 2:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    if var_x == 0:
        return 0.0
    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    return cov_xy / var_x


@dataclass
class RoiMetrics:
    """Dataclass to hold computed ROI metrics for a token."""
    roi_5m: float
    roi_15m: float
    roi_1h: float
    volatility: float
    roi_slope: float


class RoiAnalyzer:
    """Async ROI analyzer stub.

    In a production deployment this class would subscribe to the
    ``filtered_tokens`` Kafka topic, fetch the recent paper trades
    associated with each token from the phase 1 execution stub and
    compute rolling ROI metrics.  Because ``aiokafka`` may not be
    available in the execution environment, this implementation does
    not include Kafka integration; instead it provides a convenience
    method to compute and persist ROI metrics directly.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    async def analyze_trades(self, token: str, trades: Sequence[Tuple[float, float]]) -> RoiMetrics:
        """Compute ROI metrics for a token and persist them.

        Parameters
        ----------
        token:
            The token identifier.  Used as the key in the database.

        trades:
            A sequence of ``(timestamp, roi)`` tuples representing the
            recent paper trades for the token.  Timestamps are UNIX
            seconds and ``roi`` values are per trade ROI values.  The
            list need not be sorted.

        Returns
        -------
        RoiMetrics
            A dataclass containing the computed metrics.
        """
        roi_5m, roi_15m, roi_1h = compute_roi_windows(trades)
        # Use the three ROI measurements to compute volatility and slope
        volatility = calculate_volatility([roi_5m, roi_15m, roi_1h])
        # Note: x positions are in minutes for slope
        slope = calculate_slope([5, 15, 60], [roi_5m, roi_15m, roi_1h])
        # Persist the computed values
        await self.db.upsert_roi_history(
            token=token,
            roi_5m=roi_5m,
            roi_15m=roi_15m,
            roi_1h=roi_1h,
            volatility=volatility,
            roi_slope=slope,
        )
        return RoiMetrics(roi_5m, roi_15m, roi_1h, volatility, slope)