"""
Prometheus metrics for phase 2
==============================

This module centralises the definition of Prometheus metrics used by
both the ROI analyzer and scoring engine.  Defining all metrics in
one place simplifies both unit testing (by allowing metrics to be
replaced with dummies) and observability (by ensuring that all
metrics share the same namespace and lifecycle).

If the ``prometheus_client`` package is unavailable the metrics
variables are set to ``None``.  Code that updates metrics should
therefore test for ``None`` before calling methods on them.  This
approach avoids import errors when running in minimal environments.
"""

from __future__ import annotations

from typing import Optional

try:
    from prometheus_client import (
        Histogram,
        Gauge,
        start_http_server,
    )  # type: ignore
except ImportError:  # pragma: no cover
    Histogram = None  # type: ignore
    Gauge = None  # type: ignore
    start_http_server = None  # type: ignore


def _histogram(name: str, documentation: str, **kwargs):
    """Create a Histogram metric if prometheus_client is available."""
    if Histogram is None:
        return None
    return Histogram(name, documentation, **kwargs)


def _gauge(name: str, documentation: str):
    """Create a Gauge metric if prometheus_client is available."""
    if Gauge is None:
        return None
    return Gauge(name, documentation)


# Latency of ROI analyzer (unused currently but defined for completeness)
roi_analyzer_latency_seconds: Optional[Histogram] = _histogram(
    "roi_analyzer_latency_seconds", "Time spent processing ROI events"
)

# Average volatility across tokens
roi_volatility_avg: Optional[Gauge] = _gauge(
    "roi_volatility_avg", "Average volatility of ROI values"
)

# Latency of score updates
score_update_latency_seconds: Optional[Histogram] = _histogram(
    "score_update_latency_seconds", "Time spent updating scores"
)

# Average confidence across tokens
score_confidence_avg: Optional[Gauge] = _gauge(
    "score_confidence_avg", "Average confidence of scores"
)

# Distribution of score values
score_distribution: Optional[Histogram] = _histogram(
    "score_distribution",
    "Distribution of computed scores",
    buckets=[i / 20 for i in range(-20, 21)],  # buckets from -1.0 to 1.0 in steps of 0.05
)

def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics HTTP server on the given port.

    When the ``prometheus_client`` package is not installed this
    function simply returns without doing anything.
    """
    if start_http_server is None:
        return
    start_http_server(port)