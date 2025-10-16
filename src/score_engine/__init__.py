"""
score_engine package
====================

This package contains the components required to implement phase 2 of the
Liberte/Signal Hunter project.  It exposes a small API that is convenient
to use from tests and other Python modules while avoiding heavy
dependencies when optional packages such as ``aiokafka`` or ``asyncpg``
are not installed.  The top‑level imports below re‑export the most
commonly used classes and helper functions.

Key exports:

* :class:`score_engine.db.Database` – A lightweight database wrapper
  capable of either connecting to PostgreSQL via ``asyncpg`` or
  operating purely in memory when ``asyncpg`` is unavailable.  It
  provides convenience methods for creating the necessary tables and
  upserting ROI and score records.

* :func:`score_engine.score_engine.compute_score` – Pure function
  implementing the core scoring formula.  It accepts ROI values,
  volatility and trend along with a configuration dictionary and
  returns a tuple of ``(score, confidence)``.  This function does not
  depend on any external state and is therefore trivially unit
  testable.

* :func:`score_engine.roi_analyzer.compute_roi_windows` – Function for
  computing ROI windows over arbitrary trade histories.  Given a list
  of trade tuples and a current timestamp, it returns the three ROI
  values used by the scorer.  A trade tuple consists of
  ``(timestamp, price)`` and may be easily derived from the
  ``paper_trades`` table populated by phase 1.

These functions are surfaced here so tests can import them directly
without needing to wade through the internal module hierarchy.
"""

from .db import Database
from .score_engine import compute_score, normalize_volatility, compute_trend
from .roi_analyzer import compute_roi_windows, calculate_volatility, calculate_slope

__all__ = [
    "Database",
    "compute_score",
    "normalize_volatility",
    "compute_trend",
    "compute_roi_windows",
    "calculate_volatility",
    "calculate_slope",
]