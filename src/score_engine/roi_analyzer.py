import math
from typing import List, Tuple, Dict
from datetime import datetime, timezone
import structlog
from .metrics import roi_seconds, roi_computations_total

logger = structlog.get_logger(__name__)

def _to_unix(ts):
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=timezone.utc).timestamp()
    raise ValueError("unsupported timestamp format")

def _window(series, seconds_back: int):
    cutoff = series[-1][0] - seconds_back
    i = 0
    for i in range(len(series) - 1, -1, -1):
        if series[i][0] < cutoff:
            break
    j = max(0, i)
    return [p for p in series[j:]]

def _returns(prices: List[float]):
    out = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            out.append(math.log(prices[i] / prices[i - 1]))
    return out

def _slope(xs: List[float], ys: List[float]):
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den

def analyze(series: List[Tuple], timeframes=("5m","15m","1h"), min_points=8) -> Dict[str, Dict[str, float]]:
    with roi_seconds.time():
        roi_computations_total.inc()
        if not series or len(series) < min_points:
            return {}
        norm = [(_to_unix(t), float(p)) for (t, p) in series]
        norm.sort(key=lambda x: x[0])
        res = {}
        tf_map = {"5m": 300, "15m": 900, "1h": 3600}
        for tf in timeframes:
            seconds = tf_map[tf]
            win = _window(norm, seconds)
            if len(win) < min_points:
                continue
            prices = [p for (_, p) in win]
            ts = [ (t - win[0][0]) / 60.0 for (t, _) in win ]

            start_p, end_p = prices[0], prices[-1]
            if start_p <= 0:
                continue
            roi = (end_p / start_p) - 1.0
            rets = _returns(prices)
            vol = (math.sqrt(252*24*60/seconds)* (math.sqrt(sum((r)**2 for r in rets)/max(1,len(rets))))) if rets else 0.0
            slp = _slope(ts, prices)

            res[tf] = {
                "roi": roi,
                "volatility": vol,
                "slope": slp,
                "window_start": win[0][0],
                "window_end": win[-1][0],
            }
        return res
