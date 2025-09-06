from datetime import datetime, timezone
from typing import Literal
import re

_UNITS = {"m": 60, "h": 3600, "d": 86400}

def _parse_freq(freq: str) -> int:
    """'30m' -> 1800; '1h' -> 3600; '1d' -> 86400."""
    m = re.fullmatch(r"\s*(\d+)\s*([mhdMHD])\s*", freq)
    if not m:
        raise ValueError("freq must look like '30m', '1h', or '1d'")
    n, u = m.groups()
    return int(n) * _UNITS[u.lower()]

def snap_to_interval(
    dt: datetime | None = None,
    freq: str = "1h",
    mode: Literal["nearest", "floor", "ceil"] = "nearest",
) -> datetime:
    """
    Snaps the current UTC time to a grid interval.
    mode:
    -'floor' -> down (start of the current interval)
    -'ceil' -> up (start of the next interval)
    -'nearest' -> to the nearest boundary; rounds up on a tie
    Returns a timezone-aware datetime in UTC.
    """
    step = _parse_freq(freq)                    
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)   
    ts = dt.timestamp()                       
    rem = ts % step                         

    if mode == "floor":
        snapped = ts - rem
    elif mode == "ceil":
        snapped = ts if rem == 0 else ts + (step - rem)
    elif mode == "nearest":
        snapped = ts - rem if rem < (step / 2) else ts + (step - rem)
    else:
        raise ValueError("mode must be 'nearest' | 'floor' | 'ceil'")

    return datetime.fromtimestamp(snapped, tz=timezone.utc)