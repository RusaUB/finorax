import pytest
from src.shared.utils.time import snap_to_interval
import datetime as dt

UTC = dt.timezone.utc

@pytest.mark.parametrize(
    "dt,freq,mode,dt_exp",
    [
        # --- nearest (typical hour/minute cases) ---
        (dt.datetime(2025, 9, 4, 21, 39, tzinfo=UTC), "1h",  "nearest",
         dt.datetime(2025, 9, 4, 22, 0, tzinfo=UTC)),  # closer up
        (dt.datetime(2025, 9, 4, 21, 20, tzinfo=UTC), "1h",  "nearest",
         dt.datetime(2025, 9, 4, 21, 0, tzinfo=UTC)),  # closer down
        (dt.datetime(2025, 9, 4, 21, 20, tzinfo=UTC), "30m", "nearest",
         dt.datetime(2025, 9, 4, 21, 30, tzinfo=UTC)), # closer 21:30

        # --- tie (exactly half interval) should round UP (half-up rule) ---
        (dt.datetime(2025, 9, 4, 21, 30, tzinfo=UTC), "1h",  "nearest",
         dt.datetime(2025, 9, 4, 22, 0, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 21, 10, 30, tzinfo=UTC), "1m", "nearest",
         dt.datetime(2025, 9, 4, 21, 11, 0, tzinfo=UTC)),  # 30 seconds -> up

        # --- floor (always snap down to lower boundary) ---
        (dt.datetime(2025, 9, 4, 21, 39, tzinfo=UTC), "1h",  "floor",
         dt.datetime(2025, 9, 4, 21, 0, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 21, 20, tzinfo=UTC), "30m", "floor",
         dt.datetime(2025, 9, 4, 21, 0, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 0, 0, tzinfo=UTC),   "1d",  "floor",
         dt.datetime(2025, 9, 4, 0, 0, tzinfo=UTC)),  # exact boundary

        # --- ceil (always snap up to upper boundary unless already on grid) ---
        (dt.datetime(2025, 9, 4, 21, 39, tzinfo=UTC), "1h",  "ceil",
         dt.datetime(2025, 9, 4, 22, 0, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 21, 20, tzinfo=UTC), "30m", "ceil",
         dt.datetime(2025, 9, 4, 21, 30, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 0, 0, tzinfo=UTC),   "1d",  "ceil",
         dt.datetime(2025, 9, 4, 0, 0, tzinfo=UTC)),  # exact boundary stays

        # --- 1d nearest: noon is a tie -> should round UP to next midnight ---
        (dt.datetime(2025, 9, 4, 12, 0, tzinfo=UTC),  "1d",  "nearest",
         dt.datetime(2025, 9, 5, 0, 0, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 11, 59, tzinfo=UTC), "1d",  "nearest",
         dt.datetime(2025, 9, 4, 0, 0, tzinfo=UTC)),  # closer down

        # --- whitespace / case in freq should be accepted by parser ---
        (dt.datetime(2025, 9, 4, 21, 20, tzinfo=UTC), " 30M ", "nearest",
         dt.datetime(2025, 9, 4, 21, 30, tzinfo=UTC)),
        (dt.datetime(2025, 9, 4, 21, 39, tzinfo=UTC), " 1H",   "floor",
         dt.datetime(2025, 9, 4, 21, 0,  tzinfo=UTC)),

        # --- nontrivial step: 90 minutes ---
        (dt.datetime(2025, 9, 4, 1, 31, tzinfo=UTC),  "90m", "nearest",
         # 90m grid: 00:00, 01:30, 03:00... -> 01:31 is closer to 01:30
         dt.datetime(2025, 9, 4, 1, 30, tzinfo=UTC)),

        # --- 24h vs 1d should behave equivalently ---
        (dt.datetime(2025, 9, 4, 13, 0, tzinfo=UTC), "24h", "nearest",
         dt.datetime(2025, 9, 5, 0, 0, tzinfo=UTC)),
    ],
)
def test_snap_core_cases(dt,freq,mode,dt_exp):
    dt_output = snap_to_interval(dt=dt,freq=freq,mode=mode)
    assert dt_output == dt_exp
    assert dt_output.tzinfo is not None


def test_idempotency():
    """
    Snapping an already-snapped timestamp should be idempotent.
    """
    base = dt.datetime(2025, 9, 4, 21, 30, tzinfo=UTC)
    once = snap_to_interval(dt=base, freq="30m", mode="nearest")
    twice = snap_to_interval(dt=once, freq="30m", mode="nearest")
    assert once == twice == base

@pytest.mark.parametrize("bad_freq", [
    "h",       # missing number
    "1x",      # unknown unit
    "-1h",     # negative not allowed (regex won't match, still ValueError)
    "1.5h",    # decimals not allowed
])
def test_bad_freq_raises(bad_freq):
    with pytest.raises(ValueError):
        snap_to_interval(freq=bad_freq) 