from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from src.application.ports import IndicatorServicePort
from src.domain.assets import Asset


@dataclass
class _OHLCV:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class CcxtIndicatorService(IndicatorServicePort):
    """
    Indicator service backed by ccxt.

    Computes RSI for an asset at a specific UTC time using OHLCV data
    from the configured exchange. RSI is computed on candle closes, and
    the value returned corresponds to the last fully closed candle at or
    before the given `at` time.
    """

    def __init__(self, exchange_id: str = "binance", enable_rate_limit: bool = True) -> None:
        try:
            import ccxt  # type: ignore
        except Exception as e:  # pragma: no cover - runtime dependency guard
            raise ImportError("ccxt is required to use CcxtIndicatorService") from e

        if not hasattr(ccxt, exchange_id):
            raise ValueError(f"Unknown ccxt exchange: {exchange_id}")
        self._ccxt = ccxt
        self.exchange = getattr(ccxt, exchange_id)({"enableRateLimit": enable_rate_limit})
        # Load markets once to validate symbols
        self.exchange.load_markets()

    def get_rsi(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        period: int = 14,
        market: Optional[str] = None,
        quote: str = "USDT",
    ) -> float:
        self._require_utc(at, "at")
        if period <= 0:
            raise ValueError("period must be positive")

        symbol = market or f"{(asset.symbol or '').strip().upper()}/{quote.strip().upper()}"
        if symbol not in self.exchange.markets:
            # Attempt a reload in case of initial cache miss
            self.exchange.load_markets(True)
            if symbol not in self.exchange.markets:
                raise ValueError(f"Market symbol not found on exchange: {symbol}")

        frame_ms = int(self.exchange.parse_timeframe(timeframe) * 1000)
        if frame_ms <= 0:
            raise ValueError(f"Invalid timeframe: {timeframe}")

        t_ms = int(at.timestamp() * 1000)
        # Choose the last fully closed candle at or before `at`
        target_ms = ((t_ms - 1) // frame_ms) * frame_ms

        # Need enough candles to compute a stable RSI up to target
        # Use 3*period to stabilize Wilder's smoothing
        lookback = period * 3
        since_ms = target_ms - lookback * frame_ms
        limit = lookback + 2  # a little extra

        raw = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit)
        ohlcv = self._normalize_ohlcv(raw)
        if not ohlcv:
            raise ValueError("No OHLCV data returned")

        # Ensure we have the target candle in the set (some exchanges ignore `since` granularity)
        have_target = any(c.ts_ms == target_ms for c in ohlcv)
        if not have_target:
            # Try fetching one more page forward starting exactly at target to catch boundary behavior
            more = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=target_ms, limit=period + 2)
            extra = self._normalize_ohlcv(more)
            # Merge and unique by ts
            by_ts = {c.ts_ms: c for c in ohlcv}
            for c in extra:
                by_ts[c.ts_ms] = c
            ohlcv = [by_ts[k] for k in sorted(by_ts.keys())]

        closes = [c.close for c in ohlcv]
        timestamps = [c.ts_ms for c in ohlcv]
        rsi_values = self._rsi_series(closes, period)

        # Map rsi values to timestamps starting from the first index that yields an RSI
        first_rsi_idx = next((i for i, v in enumerate(rsi_values) if v is not None), None)
        if first_rsi_idx is None:
            raise ValueError("Insufficient data to compute RSI")

        ts_to_rsi = {timestamps[i]: rsi_values[i] for i in range(first_rsi_idx, len(timestamps)) if rsi_values[i] is not None}
        if target_ms not in ts_to_rsi:
            # If target candle missing (e.g., exchange returned future/incomplete candles), take last available before target
            prior_ts = [ts for ts in ts_to_rsi.keys() if ts <= target_ms]
            if not prior_ts:
                raise ValueError("RSI not available for the requested time")
            closest_ts = max(prior_ts)
            return float(ts_to_rsi[closest_ts])

        return float(ts_to_rsi[target_ms])

    def get_sma(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        period: int = 14,
        market: Optional[str] = None,
        quote: str = "USDT",
    ) -> float:
        self._require_utc(at, "at")
        if period <= 0:
            raise ValueError("period must be positive")

        symbol = market or f"{(asset.symbol or '').strip().upper()}/{quote.strip().upper()}"
        if symbol not in self.exchange.markets:
            self.exchange.load_markets(True)
            if symbol not in self.exchange.markets:
                raise ValueError(f"Market symbol not found on exchange: {symbol}")

        frame_ms = int(self.exchange.parse_timeframe(timeframe) * 1000)
        t_ms = int(at.timestamp() * 1000)
        target_ms = ((t_ms - 1) // frame_ms) * frame_ms

        required = period
        since_ms = target_ms - (required - 1) * frame_ms
        limit = required + 2

        raw = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit)
        ohlcv = self._normalize_ohlcv(raw)
        if not any(c.ts_ms == target_ms for c in ohlcv):
            more = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=target_ms, limit=period + 2)
            ohlcv = self._merge_ohlcv(ohlcv, self._normalize_ohlcv(more))

        idx = self._index_of_ts(ohlcv, target_ms)
        if idx is None or idx + 1 < period:
            raise ValueError("Insufficient data to compute SMA")

        closes = [c.close for c in ohlcv]
        sma = self._sma_at_index(closes, period, idx)
        if sma is None:
            raise ValueError("SMA not available for the requested time")
        return float(sma)

    def get_sma_cross(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        fast_period: int = 50,
        slow_period: int = 200,
        market: Optional[str] = None,
        quote: str = "USDT",
    ):
        from src.application.ports import SMACrossDTO

        self._require_utc(at, "at")
        if fast_period <= 0 or slow_period <= 0:
            raise ValueError("periods must be positive")
        if fast_period >= slow_period:
            raise ValueError("fast_period must be less than slow_period")

        symbol = market or f"{(asset.symbol or '').strip().upper()}/{quote.strip().upper()}"
        if symbol not in self.exchange.markets:
            self.exchange.load_markets(True)
            if symbol not in self.exchange.markets:
                raise ValueError(f"Market symbol not found on exchange: {symbol}")

        frame_ms = int(self.exchange.parse_timeframe(timeframe) * 1000)
        t_ms = int(at.timestamp() * 1000)
        target_ms = ((t_ms - 1) // frame_ms) * frame_ms

        # Need slow_period + 1 candles to compute prev and curr SMA for slow window
        required = slow_period + 1
        since_ms = target_ms - (required - 1) * frame_ms
        limit = required + 3

        raw = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit)
        ohlcv = self._normalize_ohlcv(raw)
        if not any(c.ts_ms == target_ms for c in ohlcv):
            more = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=target_ms, limit=slow_period + 3)
            ohlcv = self._merge_ohlcv(ohlcv, self._normalize_ohlcv(more))

        idx = self._index_of_ts(ohlcv, target_ms)
        if idx is None or idx < slow_period:
            raise ValueError("Insufficient data to compute SMA cross")

        closes = [c.close for c in ohlcv]
        fast_curr = self._sma_at_index(closes, fast_period, idx)
        slow_curr = self._sma_at_index(closes, slow_period, idx)
        fast_prev = self._sma_at_index(closes, fast_period, idx - 1)
        slow_prev = self._sma_at_index(closes, slow_period, idx - 1)

        if None in (fast_curr, slow_curr, fast_prev, slow_prev):
            raise ValueError("Insufficient data to compute SMA cross")

        crossed: str | None
        if fast_prev < slow_prev and fast_curr >= slow_curr:
            crossed = "bullish"
        elif fast_prev > slow_prev and fast_curr <= slow_curr:
            crossed = "bearish"
        else:
            crossed = None

        return SMACrossDTO(
            fast=float(fast_curr),
            slow=float(slow_curr),
            prev_fast=float(fast_prev),
            prev_slow=float(slow_prev),
            crossed=crossed,
        )

    # --- helpers ---
    def _normalize_ohlcv(self, rows: List[List[float]]) -> List[_OHLCV]:
        out: List[_OHLCV] = []
        for r in rows or []:
            if not r or len(r) < 6:
                continue
            ts, o, h, l, c, v = r[:6]
            try:
                out.append(_OHLCV(ts_ms=int(ts), open=float(o), high=float(h), low=float(l), close=float(c), volume=float(v)))
            except Exception:
                continue
        # Ensure sorted by timestamp ascending
        out.sort(key=lambda x: x.ts_ms)
        return out

    def _rsi_series(self, closes: List[float], period: int) -> List[Optional[float]]:
        if len(closes) < period + 1:
            return [None] * len(closes)

        # Price changes
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [max(ch, 0.0) for ch in changes]
        losses = [max(-ch, 0.0) for ch in changes]

        # Wilder's smoothing
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        rsi: List[Optional[float]] = [None] * len(closes)

        def compute_rsi(ag: float, al: float) -> float:
            if al == 0:
                return 100.0
            rs = ag / al
            return 100.0 - (100.0 / (1.0 + rs))

        # First RSI value corresponds to index `period`
        rsi[period] = compute_rsi(avg_gain, avg_loss)

        for i in range(period + 1, len(closes)):
            gain = gains[i - 1]  # aligned: gains/losses are one shorter
            loss = losses[i - 1]
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            rsi[i] = compute_rsi(avg_gain, avg_loss)

        return rsi

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")

    def _index_of_ts(self, ohlcv: List[_OHLCV], ts_ms: int) -> Optional[int]:
        for i, c in enumerate(ohlcv):
            if c.ts_ms == ts_ms:
                return i
        return None

    def _sma_at_index(self, closes: List[float], period: int, idx: int) -> Optional[float]:
        if idx + 1 < period:
            return None
        start = idx - period + 1
        window = closes[start : idx + 1]
        return sum(window) / float(period)

    def _merge_ohlcv(self, a: List[_OHLCV], b: List[_OHLCV]) -> List[_OHLCV]:
        by_ts = {c.ts_ms: c for c in a}
        for c in b:
            by_ts[c.ts_ms] = c
        out = [by_ts[k] for k in sorted(by_ts.keys())]
        return out