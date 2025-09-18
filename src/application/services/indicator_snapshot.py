from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import logging

from src.application.ports import IndicatorServicePort
from src.domain.assets import Asset


@dataclass(frozen=True)
class IndicatorSnapshot:
    text: str


class IndicatorSnapshotBuilder:
    def __init__(
        self,
        indicator_service: IndicatorServicePort,
        *,
        timeframe: str = "1h",
        rsi_period: int = 14,
        sma_fast: int = 50,
        sma_slow: int = 200,
    ) -> None:
        self.ind = indicator_service
        self.timeframe = timeframe
        self.rsi_period = rsi_period
        self.sma_fast = sma_fast
        self.sma_slow = sma_slow
        self._log = logging.getLogger(__name__)

    def build(self, asset: Asset, at: datetime) -> IndicatorSnapshot:
        self._require_utc(at, "at")

        parts: list[str] = []

        try:
            rsi_val = self.ind.get_rsi(asset=asset, at=at, timeframe=self.timeframe, period=self.rsi_period)
            parts.append(f"RSI({self.rsi_period},{self.timeframe})={rsi_val:.2f}")
            self._log.info("Indicators: snapshot RSI computed", extra={"asset": asset.symbol, "value": rsi_val})
        except Exception as e:
            parts.append(f"RSI({self.rsi_period},{self.timeframe})=NA")
            self._log.debug("Indicators: snapshot RSI failed", extra={"asset": asset.symbol, "error": str(e)})

        try:
            cross = self.ind.get_sma_cross(asset=asset, at=at, timeframe=self.timeframe, fast_period=self.sma_fast, slow_period=self.sma_slow)
            parts.append(f"SMA{self.sma_fast}/{self.sma_slow}({self.timeframe})={cross.fast:.2f}/{cross.slow:.2f},{cross.crossed or 'no-cross'}")
            self._log.info("Indicators: snapshot SMA cross computed", extra={"asset": asset.symbol, "crossed": cross.crossed})
        except Exception as e:
            parts.append(f"SMA{self.sma_fast}/{self.sma_slow}({self.timeframe})=NA")
            self._log.debug("Indicators: snapshot SMA cross failed", extra={"asset": asset.symbol, "error": str(e)})

        text = "; ".join(parts)
        return IndicatorSnapshot(text=text)

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
