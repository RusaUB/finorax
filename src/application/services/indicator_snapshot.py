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
    def __init__(self, indicator_service: IndicatorServicePort) -> None:
        self.ind = indicator_service
        self._log = logging.getLogger(__name__)

    def build(self, asset: Asset, at: datetime) -> IndicatorSnapshot:
        self._require_utc(at, "at")

        parts: list[str] = []

        try:
            rsi_1h = self.ind.get_rsi(asset=asset, at=at, timeframe="1h", period=14)
            parts.append(f"RSI(14,1h)={rsi_1h:.2f}")
            self._log.info("Indicators: snapshot RSI computed", extra={"asset": asset.symbol, "value": rsi_1h})
        except Exception as e:
            parts.append("RSI(14,1h)=NA")
            self._log.debug("Indicators: snapshot RSI failed", extra={"asset": asset.symbol, "error": str(e)})

        try:
            cross = self.ind.get_sma_cross(asset=asset, at=at, timeframe="1h", fast_period=50, slow_period=200)
            parts.append(f"SMA50/200(1h)={cross.fast:.2f}/{cross.slow:.2f},{cross.crossed or 'no-cross'}")
            self._log.info("Indicators: snapshot SMA cross computed", extra={"asset": asset.symbol, "crossed": cross.crossed})
        except Exception as e:
            parts.append("SMA50/200(1h)=NA")
            self._log.debug("Indicators: snapshot SMA cross failed", extra={"asset": asset.symbol, "error": str(e)})

        text = "; ".join(parts)
        return IndicatorSnapshot(text=text)

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
