from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from src.application.ports import IndicatorServicePort
from src.domain.assets import Asset


@dataclass(frozen=True)
class IndicatorSnapshot:
    text: str


class IndicatorSnapshotBuilder:
    def __init__(self, indicator_service: IndicatorServicePort) -> None:
        self.ind = indicator_service

    def build(self, asset: Asset, at: datetime) -> IndicatorSnapshot:
        self._require_utc(at, "at")

        parts: list[str] = []

        try:
            rsi_1h = self.ind.get_rsi(asset=asset, at=at, timeframe="1h", period=14)
            parts.append(f"RSI(14,1h)={rsi_1h:.2f}")
        except Exception as e:
            parts.append("RSI(14,1h)=NA")

        try:
            cross = self.ind.get_sma_cross(asset=asset, at=at, timeframe="1h", fast_period=50, slow_period=200)
            parts.append(f"SMA50/200(1h)={cross.fast:.2f}/{cross.slow:.2f},{cross.crossed or 'no-cross'}")
        except Exception:
            parts.append("SMA50/200(1h)=NA")

        text = "; ".join(parts)
        return IndicatorSnapshot(text=text)

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")