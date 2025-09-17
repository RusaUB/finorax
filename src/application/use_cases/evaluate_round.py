from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from src.domain.rounds import Round, RoundAgentScore, RoundEvaluation
from src.repositories.observations import ObservationRepository
from src.application.ports import IndicatorServicePort
from src.domain.assets import Asset
from src.utils.time import snap_to_interval
from src.repositories.rounds import RoundRepository


class EvaluateRound:
    def __init__(self, observations: ObservationRepository, indicators: IndicatorServicePort, rounds: RoundRepository | None = None) -> None:
        self.observations = observations
        self.indicators = indicators
        self.rounds = rounds

    def run(self, round: Round, quote: str = "USDT", timeframe: str = "1h") -> RoundEvaluation:
        self._require_utc(round.window_start, "round.window_start")
        self._require_utc(round.window_end, "round.window_end")
        snapped_start = snap_to_interval(round.window_start, freq=timeframe, mode="floor")
        snapped_end = snap_to_interval(round.window_end, freq=timeframe, mode="floor")
        if snapped_end <= snapped_start:
            raise ValueError("Snapped window_end must be greater than window_start for given timeframe")
        now_utc = datetime.now(timezone.utc)
        if snapped_end > now_utc:
            raise ValueError("round window_end cannot be in the future")

        obs = self.observations.list_in_window(snapped_start, snapped_end)
        by_agent_scores: Dict[str, float] = defaultdict(float)

        asset_cache: Dict[str, float] = {}
        failed_assets: set[str] = set()

        for o in obs:
            sym = (o.asset_symbol or "").strip().upper()
            if not sym:
                continue
            if o.zi_score is None:
                continue

            if sym in failed_assets:
                continue
            if sym not in asset_cache:
                try:
                    pc = self.indicators.get_price_change(
                        asset=Asset(symbol=sym),
                        start=snapped_start,
                        end=snapped_end,
                        timeframe=timeframe,
                        market=None,
                        quote=quote,
                    )
                    asset_cache[sym] = float(pc.pct_change)
                except Exception:
                    failed_assets.add(sym)
                    continue

            contribution = asset_cache[sym] * int(o.zi_score)
            by_agent_scores[o.agent_id] += contribution

        agent_scores: List[RoundAgentScore] = []
        obs_count: Dict[str, int] = defaultdict(int)
        for o in obs:
            if (o.asset_symbol or "").strip() and o.zi_score is not None:
                obs_count[o.agent_id] += 1

        for agent_id, score in by_agent_scores.items():
            agent_scores.append(RoundAgentScore(agent_id=agent_id, score=float(score), observations_count=obs_count.get(agent_id, 0)))

        agent_scores.sort(key=lambda s: s.score, reverse=True)
        evaluation = RoundEvaluation(round=round, agent_scores=agent_scores)

        if self.rounds is not None:
            try:
                self.rounds.save_evaluation(evaluation)
            except Exception:
                pass

        return evaluation

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
