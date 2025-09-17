from datetime import datetime, timedelta, timezone
from typing import Dict, List
import logging

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
        self._log = logging.getLogger(__name__)

    def run(self, round: Round, quote: str = "USDT", timeframe: str = "1h") -> RoundEvaluation:
        self._require_utc(round.window_start, "round.window_start")
        self._require_utc(round.window_end, "round.window_end")
        snapped_start = snap_to_interval(round.window_start, freq=timeframe, mode="floor")
        snapped_end = snap_to_interval(round.window_end, freq=timeframe, mode="floor")
        self._log.info("EvaluateRound: snapped window", extra={"start": snapped_start.isoformat(), "end": snapped_end.isoformat(), "timeframe": timeframe})
        if snapped_end <= snapped_start:
            raise ValueError("Snapped window_end must be greater than window_start for given timeframe")
        now_utc = datetime.now(timezone.utc)
        if snapped_end > now_utc:
            raise ValueError("round window_end cannot be in the future")

        obs = self.observations.list_in_window(snapped_start, snapped_end)
        self._log.info("EvaluateRound: observations fetched", extra={"count": len(obs)})

        asset_cache: Dict[str, float] = {}
        failed_assets: set[str] = set()

        # One row per observation: score = pct_price_change * zi_score
        agent_scores: List[RoundAgentScore] = []
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
                    self._log.debug("EvaluateRound: price change unavailable, skipping symbol", extra={"symbol": sym})
                    continue
            if not o.id:
                # Can't persist without a real observation_id (DB expects uuid)
                self._log.debug("EvaluateRound: missing observation_id, skipping", extra={"agent_id": o.agent_id, "event_id": o.event_id, "symbol": sym})
                continue
            score_val = asset_cache[sym] * int(o.zi_score)
            agent_scores.append(
                RoundAgentScore(
                    agent_id=o.agent_id,
                    observation_id=str(o.id),
                    score=float(score_val),
                )
            )

        agent_scores.sort(key=lambda s: s.score, reverse=True)
        evaluation = RoundEvaluation(round=round, agent_scores=agent_scores)

        if self.rounds is not None:
            try:
                self.rounds.save_evaluation(evaluation)
            except Exception as e:
                self._log.warning("EvaluateRound: saving round failed", extra={"error": str(e)})

        self._log.info("EvaluateRound: completed", extra={"agents_scored": len(agent_scores)})
        return evaluation

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
