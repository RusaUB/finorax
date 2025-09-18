from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import logging

from src.domain.observations import Observation
from src.repositories.agents import AgentRepository
from src.repositories.observations import ObservationRepository, ObservationUpsertResult
from src.application.ports import EventFactorizerPort
from src.application.services.indicator_snapshot import IndicatorSnapshotBuilder


@dataclass
class GenerateObservationsResult:
    total_agents: int
    total_events: int
    upserted: ObservationUpsertResult


class GenerateObservationsForActiveAgents:
    def __init__(
        self,
        agents: AgentRepository,
        observations: ObservationRepository,
        factorizer: EventFactorizerPort,
        indicators: Optional[IndicatorSnapshotBuilder] = None,
    ) -> None:
        self.agents = agents
        self.observations = observations
        self.factorizer = factorizer
        self.indicators = indicators
        self._log = logging.getLogger(__name__)

    def run(
        self,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        per_agent_limit: Optional[int] = None,
        max_tokens: int = 256,
    ) -> GenerateObservationsResult:
        if window_start is not None:
            self._require_utc(window_start, "window_start")
        if window_end is not None:
            self._require_utc(window_end, "window_end")
        if window_start is not None and window_end is not None and window_start > window_end:
            raise ValueError("window_start must be <= window_end")

        self._log.info("GenerateObs: listing active agents")
        active_agents = self.agents.list_active()
        self._log.info("GenerateObs: active agents fetched", extra={"count": len(active_agents)})
        observations: List[Observation] = []
        total_events = 0

        for a in active_agents:
            role = self.agents.get_agent_role(a.agent_id) or ""
            events = self.agents.get_agent_events(
                agent_id=a.agent_id,
                window_start=window_start,
                window_end=window_end,
                limit=per_agent_limit,
            )
            self._log.info("GenerateObs: events fetched for agent", extra={"agent_id": a.agent_id, "count": len(events)})
            # get_agent_events already returns only events with assets
            for e in events:
                total_events += 1
                indicators_context = None
                if self.indicators is not None and e.asset is not None:
                    try:
                        indicators_context = self.indicators.build(asset=e.asset, at=e.occurred_at).text
                    except Exception:
                        indicators_context = None

                res = self.factorizer.factorize(
                    event=e,
                    max_tokens=max_tokens,
                    agent_role=role,
                    indicators_context=indicators_context,
                )
                observations.append(
                    Observation(
                        agent_id=a.agent_id,
                        event_id=e.event_id,
                        asset_symbol=(e.asset.symbol if e.asset else None),
                        factor=res.factor or "",
                        zi_score=res.zi_score,
                        confidence=getattr(res, 'confidence', None),
                    )
                )

        self._log.info("GenerateObs: upserting observations", extra={"count": len(observations)})
        upserted = self.observations.upsert_many(observations)
        self._log.info("GenerateObs: upsert completed", extra={"inserted": upserted.inserted, "updated": upserted.updated})
        return GenerateObservationsResult(
            total_agents=len(active_agents),
            total_events=total_events,
            upserted=upserted,
        )

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
