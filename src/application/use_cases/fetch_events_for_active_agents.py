from dataclasses import dataclass
from typing import List
from datetime import datetime, timedelta

from src.repositories.agents import AgentRepository
from src.domain.events import Event


@dataclass
class AgentEvents:
    agent_id: str
    events: List[Event]


@dataclass
class FetchActiveAgentsEventsResult:
    total_agents: int
    results: List[AgentEvents]

    @property
    def total_events(self) -> int:
        return sum(len(r.events) for r in self.results)


class FetchEventsForActiveAgents:
    def __init__(self, agents: AgentRepository) -> None:
        self.agents = agents

    def run(
        self,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        per_agent_limit: int | None = None,
    ) -> FetchActiveAgentsEventsResult:
        if window_start is not None:
            self._require_utc(window_start, "window_start")
        if window_end is not None:
            self._require_utc(window_end, "window_end")
        if window_start is not None and window_end is not None and window_start > window_end:
            raise ValueError("window_start must be <= window_end")

        active_agents = self.agents.list_active()
        results: List[AgentEvents] = []

        for a in active_agents:
            evs = self.agents.get_agent_events(
                agent_id=a.agent_id,
                window_start=window_start,
                window_end=window_end,
                limit=per_agent_limit,
            )
            results.append(AgentEvents(agent_id=a.agent_id, events=evs))

        return FetchActiveAgentsEventsResult(total_agents=len(active_agents), results=results)

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")