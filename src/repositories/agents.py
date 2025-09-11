from typing import Protocol, Optional, List
from datetime import datetime
from src.domain.agents import Agent, CoverageProfile
from src.domain.events import Event

class AgentRepository(Protocol):
    def get(self, agent_id: str) -> Optional[Agent]: ...
    def list_active(self) -> List[Agent]: ...
    def get_agent_profile(self, agent_id: str) -> Optional[CoverageProfile]: ...
    def get_agent_events(
        self,
        agent_id: str,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        limit: int | None = None,
    ) -> List[Event]: ...

class CoverageProfileRepository(Protocol):
    def get(self, profile_key: str) -> Optional[CoverageProfile]: ...