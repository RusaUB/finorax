from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Observation:
    agent_id: str
    event_id: str
    asset_symbol: Optional[str] = None
    factor: str = ""
    zi_score: Optional[int] = None