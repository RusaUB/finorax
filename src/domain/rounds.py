from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from src.utils.time import snap_to_interval
from typing import List


@dataclass(frozen=True)
class Round:
    key: str
    window_start: datetime
    window_end: datetime

    def snapped(
        self,
        timeframe: str = "1h",
        start_mode: Literal["nearest", "floor", "ceil"] = "floor",
        end_mode: Literal["nearest", "floor", "ceil"] = "floor",
    ) -> "Round":
        s = snap_to_interval(self.window_start, freq=timeframe, mode=start_mode)
        e = snap_to_interval(self.window_end, freq=timeframe, mode=end_mode)
        return Round(key=self.key, window_start=s, window_end=e)


@dataclass(frozen=True)
class RoundAgentScore:
    agent_id: str
    observation_id: str
    score: float


@dataclass(frozen=True)
class RoundEvaluation:
    round: Round
    agent_scores: List[RoundAgentScore] = field(default_factory=list)