from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, List

from src.domain.observations import Observation


class ObservationRepository(Protocol):
    def upsert_many(self, observations: List[Observation]) -> "ObservationUpsertResult": ...
    def list_in_window(self, window_start: datetime, window_end: datetime) -> List[Observation]: ...


@dataclass
class ObservationUpsertResult:
    inserted: int
    updated: int
    observations: List[Observation]