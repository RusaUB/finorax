from dataclasses import dataclass
from typing import Protocol, List

from src.domain.observations import Observation


class ObservationRepository(Protocol):
    def upsert_many(self, observations: List[Observation]) -> "ObservationUpsertResult": ...


@dataclass
class ObservationUpsertResult:
    inserted: int
    updated: int
    observations: List[Observation]