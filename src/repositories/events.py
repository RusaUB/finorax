from typing import Protocol, List
from dataclasses import dataclass
from src.domain.events import Event

class EventRepository(Protocol):
    def upsert_many(self, events: List["Event"]) -> "UpsertResult": ...

@dataclass
class UpsertResult:
    inserted: int
    updated: int
    events: List[Event]