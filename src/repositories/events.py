from typing import Protocol, List
from dataclasses import dataclass
from src.domain.events import Event
from datetime import datetime

class EventRepository(Protocol):
    def get_events_by_categories(
        self,
        categories: List[str],
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        limit: int | None = None,
    ) -> List[Event]: ...
    
    def upsert_many(
            self, 
            events: List["Event"]
    ) -> "UpsertResult": ...

@dataclass
class UpsertResult:
    inserted: int
    updated: int
    events: List[Event]