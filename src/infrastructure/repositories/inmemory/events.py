from typing import Dict, Tuple, List, Optional
from src.repositories.events import EventRepository, UpsertResult
from src.domain.events import Event


class InMemoryEventRepository(EventRepository):
    def __init__(self) -> None:
        self._by_id: Dict[str, Event] = {}
        self._by_src_ext: Dict[Tuple[str, str], str] = {}

    def upsert_many(self, events: List[Event]) -> UpsertResult:
        inserted = 0
        updated = 0
        out: List[Event] = []

        for ev in events:
            source: Optional[str] = getattr(ev, "source", None)
            external_id: Optional[str] = getattr(ev, "external_id", None)

            if (external_id is None) and hasattr(ev, "event_id"):
                eid = getattr(ev, "event_id")
                if isinstance(eid, str) and ":" in eid:
                    left, right = eid.split(":", 1)
                    if source is None:
                        source = left
                    external_id = right

            key = (source, external_id) if (source and external_id) else None

            if key and key in self._by_src_ext:
                existing_event_id = self._by_src_ext[key]
                self._by_id[existing_event_id] = ev
                updated += 1
            elif key:
                self._by_src_ext[key] = ev.event_id
                self._by_id[ev.event_id] = ev
                inserted += 1
            else:
                if ev.event_id in self._by_id:
                    self._by_id[ev.event_id] = ev
                    updated += 1
                else:
                    self._by_id[ev.event_id] = ev
                    inserted += 1

            out.append(ev)

        return UpsertResult(inserted=inserted, updated=updated, events=out)

    def all(self) -> List[Event]:
        return list(self._by_id.values())

    def get(self, event_id: str) -> Optional[Event]:
        return self._by_id.get(event_id)
    
    def count(self) -> int:
        return len(self._by_id)