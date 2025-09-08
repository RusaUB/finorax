from typing import List
from datetime import datetime
from src.domain.events import Event
from src.application.ports import NewsFeedPort
from src.repositories.events import EventRepository, UpsertResult

class IngestEvents:
    def __init__(self, feed: NewsFeedPort, events: EventRepository):
        self.feed = feed
        self.events = events
    
    def run(self,limit:int = 10, categories: List[str] | None = None, until: datetime | None = None) -> UpsertResult:
        items = self.feed.fetch(limit=limit, categories=categories or [], until=until)
        to_upsert: List[Event] = []
        for dto in items:
            to_upsert.append(Event.from_dto(
                external_id=dto.external_id,
                source=dto.source,
                published_at=dto.published_at,
                title=dto.title,
                content=dto.content,
            ))
        return self.events.upsert_many(events=to_upsert)