from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass(frozen=True)
class Event:
    event_id: str
    occurred_at: datetime
    title: str
    content: str

    @staticmethod
    def from_dto(external_id: str, published_at: datetime, title: str, content: str, source: str)->"Event":
        if published_at.tzinfo is None or published_at.utcoffset() != timedelta(0):
            raise ValueError("occurred_at must be UTC tz-aware")
        t = (title or "").strip()
        if not t:
            raise ValueError("title is required")
        return Event(
            event_id=f"{source}:{external_id}",
            occurred_at=published_at,
            title=t,
            content=(content or "").strip(),
        )