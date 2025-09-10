from typing import Protocol, Iterable, List
from dataclasses import dataclass
from datetime import datetime
from dataclasses import field

@dataclass
class NewsItemDTO:
    external_id: str
    external_url: str
    published_at: datetime
    title: str
    content: str
    source: str
    categories: List[str] = field(default_factory=list)

class NewsFeedPort(Protocol):
    def fetch(self, limit: int = 10, categories: List[str] = [], until: datetime | None = None) -> Iterable[NewsItemDTO]: ...