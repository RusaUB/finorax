from typing import Protocol, Iterable, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class NewsItemDTO:
    external_id: str
    external_url: str
    published_on: datetime
    title: str
    content: str

class NewsFeedPort(Protocol):
    def fetch(self, categories: List[str] = [], until: datetime | None = None) -> Iterable[NewsItemDTO]: ...