from dataclasses import dataclass
from src.application.ports import NewsFeedPort, NewsItemDTO
from typing import Iterable, List
from datetime import datetime, timedelta, timezone

@dataclass(frozen=True)
class FakeFeed(NewsFeedPort):
    items: List[NewsItemDTO]
    def fetch(self, categories = [], limit = 10, until = None) -> Iterable[NewsItemDTO]:
        out = self.items
        if until is not None:
            out = [i for i in out if i.published_at < until]
        return out[:limit]
    
def make_fake_items(now: datetime | None = None) -> List[NewsItemDTO]:
    now = now or datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return [
        NewsItemDTO(
            external_id="51339571",
            external_url="https://coindesk.example/1001",
            published_at=now - timedelta(minutes=50),
            title="Buyers Appear to Control Market, DOGE May Test $0.219 Resistance Amid Falling Volume",
            content="DOGE price is trading at $0.2183, up 1.55% in the last 24 hours, with buyers controlling the market. Short-term outlook favors sideways trading between $0.2150 and $0.22 as volume declines",
            source="coindesk",
        ),
        NewsItemDTO(
            external_id="51340570",
            external_url="https://coindesk.example/1002",
            published_at=now - timedelta(minutes=20),
            title="Michael Saylor Says “Need More Orange Dots” — MicroStrategy Poised to Add More Bitcoin Ahead of Holdings Disclosure",
            content="Strategy founder Michael Saylor reiterated details about the Bitcoin Tracker, succinctly noting “Need more orange dots.” This communication aligns with Strategy’s established reporting",
            source="coindesk",
        ),
        NewsItemDTO(
            external_id="51390394",
            external_url="https://coindesk.example/1002",
            published_at=now - timedelta(minutes=10),
            title="Strategy Expands Bitcoin Holdings with Bold New Purchases",
            content="Strategy recently purchased 1,955 Bitcoin, boosting its holdings significantly. The company uses a diverse financial structure to support its Bitcoin acquisitions. Continue Reading: Strategy Expands Bitcoin Holdings with Bold New Purchases",
            source="coindesk",
        ),
    ]