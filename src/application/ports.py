from typing import Protocol, Iterable, List
from dataclasses import dataclass
from datetime import datetime
from dataclasses import field
from src.domain.events import Event
from src.domain.assets import Asset

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


# --- LLM factorization (DeepSeek via OpenAI-compatible async client) ---

@dataclass
class EventFactorDTO:
    factor: str
    zi_score: int
    confidence: int | None = None


class EventFactorizerPort(Protocol):
    def factorize(
        self,
        event: Event,
        max_tokens: int = 256,
        agent_role: str | None = None,
        indicators_context: str | None = None,
    ) -> EventFactorDTO: ...


@dataclass
class SMACrossDTO:
    fast: float
    slow: float
    prev_fast: float
    prev_slow: float
    crossed: str | None  

@dataclass
class PriceChangeDTO:
    start_ts: datetime
    end_ts: datetime
    start_price: float
    end_price: float
    abs_change: float
    pct_change: float

class IndicatorServicePort(Protocol):
    def get_rsi(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        period: int = 14,
        market: str | None = None,
        quote: str = "USDT",
    ) -> float: ...

    def get_sma(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        period: int = 14,
        market: str | None = None,
        quote: str = "USDT",
    ) -> float: ...

    def get_sma_cross(
        self,
        asset: Asset,
        at: datetime,
        timeframe: str = "1h",
        fast_period: int = 50,
        slow_period: int = 200,
        market: str | None = None,
        quote: str = "USDT",
    ) -> SMACrossDTO: ...

    def get_price_change(
        self,
        asset: Asset,
        start: datetime,
        end: datetime,
        timeframe: str = "1h",
        market: str | None = None,
        quote: str = "USDT",
    ) -> 'PriceChangeDTO': ...