from typing import List
from datetime import datetime
from src.domain.events import Event
from src.application.ports import NewsFeedPort
from src.repositories.events import EventRepository, UpsertResult
from src.repositories.assets import AssetRepository
from src.application.services.asset_extractor import AssetExtractor
from src.domain.assets import Asset

class IngestEvents:
    def __init__(self, feed: NewsFeedPort, events: EventRepository, assets: AssetRepository):
        self.feed = feed
        self.events = events
        self._asset_extractor = AssetExtractor.from_repository(assets)
    
    def run(self, limit: int = 10, categories: List[str] | None = None, until: datetime | None = None) -> UpsertResult:
        items = self.feed.fetch(limit=limit, categories=categories or [], until=until)
        to_upsert: List[Event] = []

        for dto in items:
            # Build a validated base event (ensures UTC and trimmed fields)
            base_event = Event.from_dto(
                external_id=dto.external_id,
                source=dto.source,
                published_at=dto.published_at,
                title=dto.title,
                categories=dto.categories,
                content=dto.content,
            )

            symbols = sorted(self._asset_extractor.extract_symbols(base_event))

            if not symbols:
                # No assets detected: keep original event as-is
                to_upsert.append(base_event)
                continue

            # Duplicate the event per asset with ID like "source:external_id:ASSET"
            # Also ensure the asset symbol is present in categories (uppercase)
            base_cats_upper = [(c or "").strip().upper() for c in (base_event.categories or []) if (c or "").strip()]
            for sym in symbols:
                sym_up = (sym or "").strip().upper()
                cats = list(base_cats_upper)
                if sym_up and sym_up not in cats:
                    cats.append(sym_up)

                to_upsert.append(
                    Event(
                        event_id=f"{dto.source}:{dto.external_id}:{sym_up}",
                        occurred_at=base_event.occurred_at,
                        title=base_event.title,
                        content=base_event.content,
                        categories=cats,
                        asset=Asset(symbol=sym_up),
                    )
                )

        return self.events.upsert_many(events=to_upsert)
