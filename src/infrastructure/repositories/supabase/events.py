from typing import List, Dict, Any
from src.domain.events import Event
from src.repositories.events import EventRepository, UpsertResult
from supabase import Client
from datetime import datetime, timezone

class SupabaseEventRepository(EventRepository):
    def __init__(self, sb_client: Client, table: str = "events"):
        self.sb = sb_client
        self.table = table

    def upsert_many(self, events: List[Event]) -> UpsertResult:
        if not events:
            return UpsertResult(inserted=0, updated=0)

        rows = [self._row_from_event(e) for e in events]
        ids  = [r["event_id"] for r in rows]

        existing = self.sb.table(self.table) \
            .select("event_id") \
            .in_("event_id", ids) \
            .execute()
        existing_ids = {row["event_id"] for row in (existing.data or [])}

        new_rows     = [r for r in rows if r["event_id"] not in existing_ids]
        update_rows  = [r for r in rows if r["event_id"] in existing_ids]

        inserted = len(new_rows)
        updated  = len(update_rows)

        if new_rows:
            self.sb.table(self.table).insert(new_rows).execute()

        if update_rows:
            self.sb.table(self.table)\
                .upsert(update_rows, on_conflict="event_id")\
                .execute()

        return UpsertResult(inserted=inserted, updated=updated, events=new_rows)

    def _row_from_event(self, e: Event) -> Dict[str, Any]:
        source, external_id = e.event_id.split(":", 1) if ":" in e.event_id else (e.source, e.external_id)
        return {
            "event_id":     e.event_id, # PK / unique
            "external_id":  external_id,
            "source":       source,
            "occurred_at":  e.occurred_at.isoformat(),
            "title":        e.title,
            "content":      e.content,
            "categories":   e.categories,
            "updated_at":   datetime.now(timezone.utc).isoformat()
        }