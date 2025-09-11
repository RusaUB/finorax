from typing import List, Dict, Any
from src.domain.events import Event
from src.repositories.events import EventRepository, UpsertResult
from supabase import Client
from datetime import datetime, timezone, timedelta

class SupabaseEventRepository(EventRepository):
    def __init__(self, sb_client: Client, table: str = "events"):
        self.sb = sb_client
        self.table = table

    def get_events_by_categories(
        self,
        categories: List[str],
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        limit: int | None = None,
    ) -> List[Event]:
        cats = [self._norm_cat(c) for c in (categories or []) if self._norm_cat(c)]
        if not cats:
            return []

        if window_start is not None:
            self._require_utc(window_start, "window_start")
        if window_end is not None:
            self._require_utc(window_end, "window_end")
        if window_start is not None and window_end is not None and window_start > window_end:
            raise ValueError("window_start must be <= window_end")

        q = self.sb.table(self.table).select("event_id, occurred_at, title, content, categories")
        if window_start is not None:
            q = q.gte("occurred_at", window_start.isoformat())
        if window_end is not None:
            q = q.lt("occurred_at", window_end.isoformat())  # [start, end)

        or_clauses = [f'categories.cs.["{c}"]' for c in cats]
        q = q.or_(",".join(or_clauses))

        q = q.order("occurred_at", desc=False)
        if limit is not None:
            q = q.limit(int(limit))

        res = q.execute()
        rows = res.data or []
        return [self._event_from_row(r) for r in rows]

    def upsert_many(self, events: List[Event]) -> UpsertResult:
        if not events:
            return UpsertResult(inserted=0, updated=0)

        rows = [self._row_from_event(e) for e in events]
        ids  = [r["event_id"] for r in rows]

        existing = self.sb.table(self.table).select("event_id").in_("event_id", ids).execute()
        existing_ids = {row["event_id"] for row in (existing.data or [])}

        new_rows     = [r for r in rows if r["event_id"] not in existing_ids]
        update_rows  = [r for r in rows if r["event_id"] in existing_ids]

        inserted = len(new_rows)
        updated  = len(update_rows)

        if new_rows:
            self.sb.table(self.table).insert(new_rows).execute()

        if update_rows:
            self.sb.table(self.table).upsert(update_rows, on_conflict="event_id").execute()

        return UpsertResult(inserted=inserted, updated=updated, events=events)

    def _norm_cat(self, s: str) -> str:
        return (s or "").strip().upper()

    def _event_from_row(self, row: Dict[str, Any]) -> Event:
        raw_ts = row.get("occurred_at")
        ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")) if isinstance(raw_ts, str) else raw_ts
        cats = row.get("categories") or []
        if isinstance(cats, str):
            cats = [c.strip() for c in cats.split(",") if c.strip()]
        return Event(
            event_id=row["event_id"],
            occurred_at=ts,
            title=row.get("title") or "",
            content=row.get("content") or "",
            categories=cats,
        )

    def _row_from_event(self, e: Event) -> Dict[str, Any]:
        source, external_id = e.event_id.split(":", 1)
        return {
            "event_id":     e.event_id,
            "external_id":  external_id,
            "source":       source,
            "occurred_at":  e.occurred_at.isoformat(),
            "title":        e.title,
            "content":      e.content,
            "categories":   e.categories,
            "updated_at":   datetime.now(timezone.utc).isoformat()
        }

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")