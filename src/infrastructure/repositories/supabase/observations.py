from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

from supabase import Client

from src.domain.observations import Observation
from src.repositories.observations import ObservationRepository, ObservationUpsertResult


class SupabaseObservationRepository(ObservationRepository):
    def __init__(self, sb_client: Client, table: str = "observations", events_table: str = "events") -> None:
        self.sb = sb_client
        self.table = table
        self.events_table = events_table
        self._log = logging.getLogger(__name__)

    def upsert_many(self, observations: List[Observation]) -> ObservationUpsertResult:
        if not observations:
            return ObservationUpsertResult(inserted=0, updated=0, observations=[])

        rows = [self._row_from_obs(o) for o in observations]

        agent_ids = list({r["agent_id"] for r in rows})
        event_ids = list({r["event_id"] for r in rows})

        existing = (
            self.sb
            .table(self.table)
            .select("agent_id, event_id, asset_symbol")
            .in_("agent_id", agent_ids)
            .in_("event_id", event_ids)
            .execute()
        )
        existing_keys: set[Tuple[str, str, Optional[str]]] = set()
        for r in (existing.data or []):
            existing_keys.add((r.get("agent_id"), r.get("event_id"), r.get("asset_symbol")))

        def key_of(row: Dict[str, Any]) -> Tuple[str, str, Optional[str]]:
            return (row["agent_id"], row["event_id"], row.get("asset_symbol"))

        new_rows = [r for r in rows if key_of(r) not in existing_keys]
        update_rows = [r for r in rows if key_of(r) in existing_keys]

        inserted = len(new_rows)
        updated = len(update_rows)

        if new_rows:
            self.sb.table(self.table).insert(new_rows).execute()
        if update_rows:
            self.sb.table(self.table).upsert(update_rows, on_conflict="agent_id,event_id,asset_symbol").execute()
        self._log.info("ObservationsRepo: upsert_many", extra={"inserted": inserted, "updated": updated})
        return ObservationUpsertResult(inserted=inserted, updated=updated, observations=observations)

    def _row_from_obs(self, o: Observation) -> Dict[str, Any]:
        return {
            "agent_id": o.agent_id,
            "event_id": o.event_id,
            "asset_symbol": o.asset_symbol,
            "factor": (o.factor or ""),
            "zi_score": o.zi_score,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def list_in_window(self, window_start: datetime, window_end: datetime) -> List[Observation]:
        if window_start.tzinfo is None or window_start.utcoffset() is None:
            raise ValueError("window_start must be timezone-aware UTC")
        if window_end.tzinfo is None or window_end.utcoffset() is None:
            raise ValueError("window_end must be timezone-aware UTC")
        if window_start > window_end:
            raise ValueError("window_start must be <= window_end")

        self._log.info("ObservationsRepo: listing observations by events window", extra={"start": window_start.isoformat(), "end": window_end.isoformat()})
        ev = (
            self.sb
            .table(self.events_table)
            .select("event_id")
            .gte("occurred_at", window_start.isoformat())
            .lt("occurred_at", window_end.isoformat())
        ).execute()
        event_ids = [r.get("event_id") for r in (ev.data or []) if r.get("event_id")]
        if not event_ids:
            self._log.info("ObservationsRepo: no events in window", extra={"start": window_start.isoformat(), "end": window_end.isoformat()})
            return []

        rows: List[Dict[str, Any]] = []
        id_field: str | None = None
        try:
            res = (
                self.sb
                .table(self.table)
                .select("observation_id, agent_id, event_id, asset_symbol, factor, zi_score, updated_at")
                .in_("event_id", event_ids)
            ).execute()
            rows = res.data or []
            id_field = "observation_id"
        except Exception:
            try:
                res = (
                    self.sb
                    .table(self.table)
                    .select("id, agent_id, event_id, asset_symbol, factor, zi_score, updated_at")
                    .in_("event_id", event_ids)
                ).execute()
                rows = res.data or []
                id_field = "id"
            except Exception:
                res = (
                    self.sb
                    .table(self.table)
                    .select("agent_id, event_id, asset_symbol, factor, zi_score, updated_at")
                    .in_("event_id", event_ids)
                ).execute()
                rows = res.data or []
                id_field = None
        self._log.info("ObservationsRepo: fetched observations", extra={"count": len(rows)})
        out: List[Observation] = []
        for r in rows:
            out.append(
                Observation(
                    id=(str(r.get(id_field)) if id_field and r.get(id_field) is not None else None),
                    agent_id=r.get("agent_id"),
                    event_id=r.get("event_id"),
                    asset_symbol=r.get("asset_symbol"),
                    factor=r.get("factor") or "",
                    zi_score=r.get("zi_score"),
                )
            )
        return out