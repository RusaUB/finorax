from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
from supabase import Client
from src.domain.agents import Agent, CoverageProfile
from src.domain.events import Event
from src.domain.assets import Asset
from src.repositories.agents import AgentRepository

class SupabaseAgentRepository(AgentRepository):
    def __init__(self, sb_client: Client, agent_table: str = "agents", profile_table: str = "coverage_profiles", events_table: str = "events"):
        self.sb = sb_client
        self.agent_table = agent_table
        self.profile_table = profile_table
        self.events_table = events_table
        self._log = logging.getLogger(__name__)

    def get(self, agent_id: str) -> Optional[Agent]:
        res = self.sb.table(self.agent_table)\
            .select("agent_id, name, coverage_profile_key, is_active")\
            .eq("agent_id", agent_id).execute()
        rows = res.data or []
        if not rows:
            return None
        a = rows[0]
        profile = self._fetch_profile(a["coverage_profile_key"])
        return self._agent_from_row(a, profile)

    def list_active(self) -> List[Agent]:
        res = self.sb.table(self.agent_table)\
            .select("agent_id, name, coverage_profile_key, is_active")\
            .eq("is_active", True).execute()
        rows = res.data or []
        keys = {r["coverage_profile_key"] for r in rows if r.get("coverage_profile_key")}
        profiles = self._fetch_profiles(list(keys))
        out: List[Agent] = []
        for r in rows:
            prof = profiles.get(r.get("coverage_profile_key"))
            out.append(self._agent_from_row(r, prof))
        return out

    def get_agent_profile(self, agent_id: str) -> Optional[CoverageProfile]:
        res = self.sb.table(self.agent_table).select("coverage_profile_key").eq("agent_id", agent_id).execute()
        rows = res.data or []
        if not rows:
            return None
        key = rows[0].get("coverage_profile_key")
        if not key:
            return None
        return self._fetch_profile(key)

    def get_agent_role(self, agent_id: str) -> Optional[str]:
        res = self.sb.table(self.agent_table).select("coverage_profile_key").eq("agent_id", agent_id).limit(1).execute()
        rows = res.data or []
        if not rows:
            return None
        key = rows[0].get("coverage_profile_key")
        if not key:
            return None
        prof = self._fetch_profile(key)
        return (prof.role or "").strip() if prof else None

    def get_agent_categories(self, agent_id: str) -> List[str]:
        """Return the agent's coverage categories (normalized).
        Note: categories are stored with the coverage profile in the DB,
        but are not part of the domain CoverageProfile anymore.
        """
        res = self.sb.table(self.agent_table).select("coverage_profile_key").eq("agent_id", agent_id).execute()
        rows = res.data or []
        if not rows:
            return []
        key = rows[0].get("coverage_profile_key")
        if not key:
            return []
        prof_row = self.sb.table(self.profile_table).select("categories").eq("profile_key", key).limit(1).execute()
        prof_rows = prof_row.data or []
        if not prof_rows:
            return []
        cats = prof_rows[0].get("categories") or []
        if isinstance(cats, str):
            cats = [c.strip() for c in cats.split(",") if c.strip()]
        return [self._norm_cat(c) for c in cats if self._norm_cat(c)]

    def get_agent_events(
        self,
        agent_id: str,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        limit: int | None = None,
    ) -> List[Event]:
        if window_start is not None:
            self._require_utc(window_start, "window_start")
        if window_end is not None:
            self._require_utc(window_end, "window_end")
        if window_start is not None and window_end is not None and window_start > window_end:
            raise ValueError("window_start must be <= window_end")

        q = self.sb.table(self.events_table).select("event_id, occurred_at, title, content, categories, asset_symbol")
        if window_start is not None:
            q = q.gte("occurred_at", window_start.isoformat())
        if window_end is not None:
            q = q.lt("occurred_at", window_end.isoformat())

        # Only include events that have an associated asset
        try:
            q = q.not_.is_("asset_symbol", "null")
        except Exception:
            q = q.neq("asset_symbol", None)

        q = q.order("occurred_at", desc=False)
        if limit is not None:
            q = q.limit(int(limit))

        self._log.info("AgentRepo: fetching agent events with assets", extra={"agent_id": agent_id, "window_start": window_start.isoformat() if window_start else None, "window_end": window_end.isoformat() if window_end else None, "limit": limit})
        res = q.execute()
        rows = (res.data or [])
        rows = [r for r in rows if (r.get("asset_symbol") or "").strip()]
        self._log.info("AgentRepo: fetched agent events", extra={"agent_id": agent_id, "count": len(rows)})
        return [self._event_from_row(r) for r in rows]

    def _fetch_profile(self, key: str) -> Optional[CoverageProfile]:
        res = self.sb.table(self.profile_table)\
            .select("profile_key, name, role")\
            .eq("profile_key", key).execute()
        rows = res.data or []
        if not rows:
            return None
        return self._profile_from_row(rows[0])

    def _fetch_profiles(self, keys: List[str]) -> Dict[str, CoverageProfile]:
        if not keys:
            return {}
        res = self.sb.table(self.profile_table)\
            .select("profile_key, name, role")\
            .in_("profile_key", keys).execute()
        rows = res.data or []
        out: Dict[str, CoverageProfile] = {}
        for r in rows:
            p = self._profile_from_row(r)
            out[p.profile_key] = p
        return out

    def _profile_from_row(self, row: Dict[str, Any]) -> CoverageProfile:
        return CoverageProfile(
            profile_key=row["profile_key"],
            name=row.get("name") or "",
            role=row.get("role") or "",
        )

    def _agent_from_row(self, row: Dict[str, Any], profile: CoverageProfile | None) -> Agent:
        return Agent(
            agent_id=row["agent_id"],
            name=row.get("name") or "",
            coverage_profile_key=(
                profile
                if profile is not None
                else CoverageProfile(
                    profile_key=row.get("coverage_profile_key") or "",
                    name="",
                    role="",
                )
            ),
            is_active=bool(row.get("is_active", True)),
        )

    def _event_from_row(self, row: Dict[str, Any]) -> Event:
        raw_ts = row.get("occurred_at")
        ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")) if isinstance(raw_ts, str) else raw_ts
        cats = row.get("categories") or []
        if isinstance(cats, str):
            cats = [c.strip() for c in cats.split(",") if c.strip()]
        sym = (row.get("asset_symbol") or "").strip().upper()
        return Event(
            event_id=row["event_id"],
            occurred_at=ts,
            title=row.get("title") or "",
            content=row.get("content") or "",
            categories=cats,
            asset=(Asset(symbol=sym) if sym else None),
        )

    def _norm_cat(self, s: str) -> str:
        return (s or "").strip().upper()

    def _require_utc(self, dt: datetime, name: str) -> None:
        if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
