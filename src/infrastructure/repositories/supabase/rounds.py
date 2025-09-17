from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from supabase import Client

from src.domain.rounds import  RoundEvaluation
from src.repositories.rounds import RoundRepository, SaveRoundResult


class SupabaseRoundRepository(RoundRepository):
    def __init__(self, sb_client: Client, rounds_table: str = "rounds", scores_table: str = "round_scores") -> None:
        self.sb = sb_client
        self.rounds_table = rounds_table
        self.scores_table = scores_table

    def save_evaluation(self, evaluation: RoundEvaluation) -> SaveRoundResult:
        rnd = evaluation.round
        exists = self.sb.table(self.rounds_table).select("key").eq("key", rnd.key).limit(1).execute()
        is_insert = not bool(exists.data)

        self.sb.table(self.rounds_table).upsert(
            {
                "key": rnd.key,
                "window_start": rnd.window_start.isoformat(),
                "window_end": rnd.window_end.isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="key",
        ).execute()

        rows: List[Dict[str, Any]] = [
            {
                "round_key": rnd.key,
                "agent_id": s.agent_id,
                "score": float(s.score),
                "observations_count": int(s.observations_count),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            for s in (evaluation.agent_scores or [])
        ]

        if not rows:
            return SaveRoundResult(
                inserted_round=1 if is_insert else 0,
                updated_round=0 if is_insert else 1,
                inserted_scores=0,
                updated_scores=0,
                total_scores=0,
            )

        agent_ids = [r["agent_id"] for r in rows]
        existing = (
            self.sb.table(self.scores_table)
            .select("round_key, agent_id")
            .eq("round_key", rnd.key)
            .in_("agent_id", agent_ids)
            .execute()
        )
        existing_keys = {(r.get("round_key"), r.get("agent_id")) for r in (existing.data or [])}
        key_of = lambda rr: (rr["round_key"], rr["agent_id"])  # noqa: E731
        inserted_rows = [r for r in rows if key_of(r) not in existing_keys]
        updated_rows = [r for r in rows if key_of(r) in existing_keys]

        if inserted_rows:
            self.sb.table(self.scores_table).insert(inserted_rows).execute()
        if updated_rows:
            self.sb.table(self.scores_table).upsert(updated_rows, on_conflict="round_key,agent_id").execute()

        return SaveRoundResult(
            inserted_round=1 if is_insert else 0,
            updated_round=0 if is_insert else 1,
            inserted_scores=len(inserted_rows),
            updated_scores=len(updated_rows),
            total_scores=len(rows),
        )