from datetime import datetime, timezone
from typing import Any, Dict, List
import logging

from supabase import Client

from src.domain.rounds import  RoundEvaluation
from src.repositories.rounds import RoundRepository, SaveRoundResult


class SupabaseRoundRepository(RoundRepository):
    def __init__(self, sb_client: Client, rounds_table: str = "rounds", scores_table: str = "round_scores") -> None:
        self.sb = sb_client
        self.rounds_table = rounds_table
        self.scores_table = scores_table
        self._log = logging.getLogger(__name__)

    def save_evaluation(self, evaluation: RoundEvaluation) -> SaveRoundResult:
        rnd = evaluation.round
        is_insert = True
        try:
            exists = self.sb.table(self.rounds_table).select("key").eq("key", rnd.key).limit(1).execute()
            is_insert = not bool(exists.data)
        except Exception as e:
            self._log.warning("RoundsRepo: round existence check failed", extra={"key": rnd.key, "error": str(e)})

        self._log.info("RoundsRepo: saving round", extra={"key": rnd.key, "window_start": rnd.window_start.isoformat(), "window_end": rnd.window_end.isoformat(), "new": is_insert})
        payload = {
            "key": rnd.key,
            "window_start": rnd.window_start.isoformat(),
            "window_end": rnd.window_end.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if is_insert:
                self.sb.table(self.rounds_table).insert(payload).execute()
            else:
                self.sb.table(self.rounds_table).update(payload).eq("key", rnd.key).execute()
        except Exception as e:
            self._log.warning("RoundsRepo: round save failed", extra={"key": rnd.key, "error": str(e)})

        rows: List[Dict[str, Any]] = [
            {
                "round_key": rnd.key,
                "agent_id": s.agent_id,
                "observation_id": s.observation_id,
                "score": float(s.score),
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

        observation_ids = [r["observation_id"] for r in rows]
        existing = None
        existing_keys: set[tuple[str, str]] = set()
        try:
            existing = (
                self.sb.table(self.scores_table)
                .select("round_key, observation_id")
                .eq("round_key", rnd.key)
                .in_("observation_id", observation_ids)
                .execute()
            )
            existing_keys = {(r.get("round_key"), str(r.get("observation_id"))) for r in (existing.data or [])}
        except Exception as e:
            self._log.warning("RoundsRepo: fetch existing scores failed", extra={"round_key": rnd.key, "error": str(e)})
        key_of = lambda rr: (rr["round_key"], str(rr["observation_id"]))  # noqa: E731
        inserted_rows = [r for r in rows if key_of(r) not in existing_keys]
        updated_rows = [r for r in rows if key_of(r) in existing_keys]

        try:
            if inserted_rows:
                self.sb.table(self.scores_table).insert(inserted_rows).execute()
            if updated_rows:
                # Upsert by (round_key, observation_id)
                self.sb.table(self.scores_table).upsert(updated_rows, on_conflict="round_key,observation_id").execute()
        except Exception as e:
            self._log.warning("RoundsRepo: saving scores failed", extra={"round_key": rnd.key, "error": str(e)})
        self._log.info("RoundsRepo: saved scores", extra={"round_key": rnd.key, "inserted": len(inserted_rows), "updated": len(updated_rows), "total": len(rows)})

        return SaveRoundResult(
            inserted_round=1 if is_insert else 0,
            updated_round=0 if is_insert else 1,
            inserted_scores=len(inserted_rows),
            updated_scores=len(updated_rows),
            total_scores=len(rows),
        )

    def existing_round_keys(self, keys: List[str]) -> set[str]:
        if not keys:
            return set()
        res = (
            self.sb.table(self.scores_table)
            .select("round_key")
            .in_("round_key", keys)
            .execute()
        )
        rows = res.data or []
        return {r.get("round_key") for r in rows if r.get("round_key")}