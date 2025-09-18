from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from src.utils.time import snap_to_interval
from src.domain.rounds import Round
from src.application.use_cases.ingest_events import IngestEvents
from src.application.use_cases.generate_observations_for_active_agents import (
    GenerateObservationsForActiveAgents,
)
from src.application.use_cases.evaluate_round import EvaluateRound
from src.application.ports import NewsFeedPort, EventFactorizerPort
from src.repositories.events import EventRepository
from src.repositories.assets import AssetRepository
from src.repositories.agents import AgentRepository
from src.repositories.observations import ObservationRepository
from src.repositories.rounds import RoundRepository
from src.application.services.indicator_snapshot import IndicatorSnapshotBuilder
from src.application.ports import IndicatorServicePort


def _parse_freq_seconds(freq: str) -> int:
    m = re.fullmatch(r"\s*(\d+)\s*([mhdMHD])\s*", freq)
    if not m:
        raise ValueError("freq must look like '30m', '1h', or '1d'")
    n, u = m.groups()
    units = {"m": 60, "h": 3600, "d": 86400}
    return int(n) * units[u.lower()]


@dataclass
class BackfillResult:
    requested: int
    existing: int
    processed: int
    skipped: int


class BackfillRecentRounds:
    def __init__(
        self,
        *,
        feed: NewsFeedPort,
        events: EventRepository,
        assets: AssetRepository,
        agents: AgentRepository,
        observations: ObservationRepository,
        factorizer: EventFactorizerPort,
        indicators: IndicatorServicePort,
        rounds: RoundRepository,
        gen_per_agent_limit: int | None = None,
        gen_max_tokens: int = 256,
        ingest_categories: list[str] | None = None,
        ind_timeframe: str = "1h",
        ind_rsi_period: int = 14,
        ind_sma_fast: int = 50,
        ind_sma_slow: int = 200,
        min_events_per_round: int | None = None,
    ) -> None:
        self.feed = feed
        self.events = events
        self.assets = assets
        self.agents = agents
        self.observations = observations
        self.factorizer = factorizer
        self.indicators = indicators
        self.rounds = rounds
        self._log = logging.getLogger(__name__)

        # Store generation/ingestion tuning
        self._gen_per_agent_limit = gen_per_agent_limit
        self._gen_max_tokens = gen_max_tokens
        self._ingest_categories = ingest_categories or []
        self._min_events_per_round = min_events_per_round

        # Compose reusable UCs
        self._ingest = IngestEvents(feed=self.feed, events=self.events, assets=self.assets)
        self._gen_obs = GenerateObservationsForActiveAgents(
            agents=self.agents,
            observations=self.observations,
            factorizer=self.factorizer,
            indicators=IndicatorSnapshotBuilder(
                self.indicators,
                timeframe=ind_timeframe,
                rsi_period=ind_rsi_period,
                sma_fast=ind_sma_fast,
                sma_slow=ind_sma_slow,
            ),
        )
        self._eval = EvaluateRound(observations=self.observations, indicators=self.indicators, rounds=self.rounds)

    def run(
        self,
        *,
        n: int = 10,
        timeframe: str = "1h",
        now: Optional[datetime] = None,
        ingest_limit: int = 500,
        quote: str = "USDT",
    ) -> BackfillResult:
        if n <= 0:
            return BackfillResult(requested=0, existing=0, processed=0, skipped=0)

        now = now or datetime.now(timezone.utc)
        end_anchor = snap_to_interval(now, freq=timeframe, mode="floor")
        step_sec = _parse_freq_seconds(timeframe)
        step = timedelta(seconds=step_sec)

        # Build last N windows [start, end]
        windows: List[Round] = []
        keys: List[str] = []
        for i in range(n):
            end = end_anchor - i * step
            start = end - step
            rnd = Round(key=f"round-{start.strftime('%Y%m%d%H%M')}-{end.strftime('%Y%m%d%H%M')}", window_start=start, window_end=end)
            windows.append(rnd)
            keys.append(rnd.key)

        have = self.rounds.existing_round_keys(keys)
        self._log.info("Backfill: existing rounds", extra={"requested": n, "existing": len(have)})

        processed = 0
        skipped = 0
        for rnd in windows:
            if rnd.key in have:
                skipped += 1
                continue

            # 1) Decide whether to ingest based on existing events in the round window
            try:
                existing_events = self.events.count_in_window(rnd.window_start, rnd.window_end, with_asset_only=True)
            except Exception:
                existing_events = 0
            self._log.info(
                "Backfill: existing events in window",
                extra={"round_key": rnd.key, "count": existing_events},
            )

            should_ingest = True
            if isinstance(self._min_events_per_round, int) and self._min_events_per_round > 0:
                should_ingest = existing_events < self._min_events_per_round

            if should_ingest:
                self._log.info(
                    "Backfill: ingesting events",
                    extra={
                        "round_key": rnd.key,
                        "until": rnd.window_end.isoformat(),
                        "current_count": existing_events,
                        "target": self._min_events_per_round,
                    },
                )
                try:
                    ingest_res = self._ingest.run(
                        limit=ingest_limit,
                        categories=(self._ingest_categories or None),
                        until=rnd.window_end,
                    )
                    self._log.info(
                        "Backfill: ingest result",
                        extra={"inserted": ingest_res.inserted, "updated": ingest_res.updated},
                    )
                    # Re-check events after ingest to account for upsert no-ops
                    try:
                        post_events = self.events.count_in_window(rnd.window_start, rnd.window_end, with_asset_only=True)
                    except Exception:
                        post_events = existing_events
                    self._log.info(
                        "Backfill: events after ingest",
                        extra={"round_key": rnd.key, "count": post_events},
                    )
                except Exception as e:
                    self._log.warning(
                        "Backfill: ingest failed", extra={"round_key": rnd.key, "error": str(e)}, exc_info=True
                    )
            else:
                self._log.info(
                    "Backfill: skip ingest (enough events)",
                    extra={"round_key": rnd.key, "current_count": existing_events, "target": self._min_events_per_round},
                )

            # 2) Generate observations in the window
            self._log.info("Backfill: generating observations", extra={"round_key": rnd.key})
            try:
                self._gen_obs.run(
                    window_start=rnd.window_start,
                    window_end=rnd.window_end,
                    per_agent_limit=self._gen_per_agent_limit,
                    max_tokens=self._gen_max_tokens,
                )
            except Exception as e:
                self._log.warning("Backfill: generate observations failed", extra={"round_key": rnd.key, "error": str(e)}, exc_info=True)

            # 3) Evaluate and save round
            self._log.info("Backfill: evaluating round", extra={"round_key": rnd.key})
            try:
                self._eval.run(round=rnd, quote=quote, timeframe=timeframe)
            except Exception as e:
                self._log.warning("Backfill: evaluate round failed", extra={"round_key": rnd.key, "error": str(e)}, exc_info=True)
            processed += 1

        return BackfillResult(requested=n, existing=len(have), processed=processed, skipped=skipped)
