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

        # Compose reusable UCs
        self._ingest = IngestEvents(feed=self.feed, events=self.events, assets=self.assets)
        self._gen_obs = GenerateObservationsForActiveAgents(
            agents=self.agents,
            observations=self.observations,
            factorizer=self.factorizer,
            indicators=IndicatorSnapshotBuilder(self.indicators),
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

            # 1) Ingest events up to round end
            self._log.info("Backfill: ingesting events", extra={"round_key": rnd.key, "until": rnd.window_end.isoformat()})
            try:
                self._ingest.run(limit=ingest_limit, categories=None, until=rnd.window_end)
            except Exception as e:
                self._log.warning("Backfill: ingest failed", extra={"round_key": rnd.key, "error": str(e)}, exc_info=True)

            # 2) Generate observations in the window
            self._log.info("Backfill: generating observations", extra={"round_key": rnd.key})
            try:
                self._gen_obs.run(window_start=rnd.window_start, window_end=rnd.window_end, per_agent_limit=None)
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
