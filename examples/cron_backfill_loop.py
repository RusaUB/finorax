import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

from supabase import Client, create_client

from src.infrastructure.fetchers.clients.coindesk import CoinDeskClient
from src.infrastructure.repositories.supabase.events import SupabaseEventRepository
from src.infrastructure.repositories.supabase.assets import SupabaseAssetRepository
from src.infrastructure.repositories.supabase.agents import SupabaseAgentRepository
from src.infrastructure.repositories.supabase.observations import SupabaseObservationRepository
from src.infrastructure.repositories.supabase.rounds import SupabaseRoundRepository
from src.infrastructure.indicators.ccxt_service import CcxtIndicatorService
from src.infrastructure.llm.deepseek import DeepseekClient
from src.application.use_cases.backfill_recent_rounds import BackfillRecentRounds
from src.application.use_cases.ingest_events import IngestEvents
from src.application.use_cases.generate_observations_for_active_agents import (
    GenerateObservationsForActiveAgents,
)
from src.application.services.indicator_snapshot import IndicatorSnapshotBuilder
from src.config.loader import load_base_config, conf_get, env_or_value
from src.utils.time import snap_to_interval


def make_supabase(conf: Dict[str, Any]) -> Client:
    url = env_or_value(conf_get(conf, "supabase.url_env"), conf_get(conf, "supabase.url"))
    key = env_or_value(conf_get(conf, "supabase.key_env"), conf_get(conf, "supabase.key"))
    if not url or not key:
        raise SystemExit("Supabase URL/KEY are required (configure in config/base.yaml or env)")
    return create_client(url, key)


def build_dependencies(conf: Dict[str, Any]):
    sb = make_supabase(conf)

    # Repositories
    events = SupabaseEventRepository(sb_client=sb)
    assets = SupabaseAssetRepository(sb_client=sb)
    agents = SupabaseAgentRepository(sb_client=sb)
    observations = SupabaseObservationRepository(sb_client=sb)
    rounds = SupabaseRoundRepository(sb_client=sb)

    # External services
    indicators = CcxtIndicatorService(exchange_id=conf_get(conf, "indicators.exchange_id", os.environ.get("CCXT_EXCHANGE", "binance")))
    api_key = env_or_value(conf_get(conf, "llm.api_key_env"), conf_get(conf, "llm.api_key"))
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is required (configure in config/base.yaml or env)")
    llm = DeepseekClient(
        model=conf_get(conf, "llm.model", os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")),
        api_key=api_key,
        base_url=conf_get(conf, "llm.base_url", "https://api.deepseek.com"),
    )

    # Feed
    feed_api_key = env_or_value(conf_get(conf, "news.api_key_env"), conf_get(conf, "news.api_key"))
    if not feed_api_key:
        raise SystemExit("COINDESK_API_KEY is required (configure in config/base.yaml or env)")
    feed = CoinDeskClient(api_key=feed_api_key)

    return {
        "feed": feed,
        "events": events,
        "assets": assets,
        "agents": agents,
        "observations": observations,
        "rounds": rounds,
        "indicators": indicators,
        "llm": llm,
        "sb": sb,
    }


def run_tick(conf: Dict[str, Any], deps: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    minute = now.minute
    logging.info("Cron: tick start", extra={"now": now.isoformat(), "minute": minute})

    if minute == 0:
        # :00 — run hourly backfill rounds
        try:
            uc = BackfillRecentRounds(
                feed=deps["feed"],
                events=deps["events"],
                assets=deps["assets"],
                agents=deps["agents"],
                observations=deps["observations"],
                factorizer=deps["llm"],
                indicators=deps["indicators"],
                rounds=deps["rounds"],
                gen_per_agent_limit=conf_get(conf, "generation.per_agent_limit", None),
                gen_max_tokens=int(conf_get(conf, "generation.max_tokens", conf_get(conf, "llm.max_tokens", 256))),
                ingest_categories=conf_get(conf, "news.categories", []),
                ind_timeframe=str(conf_get(conf, "indicators.timeframe", "1h")),
                ind_rsi_period=int(conf_get(conf, "indicators.rsi_period", 14)),
                ind_sma_fast=int(conf_get(conf, "indicators.sma_fast", 50)),
                ind_sma_slow=int(conf_get(conf, "indicators.sma_slow", 200)),
                min_events_per_round=int(conf_get(conf, "backfill.min_events_per_round", 0)),
            )

            res = uc.run(
                n=int(conf_get(conf, "backfill.n_rounds", 2)),
                timeframe=str(conf_get(conf, "backfill.timeframe", "1h")),
                ingest_limit=int(conf_get(conf, "backfill.ingest_limit", 500)),
                quote=str(conf_get(conf, "backfill.quote", "USDT")),
            )
            logging.info(
                "Cron: backfill done",
                extra={
                    "requested": res.requested,
                    "existing": res.existing,
                    "processed": res.processed,
                    "skipped": res.skipped,
                },
            )
        except Exception as e:
            logging.warning("Cron: backfill failed", extra={"error": str(e)})

    elif minute == 30:
        # :30 — ingest events up to :30 and generate observations for [prev :00, :30)
        window_end = snap_to_interval(now, freq="30m", mode="floor")
        window_start = window_end - timedelta(minutes=30)

        # Ingest latest events until window_end
        try:
            ingester = IngestEvents(
                feed=deps["feed"],
                events=deps["events"],
                assets=deps["assets"],
            )
            ires = ingester.run(
                limit=int(conf_get(conf, "backfill.ingest_limit", 500)),
                categories=conf_get(conf, "news.categories", []),
                until=window_end,
            )
            logging.info(
                "Cron: half-hour ingest done",
                extra={"inserted": ires.inserted, "updated": ires.updated, "window_end": window_end.isoformat()},
            )
        except Exception as e:
            logging.warning("Cron: half-hour ingest failed", extra={"error": str(e)})

        # Generate observations only for the half-hour window
        try:
            gen = GenerateObservationsForActiveAgents(
                agents=deps["agents"],
                observations=deps["observations"],
                factorizer=deps["llm"],
                indicators=IndicatorSnapshotBuilder(
                    deps["indicators"],
                    timeframe=str(conf_get(conf, "indicators.timeframe", "1h")),
                    rsi_period=int(conf_get(conf, "indicators.rsi_period", 14)),
                    sma_fast=int(conf_get(conf, "indicators.sma_fast", 50)),
                    sma_slow=int(conf_get(conf, "indicators.sma_slow", 200)),
                ),
            )
            gres = gen.run(
                window_start=window_start,
                window_end=window_end,
                per_agent_limit=conf_get(conf, "generation.per_agent_limit", None),
                max_tokens=int(conf_get(conf, "generation.max_tokens", conf_get(conf, "llm.max_tokens", 256))),
            )
            logging.info(
                "Cron: half-hour observations done",
                extra={
                    "total_agents": gres.total_agents,
                    "total_events": gres.total_events,
                    "inserted": gres.upserted.inserted,
                    "updated": gres.upserted.updated,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                },
            )
        except Exception as e:
            logging.warning("Cron: half-hour observations failed", extra={"error": str(e)})

    else:
        # In case of jitter or manual run off-schedule
        logging.info("Cron: no-op (off schedule)", extra={"minute": minute})


def main():
    logging.basicConfig(level=logging.INFO)
    conf = load_base_config()
    deps = build_dependencies(conf)

    # Run a tick every 30 minutes to alternate :00 and :30 actions
    tick_freq = "30m"
    eager = bool(conf_get(conf, "cron.eager_first_run", True))
    jitter = int(conf_get(conf, "cron.jitter_seconds", 0) or 0)

    if eager:
        run_tick(conf, deps)

    try:
        while True:
            now = datetime.now(timezone.utc)
            next_tick = snap_to_interval(now, freq=tick_freq, mode="ceil")
            sleep_sec = max(0, int((next_tick - now).total_seconds()))
            if jitter > 0:
                sleep_sec += min(jitter, 5)  # avoid large drift
            logging.info("Cron: sleeping", extra={"seconds": sleep_sec, "next": next_tick.isoformat()})
            time.sleep(sleep_sec)
            run_tick(conf, deps)
    except KeyboardInterrupt:
        logging.info("Cron: stopped by user")


if __name__ == "__main__":
    main()