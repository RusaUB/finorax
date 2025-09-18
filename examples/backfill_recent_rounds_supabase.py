import os
import logging
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
from src.config.loader import load_base_config, conf_get, env_or_value


def make_supabase(conf: Dict[str, Any]) -> Client:
    url = env_or_value(conf_get(conf, "supabase.url_env"), conf_get(conf, "supabase.url"))
    print(conf_get(conf,"supabase.url_env"))
    key = env_or_value(conf_get(conf, "supabase.key_env"), conf_get(conf, "supabase.key"))
    if not url or not key:
        raise SystemExit("Supabase URL/KEY are required (configure in config/base.yaml or env)")
    return create_client(url, key)


def main():
    logging.basicConfig(level=logging.INFO)

    conf = load_base_config()
    print(conf)
    sb = make_supabase(conf)

    # Dependencies
    feed_api_key = env_or_value(conf_get(conf, "news.api_key_env"), conf_get(conf, "news.api_key"))
    if not feed_api_key:
        raise SystemExit("COINDESK_API_KEY is required (configure in config/base.yaml or env)")
    feed = CoinDeskClient(api_key=feed_api_key)
    events = SupabaseEventRepository(sb_client=sb)
    assets = SupabaseAssetRepository(sb_client=sb)
    agents = SupabaseAgentRepository(sb_client=sb)
    observations = SupabaseObservationRepository(sb_client=sb)
    rounds = SupabaseRoundRepository(sb_client=sb)

    indicators = CcxtIndicatorService(exchange_id=conf_get(conf, "indicators.exchange_id", os.environ.get("CCXT_EXCHANGE", "binance")))

    api_key = env_or_value(conf_get(conf, "llm.api_key_env"), conf_get(conf, "llm.api_key"))
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is required (configure in config/base.yaml or env)")
    llm = DeepseekClient(
        model=conf_get(conf, "llm.model", os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")),
        api_key=api_key,
        base_url=conf_get(conf, "llm.base_url", "https://api.deepseek.com"),
    )

    uc = BackfillRecentRounds(
        feed=feed,
        events=events,
        assets=assets,
        agents=agents,
        observations=observations,
        factorizer=llm,
        indicators=indicators,
        rounds=rounds,
        gen_per_agent_limit=conf_get(conf, "generation.per_agent_limit", None),
        gen_max_tokens=int(conf_get(conf, "generation.max_tokens", conf_get(conf, "llm.max_tokens", 256))),
        ingest_categories=conf_get(conf, "news.categories", []),
        ind_timeframe=str(conf_get(conf, "indicators.timeframe", "1h")),
        ind_rsi_period=int(conf_get(conf, "indicators.rsi_period", 14)),
        ind_sma_fast=int(conf_get(conf, "indicators.sma_fast", 50)),
        ind_sma_slow=int(conf_get(conf, "indicators.sma_slow", 200)),
        min_events_per_round=int(conf_get(conf, "backfill.min_events_per_round", 0)),
    )

    n = int(conf_get(conf, "backfill.n_rounds", os.environ.get("N_ROUNDS", 2)))
    timeframe = str(conf_get(conf, "backfill.timeframe", os.environ.get("TIMEFRAME", "1h")))
    ingest_limit = int(conf_get(conf, "backfill.ingest_limit", 500))
    quote = str(conf_get(conf, "backfill.quote", "USDT"))
    res = uc.run(n=n, timeframe=timeframe, ingest_limit=ingest_limit, quote=quote)
    print({
        "requested": res.requested,
        "existing": res.existing,
        "processed": res.processed,
        "skipped": res.skipped,
    })


if __name__ == "__main__":
    main()
