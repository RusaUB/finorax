import os
import logging

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


def make_supabase() -> Client:
    url = os.environ["SUPABASE_DEV_URL"]
    key = os.environ["SUPABASE_DEV_KEY"]
    return create_client(url, key)


def main():
    logging.basicConfig(level=logging.INFO)

    sb = make_supabase()

    # Dependencies
    feed = CoinDeskClient(api_key=os.environ["COINDESK_API_KEY"])
    events = SupabaseEventRepository(sb_client=sb)
    assets = SupabaseAssetRepository(sb_client=sb)
    agents = SupabaseAgentRepository(sb_client=sb)
    observations = SupabaseObservationRepository(sb_client=sb)
    rounds = SupabaseRoundRepository(sb_client=sb)

    indicators = CcxtIndicatorService(exchange_id=os.environ.get("CCXT_EXCHANGE", "binance"))

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is required")
    llm = DeepseekClient(model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"), api_key=api_key)

    uc = BackfillRecentRounds(
        feed=feed,
        events=events,
        assets=assets,
        agents=agents,
        observations=observations,
        factorizer=llm,
        indicators=indicators,
        rounds=rounds,
    )

    n = int(os.environ.get("N_ROUNDS", "2"))
    timeframe = os.environ.get("TIMEFRAME", "1h")
    res = uc.run(n=n, timeframe=timeframe, ingest_limit=1)
    print({
        "requested": res.requested,
        "existing": res.existing,
        "processed": res.processed,
        "skipped": res.skipped,
    })


if __name__ == "__main__":
    main()