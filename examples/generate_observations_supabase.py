import os
from datetime import datetime, timedelta, timezone

from supabase import Client, create_client

from src.infrastructure.repositories.supabase.agents import SupabaseAgentRepository
from src.infrastructure.repositories.supabase.observations import SupabaseObservationRepository
from src.application.use_cases.generate_observations_for_active_agents import (
    GenerateObservationsForActiveAgents,
)
from src.infrastructure.llm.deepseek import DeepseekClient
from src.infrastructure.indicators.ccxt_service import CcxtIndicatorService
from src.application.services.indicator_snapshot import IndicatorSnapshotBuilder


def make_supabase() -> Client:
    url = os.environ["SUPABASE_DEV_URL"]
    key = os.environ["SUPABASE_DEV_KEY"]
    return create_client(url, key)


def parse_iso_utc(value: str | None):
    if not value:
        return None
    v = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def main():
    sb = make_supabase()
    agents_repo = SupabaseAgentRepository(sb_client=sb)
    observations_repo = SupabaseObservationRepository(sb_client=sb)


    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is required")
    model = "deepseek-chat"
    llm = DeepseekClient(model=model, api_key=api_key)

    # Indicators (optional)
    indicators = None
    try:
        ind_svc = CcxtIndicatorService(exchange_id=os.environ.get("CCXT_EXCHANGE", "binance"))
        indicators = IndicatorSnapshotBuilder(ind_svc)
    except Exception:
        indicators = None

    now = datetime.now(tz=timezone.utc)
    window_start = now - timedelta(hours=24)
    window_end = now
    per_agent_limit = "10"

    uc = GenerateObservationsForActiveAgents(
        agents=agents_repo,
        observations=observations_repo,
        factorizer=llm,
        indicators=indicators,
    )

    res = uc.run(
        window_start=window_start,
        window_end=window_end,
        per_agent_limit=per_agent_limit,
    )

    print({
        "total_agents": res.total_agents,
        "total_events": res.total_events,
        "inserted": res.upserted.inserted,
        "updated": res.upserted.updated,
    })


if __name__ == "__main__":
    main()