import os
import logging
from datetime import datetime, timedelta, timezone

from supabase import Client, create_client

from src.domain.rounds import Round
from src.infrastructure.repositories.supabase.observations import SupabaseObservationRepository
from src.infrastructure.indicators.ccxt_service import CcxtIndicatorService
from src.application.use_cases.evaluate_round import EvaluateRound
from src.infrastructure.repositories.supabase.rounds import SupabaseRoundRepository


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
    logging.basicConfig(level=logging.INFO)
    sb = make_supabase()
    observations = SupabaseObservationRepository(sb_client=sb)
    rounds_repo = SupabaseRoundRepository(sb_client=sb)
    indicators = CcxtIndicatorService(exchange_id=os.environ.get("CCXT_EXCHANGE", "binance"))

    now = datetime.now(tz=timezone.utc)
    start = parse_iso_utc(os.environ.get("WINDOW_START")) or (now - timedelta(hours=1))
    end = parse_iso_utc(os.environ.get("WINDOW_END")) or now 
    quote = os.environ.get("QUOTE", "USDT")
    timeframe = os.environ.get("TIMEFRAME", "1h")

    rnd_key = os.environ.get("ROUND_KEY", f"round-{start.strftime('%Y%m%d%H%M')}-{end.strftime('%Y%m%d%H%M')}")
    rnd = Round(key=rnd_key, window_start=start, window_end=end).snapped(timeframe=timeframe)
    uc = EvaluateRound(observations=observations, indicators=indicators, rounds=rounds_repo)
    res = uc.run(round=rnd, quote=quote, timeframe=timeframe)

    print({
        "round": res.round.key,
        "window_start": res.round.window_start.isoformat(),
        "window_end": res.round.window_end.isoformat(),
        "scores": [s.__dict__ for s in res.agent_scores],
    })


if __name__ == "__main__":
    main()
