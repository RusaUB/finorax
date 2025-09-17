import os
import logging
from supabase import Client, create_client
from src.infrastructure.fetchers.clients.coindesk import CoinDeskClient
from src.infrastructure.repositories.supabase.events import SupabaseEventRepository
from src.infrastructure.repositories.supabase.assets import SupabaseAssetRepository
from src.application.use_cases.ingest_events import IngestEvents
from datetime import datetime, timezone

def make_supabase() -> Client:
    url = os.environ["SUPABASE_DEV_URL"]
    key = os.environ["SUPABASE_DEV_KEY"]  
    return create_client(url, key)

def main():
    logging.basicConfig(level=logging.INFO)
    sb = make_supabase()
    coindesk_api_key = os.environ["COINDESK_API_KEY"]

    feed = CoinDeskClient(api_key=coindesk_api_key)
    events_repo = SupabaseEventRepository(sb_client=sb)
    assets_repo = SupabaseAssetRepository(sb_client=sb)

    uc = IngestEvents(feed=feed, events=events_repo, assets=assets_repo)
    res = uc.run(until=datetime.now(timezone.utc))
    #yesterday
    #res = uc.run(until=datetime.now(tz=timezone.utc)-timedelta(1,0,0,0,0,0,0))
    return res

if __name__ == "__main__":
    main()