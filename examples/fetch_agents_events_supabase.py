import os
from datetime import datetime, timedelta, timezone

from supabase import Client, create_client

from src.infrastructure.repositories.supabase.events import SupabaseEventRepository
from src.infrastructure.repositories.supabase.agents import SupabaseAgentRepository
from src.application.use_cases.fetch_events_for_active_agents import (
    FetchEventsForActiveAgents,
)


def make_supabase() -> Client:
    url = os.environ["SUPABASE_DEV_URL"]
    key = os.environ["SUPABASE_DEV_KEY"]
    return create_client(url, key)


def main():
    sb = make_supabase()

    agents_repo = SupabaseAgentRepository(sb_client=sb)

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    window_end = now 
    window_start = window_end - timedelta(hours=1)

    uc = FetchEventsForActiveAgents(agents=agents_repo)
    res = uc.run(window_start=window_start, window_end=window_end, per_agent_limit=10)

    print(f"Active agents: {res.total_agents}")
    print(f"Total events: {res.total_events}")
    for r in res.results:
        print(f"Agent {r.agent_id}: {len(r.events)} events")
        for e in r.events:
            print(" -", e.event_id, e.occurred_at.isoformat(), ",", e.title)


if __name__ == "__main__":
    main()