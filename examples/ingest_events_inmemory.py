from src.application.use_cases.ingest_events import IngestEvents
from src.infrastructure.repositories.inmemory.events import InMemoryEventRepository
from examples._fake_feed import FakeFeed, make_fake_items

def main():
    feed = FakeFeed(items=make_fake_items())
    repo = InMemoryEventRepository()

    uc = IngestEvents(feed=feed, events=repo)
    res = uc.run()

    print("Result from use case:", res)
    print("Events stored:", repo.count())

if __name__ == "__main__":
    main()