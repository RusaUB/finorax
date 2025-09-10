import requests
from typing import Iterable, List
from datetime import datetime, timezone
from src.shared.utils.time import snap_to_interval
from src.application.ports import NewsFeedPort
from src.application.ports import NewsItemDTO

class CoinDeskClient(NewsFeedPort):
    def __init__(self, api_key:str, base_url: str = "https://data-api.coindesk.com/news/v1/article/list"):
        self.api_key = api_key
        self.base_url = base_url

    def fetch(self,limit:int = 10, categories: List[str] = [], until: datetime | None = None) -> Iterable[NewsItemDTO]:
        params = {"lang":"EN", "limit":limit, "api_key":self.api_key}
        if until:
            params["to_ts"] = snap_to_interval(dt=until).timestamp()
        else:
            params["to_ts"] = -1
        if len(categories) > 0:
            params["categories"] = categories
        res = requests.get(self.base_url, params=params, headers={"Content-type":"application/json; charset=UTF-8"}).json()
        for item in res.get("Data",[]):
            yield NewsItemDTO(
                external_id=item["ID"],
                external_url=item["URL"],
                published_at=datetime.fromtimestamp(item["PUBLISHED_ON"], tz=timezone.utc),
                title=item["TITLE"],
                content=item["BODY"],
                categories=[i["CATEGORY"] for i in item["CATEGORY_DATA"]],
                source="coindesk"
            )