from supabase import Client

from src.domain.assets import Asset
from src.repositories.assets import AssetRepository


class SupabaseAssetRepository(AssetRepository):
    def __init__(self, sb_client: Client, table: str = "assets") -> None:
        self.sb = sb_client
        self.table = table

    def get_asset(self, symbol: str) -> Asset:
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required")

        res = self.sb.table(self.table).select("symbol").eq("symbol", sym).limit(1).execute()
        rows = res.data or []
        if not rows:
            raise ValueError(f"Asset not found: {sym}")
        return Asset(symbol=rows[0]["symbol"]) 

    def list_symbols(self) -> set[str]:
        res = self.sb.table(self.table).select("symbol").execute()
        rows = res.data or []
        return { (r.get("symbol") or "").strip().upper() for r in rows if (r.get("symbol") or "").strip() }