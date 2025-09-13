from dataclasses import dataclass

@dataclass(frozen=True)
class Asset:
    symbol: str