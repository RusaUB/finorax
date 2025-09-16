import re
from typing import Iterable, List, Set

from src.domain.assets import Asset
from src.domain.events import Event
from src.repositories.assets import AssetRepository


class AssetExtractor:
    """
    Extracts asset symbols from an Event.

    Strategy:
    - Known symbols come from an AssetRepository (or a passed-in set).
    - Matches symbols in `title` and `content` using a compiled regex:
      - Case-insensitive, optional leading "$", word-like boundaries.
    - Also checks `categories` for exact symbol matches.
    - Returns unique assets; empty list if none found.
    """

    def __init__(self, known_symbols: Set[str]):
        if not isinstance(known_symbols, set):
            known_symbols = set(known_symbols or [])
        self._symbols: Set[str] = {s.strip().upper() for s in known_symbols if (s or "").strip()}
        self._pattern = self._compile_pattern(self._symbols)

    @classmethod
    def from_repository(cls, repo: AssetRepository) -> "AssetExtractor":
        return cls(known_symbols=repo.list_symbols())

    def extract_symbols(self, event: Event) -> Set[str]:
        """
        Return a set of matched asset symbols (uppercase) for the given event.
        """
        if not self._symbols:
            return set()

        haystack = f"{event.title or ''} \n {event.content or ''}"

        matches_in_text = set(m.group(1).upper() for m in self._pattern.finditer(haystack))

        # Include exact category matches as symbols too
        cat_symbols = { (c or "").strip().upper() for c in (event.categories or []) }
        matches_in_cats = { c for c in cat_symbols if c in self._symbols }

        return { *matches_in_text, *matches_in_cats }

    def extract_assets(self, event: Event) -> List[Asset]:
        """
        Return a list of Asset objects (unique by symbol). Empty if none.
        """
        syms = self.extract_symbols(event)
        # Sort for determinism
        return [Asset(symbol=s) for s in sorted(syms)]

    # --- internals ---
    def _compile_pattern(self, symbols: Set[str]) -> re.Pattern:
        if not symbols:
            # Match nothing
            return re.compile(r"a\b^", re.IGNORECASE)

        # Sort by length descending to prefer the longest symbol where relevant
        parts = [re.escape(s) for s in sorted(symbols, key=lambda x: (-len(x), x))]
        # Optional leading '$', and ensure not surrounded by alphanumerics
        pattern = rf"(?<![A-Za-z0-9])(?:\$)?(" + "|".join(parts) + rf")(?![A-Za-z0-9])"
        return re.compile(pattern, re.IGNORECASE)