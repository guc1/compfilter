"""Runtime helpers shared across streaming filters.

The previous implementation repeated header normalization and column index
lookups inside every filter. That approach adds measurable overhead when the
CSV contains millions of rows because each filter recomputed the same
`[col.strip().lower() for col in header]` list for every request.

This module centralises that work in :class:`FilterContext`. The context is
constructed once per request and exposes quick lookups for case-insensitive
column names as well as a tiny cache that filters can use to memoise expensive
preparations. Filters can opt-in by implementing ``apply_with_context`` while
keeping the existing ``apply`` signature for backwards compatibility.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Sequence


def normalize_column_name(name: Optional[str]) -> str:
    """Return a lowercase alphanumeric slug for a CSV header cell."""

    if name is None:
        return ""
    # Normalise BOM-prefixed headers and collapse everything to [a-z0-9].
    return re.sub(r"[^a-z0-9]", "", str(name).lstrip("\ufeff").lower())


@dataclass(slots=True)
class FilterContext:
    """Keep normalised header state for ultra-fast column resolution."""

    header: Sequence[str]
    normalized_header: Sequence[str]
    index_map: Dict[str, int]
    _cache: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_header(cls, header: Sequence[str]) -> "FilterContext":
        normalized = tuple(normalize_column_name(col) for col in header)
        index_map: Dict[str, int] = {}
        for idx, name in enumerate(normalized):
            if name and name not in index_map:
                index_map[name] = idx
        return cls(tuple(header), normalized, index_map)

    def index_for(self, *candidates: str) -> Optional[int]:
        """Return the first matching column index for the given candidates."""

        for cand in candidates:
            if cand is None:
                continue
            normalized = normalize_column_name(cand)
            if normalized and normalized in self.index_map:
                return self.index_map[normalized]
        return None

    def index_for_candidates(self, candidates: Iterable[str]) -> Optional[int]:
        return self.index_for(*tuple(candidates))

    def cached(self, key: str, factory) -> Any:
        """Return cached value keyed by *key*, computing it lazily via factory."""

        if key not in self._cache:
            self._cache[key] = factory()
        return self._cache[key]

