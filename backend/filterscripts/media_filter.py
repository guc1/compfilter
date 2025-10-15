"""Filter logic for 'media' presence across multiple columns.

Selected channels must ALL be present in the row (AND logic).
Channels: email, facebook, instagram, linkedin, pinterest, twitter, youtube, internetaddress.
"""

import ast
from typing import Iterable, List, Generator, Optional, Dict

from .runtime import FilterContext

FILTER_KEY = "media"

CHANNELS = ["email", "facebook", "instagram", "linkedin", "pinterest", "twitter", "youtube", "internetaddress"]

# Candidate column names per channel (case-insensitive)
COL_CANDIDATES: Dict[str, List[str]] = {
    "email":            ["email", "e-mail", "emails"],
    "facebook":         ["facebook", "fb"],
    "instagram":        ["instagram", "ig"],
    "linkedin":         ["linkedin", "linked_in"],
    "pinterest":        ["pinterest"],
    "twitter":          ["twitter", "x"],
    "youtube":          ["youtube", "you_tube"],
    "internetaddress":  ["internetaddress", "website", "url", "site", "homepage", "web", "internet_adres"],
}


def name() -> str:
    return FILTER_KEY


def distinct_values(*_a, **_k) -> List[str]:
    # fixed list used to render checkboxes
    return CHANNELS


def _find_col(
    header: List[str],
    candidates: List[str],
    context: Optional[FilterContext] = None,
) -> Optional[int]:
    if context is not None:
        return context.index_for_candidates(candidates)
    norm = [h.strip().lower() for h in header]
    for c in candidates:
        c2 = c.strip().lower()
        if c2 in norm:
            return norm.index(c2)
    return None

def _has_value(cell: Optional[str]) -> bool:
    if cell is None:
        return False
    s = cell.strip()
    if s == "":
        return False
    if s in ("[]", "{}", "null", "None"):
        return False
    # try list/dict parsing
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, (list, tuple, set, dict)):
            return len(obj) > 0
        return True
    except Exception:
        return True

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    context = FilterContext.from_header(header)
    yield from apply_with_context(rows_iter, header, selected_values, context)


def apply_with_context(
    rows_iter: Iterable[List[str]],
    header: List[str],
    selected_values: List[str],
    context: FilterContext,
) -> Iterable[List[str]]:
    """Keep rows where ALL selected media channels are present (AND)."""
    selected = [
        v.strip().lower()
        for v in (selected_values or [])
        if isinstance(v, str) and v.strip()
    ]
    if not selected:
        return rows_iter

    idx_map: Dict[str, Optional[int]] = {}
    for ch in selected:
        idx_map[ch] = _find_col(header, COL_CANDIDATES.get(ch, [ch]), context)

    if any(idx is None for idx in idx_map.values()):
        return []

    def _iter() -> Generator[List[str], None, None]:
        for row in rows_iter:
            ok = True
            for ch, idx in idx_map.items():
                cell = row[idx] if idx is not None and idx < len(row) else ""
                if not _has_value(cell):
                    ok = False
                    break
            if ok:
                yield row

    return _iter()
