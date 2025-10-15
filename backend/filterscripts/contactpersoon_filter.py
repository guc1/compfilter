"""Filter logic for 'contactpersoon' presence."""
import ast
from typing import Iterable, List, Generator, Optional

from .runtime import FilterContext

FILTER_KEY = "contactpersoon"


def name() -> str:
    return FILTER_KEY


def distinct_values(*_a, **_k) -> List[str]:
    # fixed options for boolean-like filter
    return ["TRUE", "FALSE"]


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

def _has_contact(cell: Optional[str]) -> bool:
    if cell is None:
        return False
    s = cell.strip()
    if s == "":
        return False
    # common empty literal strings
    if s in ("[]", "{}", "null", "None"):
        return False
    # try to parse list/dict
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, (list, tuple, set, dict)):
            return len(obj) > 0
        # any other parsed scalar that is non-empty -> treat as present
        return True
    except Exception:
        # fall back: any non-empty string counts as present
        return True

def apply(
    rows_iter: Iterable[List[str]],
    header: List[str],
    selected_values: List[str],
) -> Generator[List[str], None, None]:
    context = FilterContext.from_header(header)
    yield from apply_with_context(rows_iter, header, selected_values, context)


def apply_with_context(
    rows_iter: Iterable[List[str]],
    header: List[str],
    selected_values: List[str],
    context: FilterContext,
) -> Iterable[List[str]]:
    """Apply the contactpersoon presence filter using cached header lookups."""
    sel = {v.strip().upper() for v in (selected_values or []) if isinstance(v, str)}
    if not sel or sel == {"TRUE", "FALSE"}:
        return rows_iter

    idx = _find_col(
        header,
        ["contactpersoon", "contact_persoon", "contact_person", "contactpersonen"],
        context,
    )
    want_true = "TRUE" in sel and "FALSE" not in sel
    want_false = "FALSE" in sel and "TRUE" not in sel

    if idx is None:
        if want_true and not want_false:
            return []
        return rows_iter

    def _iter() -> Generator[List[str], None, None]:
        for row in rows_iter:
            present = _has_contact(row[idx] if idx < len(row) else "")
            if want_true and present:
                yield row
            elif want_false and (not present):
                yield row

    return _iter()
