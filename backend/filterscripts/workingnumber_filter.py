"""Working number filter: keep rows where the employee count range overlaps the user's range."""

from typing import Iterable, List, Generator, Optional

from .runtime import FilterContext

FILTER_KEY = "workingnumber"

CANDS_MIN = ["workingminimum", "working_minimum", "werk_min", "min_employees"]
CANDS_MAX = ["workingmaximum", "working_maximum", "werk_max", "max_employees"]

UNKNOWN_SENTINEL = 999_999_999  # treat as unknown, not infinity, for filtering logic


def name() -> str:
    return FILTER_KEY


def distinct_values(*_a, **_k):
    return []  # numeric input from UI


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

def _to_int_or_none(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        # some files store as floats in text
        return int(float(s))
    except Exception:
        return None

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    context = FilterContext.from_header(header)
    yield from apply_with_context(rows_iter, header, selected_values, context)


def apply_with_context(
    rows_iter: Iterable[List[str]],
    header: List[str],
    selected_values: List[str],
    context: FilterContext,
) -> Iterable[List[str]]:
    u_min = _to_int_or_none(selected_values[0]) if (selected_values and len(selected_values) > 0) else None
    u_max = _to_int_or_none(selected_values[1]) if (selected_values and len(selected_values) > 1) else None

    if u_min is None and u_max is None:
        return rows_iter

    i_min = _find_col(header, CANDS_MIN, context)
    i_max = _find_col(header, CANDS_MAX, context)
    if i_min is None or i_max is None:
        return []

    eff_u_min = u_min if u_min is not None else -10**18
    eff_u_max = u_max if u_max is not None else 10**18

    def _iter() -> Generator[List[str], None, None]:
        for row in rows_iter:
            r_min = _to_int_or_none(row[i_min] if i_min < len(row) else None)
            r_max = _to_int_or_none(row[i_max] if i_max < len(row) else None)

            is_unknown = (
                r_min is None or r_max is None or
                r_max == UNKNOWN_SENTINEL
            )

            if is_unknown:
                continue

            if r_min is None or r_max is None or r_min > r_max:
                continue

            if (r_min <= eff_u_max) and (eff_u_min <= r_max):
                yield row

    return _iter()
