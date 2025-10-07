"""Working number filter: keep rows where the employee count range overlaps the user's range.
- Columns: workingminimum, workingmaximum (case-insensitive)
- Placeholder 999999999 means "unknown". When the user sets any bound (min or max),
  rows with unknown (e.g. min=0 & max=999999999) are EXCLUDED.
- If user leaves both bounds empty => filter inactive (passthrough).
- User tokens: ["<min>","<max>"] where each is "" (ignore) or integer string.
"""

from typing import Iterable, List, Generator, Optional

FILTER_KEY = "workingnumber"

CANDS_MIN = ["workingminimum", "working_minimum", "werk_min", "min_employees"]
CANDS_MAX = ["workingmaximum", "working_maximum", "werk_max", "max_employees"]

UNKNOWN_SENTINEL = 999_999_999  # treat as unknown, not infinity, for filtering logic

def name() -> str:
    return FILTER_KEY

def distinct_values(*_a, **_k):
    return []  # numeric input from UI

def _find_col(header: List[str], candidates: List[str]) -> Optional[int]:
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
    # Parse user min/max
    u_min = _to_int_or_none(selected_values[0]) if (selected_values and len(selected_values) > 0) else None
    u_max = _to_int_or_none(selected_values[1]) if (selected_values and len(selected_values) > 1) else None

    # If no bounds -> passthrough
    if u_min is None and u_max is None:
        yield from rows_iter
        return

    i_min = _find_col(header, CANDS_MIN)
    i_max = _find_col(header, CANDS_MAX)
    if i_min is None or i_max is None:
        # Can't apply without columns
        return

    for row in rows_iter:
        r_min = _to_int_or_none(row[i_min] if i_min < len(row) else None)
        r_max = _to_int_or_none(row[i_max] if i_max < len(row) else None)

        # Mark unknown if sentinel present or either missing
        is_unknown = (
            r_min is None or r_max is None or
            r_max == UNKNOWN_SENTINEL
        )

        if is_unknown:
            # When user set any bound, unknown rows are excluded
            continue

        # Build effective user range; open-ended sides expand to wide numbers
        eff_u_min = u_min if u_min is not None else -10**18
        eff_u_max = u_max if u_max is not None else  10**18

        # Valid sanity: r_min <= r_max; if not, skip
        if r_min is None or r_max is None or r_min > r_max:
            continue

        # Overlap test: [r_min, r_max] ∩ [eff_u_min, eff_u_max] ≠ ∅
        if (r_min <= eff_u_max) and (eff_u_min <= r_max):
            yield row
