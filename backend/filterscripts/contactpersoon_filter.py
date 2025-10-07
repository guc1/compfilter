"""Filter logic for 'contactpersoon' presence."""
import ast
from typing import Iterable, List, Generator, Optional

FILTER_KEY = "contactpersoon"

def name() -> str:
    return FILTER_KEY

def distinct_values(*_a, **_k) -> List[str]:
    # fixed options for boolean-like filter
    return ["TRUE", "FALSE"]

def _find_col(header: List[str], candidates: List[str]) -> Optional[int]:
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

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    """
    TRUE  => keep rows where contactpersoon is present
    FALSE => keep rows where contactpersoon is empty/absent
    If both or none selected => passthrough.
    """
    sel = {v.strip().upper() for v in (selected_values or []) if isinstance(v, str)}
    if not sel or sel == {"TRUE", "FALSE"}:
        # selecting both (or nothing) is effectively no filter
        yield from rows_iter
        return

    idx = _find_col(header, ["contactpersoon", "contact_persoon", "contact_person", "contactpersonen"])
    if idx is None:
        # column missing => if user wants TRUE, nothing matches. For FALSE, everything matches.
        want_true = "TRUE" in sel and "FALSE" not in sel
        if want_true:
            return
        else:
            yield from rows_iter
            return

    want_true = "TRUE" in sel and "FALSE" not in sel
    want_false = "FALSE" in sel and "TRUE" not in sel

    for row in rows_iter:
        present = _has_contact(row[idx] if idx < len(row) else "")
        if want_true and present:
            yield row
        elif want_false and (not present):
            yield row
