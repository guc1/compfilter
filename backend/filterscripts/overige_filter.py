"""Filter 'overige' combining:
- oprichtingsdatum range (date_min/date_max in ISO from UI date inputs)
- tradenames presence TRUE/FALSE (tri-state)

Tokens from UI:
  - date_min=YYYY-MM-DD
  - date_max=YYYY-MM-DD
  - tn=TRUE | tn=FALSE
"""

from typing import Iterable, List, Generator, Optional, Dict
import re
from datetime import date

FILTER_KEY = "overige"

# Column candidates
DATE_COL_CANDS = ["oprichtingsdatum", "oprichtings_datum", "date_of_incorporation", "foundation_date"]
TN_COL_CANDS   = ["tradenames", "trade_names", "handelsnamen", "handels_namen"]

NL_MONTHS = {
    "januari":1, "februari":2, "maart":3, "april":4, "mei":5, "juni":6,
    "juli":7, "augustus":8, "september":9, "oktober":10, "november":11, "december":12
}

def name() -> str:
    return FILTER_KEY

def distinct_values(*_a, **_k) -> List[str]:
    return []  # group UI provides inputs

def _find_col(header: List[str], candidates: List[str]) -> Optional[int]:
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
    if s == "" or s in ("[]", "{}", "null", "None"):
        return False
    return True

def _parse_iso(s: str) -> Optional[date]:
    try:
        y,m,d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None

def _parse_nl_date(s: str) -> Optional[date]:
    # e.g., "11 maart 2019", "1 januari 2005"
    m = re.match(r"^\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*$", s.strip(), re.IGNORECASE)
    if not m:
        return None
    dd, mon, yy = m.groups()
    mon = mon.lower()
    if mon not in NL_MONTHS:
        return None
    try:
        return date(int(yy), NL_MONTHS[mon], int(dd))
    except Exception:
        return None

def _parse_any_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    # try ISO first
    iso = _parse_iso(s)
    if iso:
        return iso
    # try dutch human format
    return _parse_nl_date(s)

def _parse_tokens(selected_values: List[str]) -> Dict[str, object]:
    dmin = None
    dmax = None
    tn_wants = set()
    for tok in (selected_values or []):
        t = (tok or "").strip()
        if not t: continue
        low = t.lower()
        if low.startswith("date_min="):
            dmin = _parse_iso(t.split("=",1)[1])
        elif low.startswith("date_max="):
            dmax = _parse_iso(t.split("=",1)[1])
        elif low == "tn=true":
            tn_wants.add(True)
        elif low == "tn=false":
            tn_wants.add(False)
    # tn tri-state
    if True in tn_wants and False in tn_wants:
        tn = None
    elif True in tn_wants:
        tn = True
    elif False in tn_wants:
        tn = False
    else:
        tn = None
    return {"date_min": dmin, "date_max": dmax, "tn": tn}

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    cons = _parse_tokens(selected_values)
    if cons["date_min"] is None and cons["date_max"] is None and cons["tn"] is None:
        yield from rows_iter
        return

    d_idx = _find_col(header, DATE_COL_CANDS) if (cons["date_min"] or cons["date_max"]) else None
    t_idx = _find_col(header, TN_COL_CANDS)   if (cons["tn"] is not None) else None

    if (cons["date_min"] or cons["date_max"]) and d_idx is None:
        return
    if cons["tn"] is not None and t_idx is None:
        return

    dmin = cons["date_min"] or date.min
    dmax = cons["date_max"] or date.max

    for row in rows_iter:
        ok = True
        if (cons["date_min"] or cons["date_max"]):
            raw = row[d_idx] if d_idx is not None and d_idx < len(row) else ""
            parsed = _parse_any_date(raw)
            if not parsed:
                ok = False
            else:
                ok = (dmin <= parsed <= dmax)

        if ok and cons["tn"] is not None:
            present = _has_value(row[t_idx] if t_idx is not None and t_idx < len(row) else "")
            ok = ok and (present == cons["tn"])

        if ok:
            yield row
