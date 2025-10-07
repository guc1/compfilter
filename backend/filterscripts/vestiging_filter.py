"""Filter 'vestiging' (group panel) combining:
- gebruiksdoelverblijfsobject (multiselect)
- hoofdvestiging (TRUE/FALSE; tri-state)
- kvk_non_mailing_indicator (TRUE/FALSE; tri-state)
- oppervlakteverblijfsobject (min/max; either side optional)

Selected values are provided as a flat list of tokens from the UI:
  - "gd=<value>" for each selected gebruiksdoel
  - "hv=TRUE" | "hv=FALSE"
  - "nm=TRUE" | "nm=FALSE"
  - "oppmin=<number>" (optional)
  - "oppmax=<number>" (optional)

Only constraints that are present are applied (AND logic).
"""

import csv
from typing import Iterable, List, Generator, Optional, Dict, Set, Tuple
from ..config import CSV_DELIMITER, CSV_ENCODING

FILTER_KEY = "vestiging"

# Canonical option list (you provided these)
GEBRUIKSDOEL_OPTIONS = [
    "woonfunctie",
    "kantoorfunctie",
    "industriefunctie",
    "winkelfunctie",
    "bijeenkomstfunctie",
    "gezondheidszorgfunctie",
    "onderwijsfunctie",
    "overige gebruiksfunctie",
    "sportfunctie",
    "logiesfunctie",
    "ligplaats",
    "standplaats",
    "UNKNOWN",
    "celfunctie",
]

def name() -> str:
    return FILTER_KEY

def distinct_values(*_a, **_k) -> List[str]:
    """Return gebruiksdoel choices; UI will add the other controls."""
    return GEBRUIKSDOEL_OPTIONS

def _find_col(header: List[str], candidates: List[str]) -> Optional[int]:
    norm = [h.strip().lower() for h in header]
    for c in candidates:
        c2 = c.strip().lower()
        if c2 in norm:
            return norm.index(c2)
    return None

def _truthy(cell: Optional[str]) -> Optional[bool]:
    """Normalize various TRUE/FALSE-like values. Returns True/False, or None if unknown/empty."""
    if cell is None:
        return None
    s = cell.strip().upper()
    if s == "":
        return None
    if s in {"TRUE", "1", "J", "JA", "Y", "YES"}:
        return True
    if s in {"FALSE", "0", "N", "NEE", "NO"}:
        return False
    return None  # unknown token

def _to_int_or_none(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None

def _parse_tokens(selected_values: List[str]) -> Dict[str, object]:
    """Turn flat token list into constraints dict."""
    gd: Set[str] = set()
    hv_wants: Set[bool] = set()
    nm_wants: Set[bool] = set()
    oppmin: Optional[int] = None
    oppmax: Optional[int] = None

    for tok in (selected_values or []):
        t = (tok or "").strip()
        if not t:
            continue
        low = t.lower()

        if low.startswith("gd="):
            gd.add(t[3:])
        elif low == "hv=true":
            hv_wants.add(True)
        elif low == "hv=false":
            hv_wants.add(False)
        elif low == "nm=true":
            nm_wants.add(True)
        elif low == "nm=false":
            nm_wants.add(False)
        elif low.startswith("oppmin="):
            v = _to_int_or_none(t.split("=",1)[1])
            if v is not None:
                oppmin = v
        elif low.startswith("oppmax="):
            v = _to_int_or_none(t.split("=",1)[1])
            if v is not None:
                oppmax = v

    # If both TRUE and FALSE picked for the same flag, ignore that flag.
    hv: Optional[bool]
    nm: Optional[bool]
    hv = None if (True in hv_wants and False in hv_wants) else (True if True in hv_wants else (False if False in hv_wants else None))
    nm = None if (True in nm_wants and False in nm_wants) else (True if True in nm_wants else (False if False in nm_wants else None))

    return {"gd": gd, "hv": hv, "nm": nm, "oppmin": oppmin, "oppmax": oppmax}

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    cons = _parse_tokens(selected_values)

    # If nothing selected at all -> passthrough
    if not cons["gd"] and cons["hv"] is None and cons["nm"] is None and cons["oppmin"] is None and cons["oppmax"] is None:
        yield from rows_iter
        return

    # Column indices
    gd_idx   = _find_col(header, ["gebruiksdoelverblijfsobject", "gebruiksdoel", "gebruiksdoel_verblijfsobject"])
    hv_idx   = _find_col(header, ["hoofdvestiging", "is_hoofdv", "ishoofdvestiging"])
    nm_idx   = _find_col(header, ["kvk_non_mailing_indicator", "non_mailing_indicator", "nonmailing", "non_mailing"])
    opp_idx  = _find_col(header, ["oppervlakteverblijfsobject", "oppervlakte", "oppervlakte_verblijfsobject"])

    # If a constraint exists for a column we can't find, then nothing can match
    if cons["gd"] and gd_idx is None:
        return
    if cons["hv"] is not None and hv_idx is None:
        return
    if cons["nm"] is not None and nm_idx is None:
        return
    if (cons["oppmin"] is not None or cons["oppmax"] is not None) and opp_idx is None:
        return

    # Defaults for open-ended range
    INF = 10**18
    umin = 0 if cons["oppmin"] is None else cons["oppmin"]
    umax = INF if cons["oppmax"] is None else cons["oppmax"]

    want_gd: Set[str] = set(x.strip() for x in cons["gd"]) if cons["gd"] else set()

    for row in rows_iter:
        ok = True

        if ok and want_gd:
            val = (row[gd_idx] if gd_idx is not None and gd_idx < len(row) else "")
            v = " ".join(val.strip().lower().split()) if val else "unknown"
            # Compare lowercase normalized
            ok = (v in {s.lower() for s in want_gd})

        if ok and cons["hv"] is not None:
            val = row[hv_idx] if hv_idx is not None and hv_idx < len(row) else None
            ok = (_truthy(val) == cons["hv"])

        if ok and cons["nm"] is not None:
            val = row[nm_idx] if nm_idx is not None and nm_idx < len(row) else None
            ok = (_truthy(val) == cons["nm"])

        if ok and (cons["oppmin"] is not None or cons["oppmax"] is not None):
            raw = row[opp_idx] if opp_idx is not None and opp_idx < len(row) else None
            vv = _to_int_or_none(raw)
            if vv is None:
                ok = False
            else:
                ok = (umin <= vv <= umax)

        if ok:
            yield row
