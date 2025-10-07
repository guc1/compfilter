"""Filter logic for 'traditional_outreach' over faxnumber_formatted, phonenumber_formatted,
and specialpostadress (postaladdress) presence.

Options exposed as checkboxes:
  - fax=TRUE, fax=FALSE
  - phone=TRUE, phone=FALSE
  - post=TRUE, post=FALSE

Rules:
- Build constraints from selected options; constraints are ANDed.
- If both TRUE and FALSE are selected for the same field, that field is ignored (no constraint).
- Presence means the column exists and is non-empty (handles stringified lists too).
"""

import ast
from typing import Iterable, List, Generator, Optional, Dict

FILTER_KEY = "traditional_outreach"

# columns (case-insensitive)
FAX_COL_CANDS    = ["faxnumber_formatted", "fax", "fax_number"]
PHONE_COL_CANDS  = ["phonenumber_formatted", "phone", "phone_number", "telephone", "telefoon"]
POSTAL_COL_CANDS = ["postaladdress", "postal_address", "postadres", "post_adres", "postbus", "specialpostadress"]

OPTIONS = [
    "fax=TRUE", "fax=FALSE",
    "phone=TRUE", "phone=FALSE",
    "post=TRUE", "post=FALSE",
]

def name() -> str:
    return FILTER_KEY

def distinct_values(*_a, **_k) -> List[str]:
    return OPTIONS

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
    if s == "":
        return False
    if s in ("[]", "{}", "null", "None"):
        return False
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, (list, tuple, set, dict)):
            return len(obj) > 0
        return True
    except Exception:
        return True

def _parse_constraints(selected_values: List[str]) -> Dict[str, Optional[bool]]:
    """
    Returns dict: {'fax': True|False|None, 'phone': True|False|None, 'post': True|False|None}
    None means "no constraint for that field".
    """
    want = {"fax": set(), "phone": set(), "post": set()}
    for v in (selected_values or []):
        s = (v or "").strip().lower()
        if s == "fax=true": want["fax"].add(True)
        elif s == "fax=false": want["fax"].add(False)
        elif s == "phone=true": want["phone"].add(True)
        elif s == "phone=false": want["phone"].add(False)
        elif s == "post=true": want["post"].add(True)
        elif s == "post=false": want["post"].add(False)

    out: Dict[str, Optional[bool]] = {}
    for k in ("fax", "phone", "post"):
        if True in want[k] and False in want[k]:
            out[k] = None  # conflicting -> ignore
        elif True in want[k]:
            out[k] = True
        elif False in want[k]:
            out[k] = False
        else:
            out[k] = None
    return out

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    cons = _parse_constraints(selected_values)
    # If all fields have None (no constraints at all), pass-through
    if all(cons[k] is None for k in ("fax", "phone", "post")):
        yield from rows_iter
        return

    fax_idx   = _find_col(header, FAX_COL_CANDS)   if cons["fax"]  is not None else None
    phone_idx = _find_col(header, PHONE_COL_CANDS) if cons["phone"] is not None else None
    post_idx  = _find_col(header, POSTAL_COL_CANDS)if cons["post"] is not None else None

    # If a constraint is required but the column is missing -> no rows can match
    if (cons["fax"]  is not None and fax_idx  is None) or \
       (cons["phone"]is not None and phone_idx is None) or \
       (cons["post"] is not None and post_idx  is None):
        return

    for row in rows_iter:
        ok = True
        if cons["fax"] is not None:
            present = _has_value(row[fax_idx] if fax_idx is not None and fax_idx < len(row) else "")
            ok = ok and (present == cons["fax"])
        if ok and cons["phone"] is not None:
            present = _has_value(row[phone_idx] if phone_idx is not None and phone_idx < len(row) else "")
            ok = ok and (present == cons["phone"])
        if ok and cons["post"] is not None:
            present = _has_value(row[post_idx] if post_idx is not None and post_idx < len(row) else "")
            ok = ok and (present == cons["post"])
        if ok:
            yield row
