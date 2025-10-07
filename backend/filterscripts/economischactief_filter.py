"""Filter logic for 'economischactief' (TRUE/FALSE etc.)."""
import csv
from pathlib import Path
from typing import Iterable, List, Generator, Set

from ..config import CSV_DELIMITER, CSV_ENCODING

FILTER_KEY = "economischactief"

def name() -> str:
    return FILTER_KEY

def distinct_values(csv_path: Path) -> List[str]:
    """Stream the CSV to collect unique economischactief values (normalized)."""
    uniq: Set[str] = set()
    with csv_path.open("r", encoding=CSV_ENCODING, newline="") as f:
        rdr = csv.reader(f, delimiter=CSV_DELIMITER)
        header = next(rdr)
        header_norm = [h.strip().lower() for h in header]
        try:
            idx = header_norm.index("economischactief")
        except ValueError:
            return []
        for row in rdr:
            if idx < len(row):
                raw = (row[idx] or "").strip()
                v = raw if raw != "" else "UNKNOWN"
                uniq.add(v)
    # Keep common booleans first if present
    order_hint = {"TRUE": 0, "FALSE": 1, "Ja": 0, "Nee": 1, "1": 0, "0": 1}
    return sorted(uniq, key=lambda s: (100 if s not in order_hint else order_hint[s], s.lower()))

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    """Filter rows by economischactief ∈ selected_values. Empty selection => pass-through."""
    if not selected_values:
        yield from rows_iter
        return
    try:
        idx = [h.strip().lower() for h in header].index("economischactief")
    except ValueError:
        return

    selected = set(v.strip() for v in selected_values)

    for row in rows_iter:
        val = (row[idx] or "").strip()
        if val == "":
            val = "UNKNOWN"
        if val in selected:
            yield row
