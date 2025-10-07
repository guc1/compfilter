import csv
from pathlib import Path
from typing import Iterable, List, Generator, Set
from ..config import CSV_DELIMITER, CSV_ENCODING
FILTER_KEY = "rechtsvorm"
def name() -> str: return FILTER_KEY
def distinct_values(csv_path: Path) -> List[str]:
    uniq: Set[str] = set()
    with csv_path.open("r", encoding=CSV_ENCODING, newline="") as f:
        rdr = csv.reader(f, delimiter=CSV_DELIMITER)
        header = next(rdr)
        try:
            idx = [h.strip().lower() for h in header].index("rechtsvorm")
        except ValueError:
            return []
        for row in rdr:
            if idx < len(row):
                v = (row[idx] or "").strip()
                uniq.add(v if v else "UNKNOWN")
    return sorted(uniq, key=lambda s: (s == "UNKNOWN", s.lower()))
def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    if not selected_values:
        yield from rows_iter
        return
    try:
        idx = [h.strip().lower() for h in header].index("rechtsvorm")
    except ValueError:
        return
    selected = set(v.strip() for v in selected_values)
    for row in rows_iter:
        val = (row[idx] or "").strip() or "UNKNOWN"
        if val in selected:
            yield row
