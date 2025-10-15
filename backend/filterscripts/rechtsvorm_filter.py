import csv
from pathlib import Path
from typing import Iterable, List, Generator, Set

from ..config import CSV_DELIMITER, CSV_ENCODING
from .runtime import FilterContext
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
    context = FilterContext.from_header(header)
    yield from apply_with_context(rows_iter, header, selected_values, context)


def apply_with_context(
    rows_iter: Iterable[List[str]],
    header: List[str],
    selected_values: List[str],
    context: FilterContext,
) -> Iterable[List[str]]:
    if not selected_values:
        return rows_iter

    idx = context.index_for("rechtsvorm")
    if idx is None:
        return []

    selected = { (v or "").strip() or "UNKNOWN" for v in selected_values if isinstance(v, str) }
    if not selected:
        return rows_iter

    def _iter() -> Generator[List[str], None, None]:
        for row in rows_iter:
            raw = row[idx] if idx < len(row) else ""
            val = (raw or "").strip() or "UNKNOWN"
            if val in selected:
                yield row

    return _iter()
