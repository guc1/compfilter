"""Combinator: orchestrates filters and streaming read/write (CSV-safe)."""
import csv
from pathlib import Path
from typing import Dict, List, Generator, Iterable

from ..config import CSV_PATH, CSV_DELIMITER, CSV_ENCODING

# Import every filter module here
from . import (
    rechtsvorm_filter,
    location_filter,
    economischactief_filter,
    workingnumber_filter,
    contactpersoon_filter,
    media_filter,
    traditional_outreach_filter,
    vestiging_filter,
    overige_filter,
    sbi_filter,
)

# Register filters in the order you want them shown
FILTERS: Dict[str, object] = {
    economischactief_filter.name(): economischactief_filter,
    rechtsvorm_filter.name(): rechtsvorm_filter,
    workingnumber_filter.name(): workingnumber_filter,
    location_filter.name(): location_filter,
    contactpersoon_filter.name(): contactpersoon_filter,
    media_filter.name(): media_filter,
    traditional_outreach_filter.name(): traditional_outreach_filter,
    vestiging_filter.name(): vestiging_filter,
    overige_filter.name(): overige_filter,
    sbi_filter.name(): sbi_filter,
}

# Display meta
FILTER_META: Dict[str, Dict[str, str]] = {
    "economischactief":     {"label": "Economisch actief",   "type": "multiselect"},
    "rechtsvorm":           {"label": "Rechtsvorm",          "type": "multiselect"},
    "location":            {"label": "Location",            "type": "multiselect"},
    "workingnumber":        {"label": "Working number",      "type": "number"},
    "contactpersoon":       {"label": "Contact persoon",     "type": "multiselect"},
    "media":                {"label": "Media",               "type": "multiselect"},
    "traditional_outreach": {"label": "Traditional outreach","type": "multiselect"},
    "vestiging":            {"label": "Vestiging",           "type": "group"},
    "overige":              {"label": "Overige",             "type": "group"},
    "sbi":                  {"label": "SBI",                "type": "sbi"},
}

def list_filters() -> List[Dict]:
    out: List[Dict] = []
    for key in FILTERS.keys():
        meta = FILTER_META.get(key, {"label": key.capitalize(), "type": "multiselect"})
        out.append({"key": key, "label": meta["label"], "type": meta["type"]})
    return out

def get_filter_options() -> Dict[str, List[str]]:
    """Ask each module for options when it makes sense."""
    opts: Dict[str, List[str]] = {}
    for k, mod in FILTERS.items():
        ftype = FILTER_META.get(k, {}).get("type", "multiselect")
        if hasattr(mod, "distinct_values"):
            # For multiselect/group we expose their discrete values (if any)
            try:
                opts[k] = mod.distinct_values(CSV_PATH)
            except TypeError:
                opts[k] = mod.distinct_values()
        else:
            opts[k] = []
    return opts

def _stream_rows(csv_path: Path):
    f = csv_path.open("r", encoding=CSV_ENCODING, newline="")
    rdr = csv.reader(f, delimiter=CSV_DELIMITER)
    header = next(rdr)
    def _iter():
        try:
            for row in rdr:
                yield row
        finally:
            f.close()
    return header, _iter()

def preview_count(selected_filters: Dict[str, List[str]]) -> int:
    header, rows = _stream_rows(CSV_PATH)
    filtered: Iterable[List[str]] = rows
    for k, selected in selected_filters.items():
        mod = FILTERS.get(k)
        if mod:
            filtered = mod.apply(filtered, header, selected)
    return sum(1 for _ in filtered)

def stream_filtered_csv(selected_filters: Dict[str, List[str]]) -> Generator[str, None, None]:
    """Yield properly quoted CSV lines with CRLF line endings and a UTF-8 BOM."""
    import io
    header, rows = _stream_rows(CSV_PATH)

    def writer_line(row: List[str]) -> str:
        buf = io.StringIO()
        w = csv.writer(
            buf,
            delimiter=CSV_DELIMITER,
            quotechar='"',
            lineterminator="\r\n",
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
            escapechar=None,
        )
        w.writerow(row)
        return buf.getvalue()

    # BOM
    yield "\ufeff"
    # Header
    yield writer_line(header)

    # Apply filters (AND)
    filtered: Iterable[List[str]] = rows
    for k, selected in selected_filters.items():
        mod = FILTERS.get(k)
        if mod:
            filtered = mod.apply(filtered, header, selected)

    # Rows
    for row in filtered:
        yield writer_line(row)
