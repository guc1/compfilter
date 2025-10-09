"""Combinator: orchestrates filters and streaming read/write (CSV-safe)."""
import csv
import re
from pathlib import Path
from typing import Dict, List, Generator, Iterable, Tuple, Optional, Set, Any

from ..config import CSV_PATH, CSV_DELIMITER, CSV_ENCODING
from ..analysis import perform_analysis

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


_DUPLICATE_CACHE: Dict[str, Tuple[Tuple[Tuple[str, int, int], ...], Set[str]]] = {}
_KVK_CANDIDATES = {"kvk", "kvknummer", "kvknr", "kvknumber"}


def _normalize_column_name(name: str) -> str:
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]", "", str(name).lstrip("\ufeff").lower())


def _find_kvk_index(header: List[str]) -> Optional[int]:
    for idx, col in enumerate(header):
        normalized = _normalize_column_name(col)
        if normalized in _KVK_CANDIDATES or normalized.startswith("kvk"):
            return idx
    return None


def _folder_signature(files: List[Path]) -> Tuple[Tuple[str, int, int], ...]:
    sig: List[Tuple[str, int, int]] = []
    for f in files:
        stat = f.stat()
        sig.append((f.name, int(stat.st_size), int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))))
    return tuple(sig)


def _load_existing_kvk_numbers(folder: Path) -> Set[str]:
    resolved = folder.expanduser().resolve()
    if not resolved.exists():
        raise ValueError(f"Duplicates folder does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"Duplicates folder is not a directory: {resolved}")

    files = [p for p in resolved.iterdir() if p.is_file() and p.suffix.lower() == ".csv"]
    signature = _folder_signature(files)
    cached = _DUPLICATE_CACHE.get(str(resolved))
    if cached and cached[0] == signature:
        return set(cached[1])

    kvks: Set[str] = set()
    skipped: List[str] = []
    for csv_path in files:
        try:
            with csv_path.open("r", encoding=CSV_ENCODING, newline="") as handle:
                reader = csv.reader(handle, delimiter=CSV_DELIMITER)
                header = next(reader, None)
                if not header:
                    continue
                if header and header[0]:
                    header[0] = header[0].lstrip("\ufeff")
                idx = _find_kvk_index(header)
                if idx is None:
                    skipped.append(csv_path.name)
                    continue
                for row in reader:
                    if idx < len(row):
                        kvk = row[idx].strip()
                        if kvk:
                            kvks.add(kvk)
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            raise ValueError(f"Failed to read {csv_path.name}: {exc}")

    if skipped:
        print(f"[DUPLICATES] skipped {len(skipped)} file(s) without KVK column: {', '.join(skipped[:5])}")

    _DUPLICATE_CACHE[str(resolved)] = (signature, set(kvks))
    return kvks


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _apply_duplicate_filter(
    rows: Iterable[List[str]],
    header: List[str],
    folder: str,
) -> Iterable[List[str]]:
    folder_str = str(folder or "").strip()
    if not folder_str:
        raise ValueError("Provide a folder path to filter duplicates.")
    folder_path = Path(folder_str)
    kvk_idx = _find_kvk_index(header)
    if kvk_idx is None:
        raise ValueError("Could not find a KVK column in the source CSV.")
    existing = _load_existing_kvk_numbers(folder_path)

    def _iter():
        seen: Set[str] = set()
        for row in rows:
            kvk_val = row[kvk_idx].strip() if kvk_idx < len(row) else ""
            if kvk_val:
                if kvk_val in existing or kvk_val in seen:
                    continue
                seen.add(kvk_val)
            yield row

    return _iter()

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

def preview_count(
    selected_filters: Dict[str, List[str]],
    advanced: Optional[Dict[str, object]] = None,
) -> int:
    header, rows = _stream_rows(CSV_PATH)
    filtered = _apply_filters(rows, header, selected_filters, advanced)
    return sum(1 for _ in filtered)


def statistical_analysis(
    selected_filters: Dict[str, List[str]],
    advanced: Optional[Dict[str, object]] = None,
    dimensions: Optional[List[str]] = None,
) -> Dict[str, object]:
    header, rows = _stream_rows(CSV_PATH)
    filtered = _apply_filters(rows, header, selected_filters, advanced)
    dims = dimensions or []
    return perform_analysis(header, filtered, dims)

def _apply_filters(
    rows: Iterable[List[str]],
    header: List[str],
    selected_filters: Dict[str, List[str]],
    advanced: Optional[Dict[str, object]] = None,
) -> Iterable[List[str]]:
    filtered: Iterable[List[str]] = rows
    for k, selected in selected_filters.items():
        mod = FILTERS.get(k)
        if mod:
            filtered = mod.apply(filtered, header, selected)

    adv = advanced or {}
    filter_duplicates = (
        adv.get("filterDuplicates")
        if "filterDuplicates" in adv
        else adv.get("filter_duplicates")
    )
    if filter_duplicates is None:
        filter_duplicates = adv.get("filterDubs")
    if _coerce_bool(filter_duplicates):
        folder = (
            adv.get("duplicatesPath")
            or adv.get("duplicates_path")
            or adv.get("folderPath")
            or adv.get("folder_path")
            or adv.get("folder")
        )
        filtered = _apply_duplicate_filter(filtered, header, folder)
    return filtered


def stream_filtered_csv(
    selected_filters: Dict[str, List[str]],
    advanced: Optional[Dict[str, object]] = None,
) -> Generator[str, None, None]:
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
    filtered = _apply_filters(rows, header, selected_filters, advanced)

    # Rows
    for row in filtered:
        yield writer_line(row)


def _sanitize_basename(base_name: str) -> str:
    """Ensure the file stem is filesystem friendly."""
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._-")
    return stem or "results"


def save_filtered_csv_multi(
    selected_filters: Dict[str, List[str]],
    destinations: List[Dict[str, Any]],
    advanced: Optional[Dict[str, object]] = None,
) -> Tuple[List[Path], int, List[Dict[str, Any]]]:
    """Save filtered results into multiple destinations.

    Each destination dict must contain:
        directory: str or Path
        base_name: str
        max_rows_per_file: int
        rows_requested: Optional[int] (None => rest)
    """

    if not destinations:
        raise ValueError("At least one save destination is required")

    header, rows = _stream_rows(CSV_PATH)
    filtered = _apply_filters(rows, header, selected_filters, advanced)

    prepared: List[Dict[str, Any]] = []
    rest_index: Optional[int] = None

    for idx, raw in enumerate(destinations):
        directory_raw = raw.get("directory")
        base_name_raw = raw.get("base_name") or raw.get("baseName") or ""
        max_rows_raw = raw.get("max_rows_per_file") or raw.get("maxRowsPerFile")
        rows_raw = raw.get("rows_requested") or raw.get("rowsRequested")

        if isinstance(rows_raw, str) and rows_raw.strip().upper() == "R":
            rows_raw = None

        if directory_raw is None or str(directory_raw).strip() == "":
            raise ValueError(f"Destination {idx + 1}: directory is required")
        if max_rows_raw is None:
            raise ValueError(f"Destination {idx + 1}: max_rows_per_file is required")

        try:
            max_rows = int(max_rows_raw)
        except (TypeError, ValueError):
            raise ValueError(f"Destination {idx + 1}: max_rows_per_file must be an integer")
        if max_rows <= 0:
            raise ValueError(f"Destination {idx + 1}: max_rows_per_file must be greater than zero")

        try:
            directory_path = Path(directory_raw).expanduser().resolve()
        except Exception as exc:
            raise ValueError(f"Destination {idx + 1}: invalid directory: {exc}")
        directory_path.mkdir(parents=True, exist_ok=True)

        safe_base = _sanitize_basename(str(base_name_raw))

        is_rest = rows_raw is None
        if is_rest:
            if rest_index is not None:
                raise ValueError("Only one destination can use R (rest).")
            requested_rows: Optional[int] = None
        else:
            try:
                requested_rows = int(rows_raw)
            except (TypeError, ValueError):
                raise ValueError(f"Destination {idx + 1}: rows must be a positive integer or 'R'")
            if requested_rows <= 0:
                raise ValueError(f"Destination {idx + 1}: rows must be greater than zero")

        entry = {
            "directory": directory_path,
            "safe_base": safe_base,
            "max_rows": max_rows,
            "requested": requested_rows,
            "remaining": requested_rows,
            "file_index": 0,
            "handle": None,
            "writer": None,
            "current_count": 0,
            "files": [],
            "total_rows": 0,
        }
        if is_rest:
            rest_index = len(prepared)
        prepared.append(entry)

    def ensure_writer(entry: Dict[str, Any]):
        if entry["writer"] is None or entry["current_count"] >= entry["max_rows"]:
            if entry["handle"]:
                entry["handle"].close()
            entry["file_index"] += 1
            fname = f"{entry['safe_base']}{entry['file_index']}.csv"
            path = entry["directory"] / fname
            handle = path.open("w", encoding=CSV_ENCODING, newline="")
            handle.write("\ufeff")
            writer = csv.writer(
                handle,
                delimiter=CSV_DELIMITER,
                quotechar='"',
                lineterminator="\r\n",
                quoting=csv.QUOTE_MINIMAL,
                doublequote=True,
                escapechar=None,
            )
            writer.writerow(header)
            entry["files"].append(path)
            entry["handle"] = handle
            entry["writer"] = writer
            entry["current_count"] = 0

    total_rows_written = 0

    try:
        for row in filtered:
            destination_index: Optional[int] = None
            for idx, entry in enumerate(prepared):
                remaining = entry.get("remaining")
                if remaining is not None and remaining > 0:
                    destination_index = idx
                    break
            if destination_index is None:
                destination_index = rest_index
            if destination_index is None:
                raise ValueError(
                    "More rows produced than allocated. Increase the fixed amounts or add an R destination."
                )

            entry = prepared[destination_index]
            ensure_writer(entry)
            entry["writer"].writerow(row)
            entry["current_count"] += 1
            entry["total_rows"] += 1
            total_rows_written += 1
            if entry["remaining"] is not None:
                entry["remaining"] -= 1

        if total_rows_written == 0 and prepared:
            first = prepared[0]
            if not first["files"]:
                ensure_writer(first)

    finally:
        for entry in prepared:
            if entry["handle"]:
                entry["handle"].close()
                entry["handle"] = None
                entry["writer"] = None
                entry["current_count"] = 0

    all_files: List[Path] = []
    details: List[Dict[str, Any]] = []
    for entry in prepared:
        all_files.extend(entry["files"])
        details.append({
            "directory": str(entry["directory"]),
            "base_name": entry["safe_base"],
            "max_rows_per_file": entry["max_rows"],
            "requested_rows": entry["requested"],
            "mode": "rest" if entry["requested"] is None else "fixed",
            "rows_written": entry["total_rows"],
            "files": [str(p) for p in entry["files"]],
        })

    return all_files, total_rows_written, details


def save_filtered_csv(
    selected_filters: Dict[str, List[str]],
    directory: Path,
    base_name: str,
    max_rows_per_file: int,
    advanced: Optional[Dict[str, object]] = None,
) -> Tuple[List[Path], int]:
    """Backward-compatible single destination save."""

    files, total_rows, _ = save_filtered_csv_multi(
        selected_filters,
        [
            {
                "directory": directory,
                "base_name": base_name,
                "max_rows_per_file": max_rows_per_file,
                "rows_requested": None,
            }
        ],
        advanced,
    )
    return files, total_rows
