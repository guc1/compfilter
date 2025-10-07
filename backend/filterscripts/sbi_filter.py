"""Streaming filter for SBI code selections (main/sub/all).
Supports manual code entry and uploaded CSV lists (stored locally).
"""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

FILTER_KEY = "sbi"

MAIN_COL_CANDS = ["mainsbi", "main_sbi", "hoofd_sbi", "hoofdactiviteit"]
SUB_COL_CANDS = ["subsbi", "sub_sbi", "nevenactiviteiten", "nevensbi"]
ALL_COL_CANDS = ["allsbi", "all_sbi", "alle_sbi", "sbi_codes"]

BUCKETS = ("main", "sub", "all")

_DATA_DIR = Path(__file__).with_name("data") / "sbi_lists"
_CACHE: Dict[Tuple[str, str], Set[str]] = {}


def name() -> str:
    return FILTER_KEY


def distinct_values(*_a, **_k) -> List[str]:
    """UI is custom; no fixed options."""
    return []


def _bucket_dir(bucket: str) -> Path:
    if bucket not in BUCKETS:
        raise ValueError(f"Unknown bucket: {bucket}")
    path = _DATA_DIR / bucket
    path.mkdir(parents=True, exist_ok=True)
    return path


def _normalize_code(code: Optional[str]) -> Optional[str]:
    if code is None:
        return None
    s = str(code).strip()
    if not s:
        return None
    return s


def _parse_row_codes(cell: Optional[str]) -> Set[str]:
    if cell is None:
        return set()
    s = str(cell).strip()
    if not s:
        return set()
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1]
    else:
        inner = s
    if not inner:
        return set()
    parts = [p.strip().strip("'\"") for p in inner.split(",")]
    return {c for c in (_normalize_code(p) for p in parts) if c}


def _read_codes_from_upload(raw: str) -> List[str]:
    text = raw.strip("\ufeff\n\r \t")
    if not text:
        return []
    try:
        dialect = csv.Sniffer().sniff(text, delimiters=",;\t")
        delim = dialect.delimiter
    except Exception:
        delim = ","
    rdr = csv.reader(io.StringIO(text), delimiter=delim)
    codes: List[str] = []
    for row in rdr:
        if not row:
            continue
        cell = row[0]
        norm = _normalize_code(cell)
        if not norm:
            continue
        # Skip header row heuristically if it contains letters
        if not codes and any(ch.isalpha() for ch in cell):
            continue
        codes.append(norm)
    return codes


def list_uploaded_files() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for bucket in BUCKETS:
        folder = _bucket_dir(bucket)
        names = sorted(p.stem for p in folder.glob("*.txt"))
        out[bucket] = names
    return out


def save_uploaded_csv(bucket: str, original_name: str, raw_content: Union[str, bytes]) -> str:
    if isinstance(raw_content, bytes):
        content = raw_content.decode("utf-8-sig")
    else:
        content = raw_content
    codes = _read_codes_from_upload(content)
    if not codes:
        raise ValueError("No SBI codes found in uploaded file")

    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(original_name).stem).strip("._") or "sbi_list"
    folder = _bucket_dir(bucket)
    path = folder / f"{stem}.txt"
    data = "\n".join(dict.fromkeys(codes)) + "\n"
    path.write_text(data, encoding="utf-8")
    _CACHE.pop((bucket, stem), None)
    return stem


def _load_codes_from_file(bucket: str, stem: str) -> Set[str]:
    key = (bucket, stem)
    if key in _CACHE:
        return _CACHE[key]
    path = _bucket_dir(bucket) / f"{stem}.txt"
    if not path.exists():
        return set()
    codes = {_normalize_code(line) for line in path.read_text(encoding="utf-8").splitlines() if _normalize_code(line)}
    _CACHE[key] = codes
    return codes


def _find_col(header: List[str], candidates: List[str]) -> Optional[int]:
    lowered = [h.strip().lower() for h in header]
    for cand in candidates:
        c = cand.strip().lower()
        if c in lowered:
            return lowered.index(c)
    return None


def _normalize_selection(selected: Union[Dict[str, object], List[str], None]) -> Dict[str, Dict[str, Union[List[str], Optional[str]]]]:
    result = {
        "main": {"codes": [], "file": None},
        "sub": {"codes": [], "file": None},
        "all": {"codes": [], "file": None},
    }
    if isinstance(selected, dict):
        for bucket in BUCKETS:
            bucket_data = selected.get(bucket)
            if isinstance(bucket_data, dict):
                codes = bucket_data.get("codes", [])
                if isinstance(codes, list):
                    result[bucket]["codes"] = [c for c in (_normalize_code(v) for v in codes) if c]
                file_name = bucket_data.get("file")
                if isinstance(file_name, str) and _normalize_code(file_name):
                    result[bucket]["file"] = file_name
    elif isinstance(selected, list):
        for token in selected:
            if not isinstance(token, str):
                continue
            t = token.strip()
            if not t:
                continue
            if "=" in t:
                key, value = t.split("=", 1)
            elif ":" in t:
                key, value = t.split(":", 1)
            else:
                continue
            key = key.strip().lower()
            value = _normalize_code(value)
            if not value:
                continue
            if key in ("main", "sub", "all"):
                result[key]["codes"].append(value)
            elif key in ("main_file", "sub_file", "all_file"):
                bucket = key.split("_", 1)[0]
                result[bucket]["file"] = value
    return result


def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: Union[Dict[str, object], List[str], None]) -> Generator[List[str], None, None]:
    normalized = _normalize_selection(selected_values)

    active_buckets = {
        bucket: {
            "codes": {c for c in normalized[bucket]["codes"] if c},
            "file": normalized[bucket]["file"],
        }
        for bucket in BUCKETS
    }

    # Load codes from referenced files
    for bucket, data in active_buckets.items():
        file_label = data["file"]
        if file_label:
            data["codes"].update(_load_codes_from_file(bucket, file_label))

    if all(len(data["codes"]) == 0 for data in active_buckets.values()):
        yield from rows_iter
        return

    idx_main = _find_col(header, MAIN_COL_CANDS) if active_buckets["main"]["codes"] else None
    idx_sub = _find_col(header, SUB_COL_CANDS) if active_buckets["sub"]["codes"] else None
    idx_all = _find_col(header, ALL_COL_CANDS) if active_buckets["all"]["codes"] else None

    for row in rows_iter:
        ok = True
        if idx_main is not None:
            values = _parse_row_codes(row[idx_main] if idx_main < len(row) else "")
            ok = bool(values & active_buckets["main"]["codes"])
        if ok and idx_sub is not None:
            values = _parse_row_codes(row[idx_sub] if idx_sub < len(row) else "")
            ok = bool(values & active_buckets["sub"]["codes"])
        if ok and idx_all is not None:
            values = _parse_row_codes(row[idx_all] if idx_all < len(row) else "")
            ok = bool(values & active_buckets["all"]["codes"])
        if ok:
            yield row
