"""Utilities for campaign tracking CSV generation and metadata."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from backend.filterscripts import combinator
from backend.config import CSV_DELIMITER, CSV_ENCODING

REPO_ROOT = Path(__file__).resolve().parents[1]
TRACKING_DIR = REPO_ROOT / "bigdata" / "tracking"
LATEST_CAMPAIGN_PATH = TRACKING_DIR / "latest_campaign.json"


def ensure_tracking_dir() -> Path:
    TRACKING_DIR.mkdir(parents=True, exist_ok=True)
    return TRACKING_DIR


def load_latest_campaign() -> Optional[Dict]:
    if not LATEST_CAMPAIGN_PATH.exists():
        return None
    try:
        with LATEST_CAMPAIGN_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def save_latest_campaign(data: Dict) -> None:
    ensure_tracking_dir()
    payload = {
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        **{k: v for k, v in data.items() if k != "timestamp"},
    }
    with LATEST_CAMPAIGN_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def default_tracking_path(directory: str | Path, base_name: str) -> Path:
    directory_path = Path(directory).expanduser()
    stem = f"{base_name}_tracking" if base_name else "campaign_tracking"
    return (directory_path / f"{stem}.csv").resolve()


def _format_list(values: Sequence[str]) -> str:
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    inner = ", ".join(f"'{item}'" for item in cleaned)
    return f"[{inner}]"


def _format_optional(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_duplicates_folder(advanced: Dict) -> str:
    keys = [
        "duplicatesPath",
        "duplicates_path",
        "folderPath",
        "folder_path",
        "folder",
    ]
    for key in keys:
        val = advanced.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _generate_unique_id(existing: set[str]) -> str:
    while True:
        candidate = "".join(random.choices("0123456789", k=8))
        if candidate not in existing:
            existing.add(candidate)
            return candidate


@dataclass
class FilterColumns:
    economischactief: str = ""
    rechtsvorm: str = ""
    location: str = ""
    working_min: str = ""
    working_max: str = ""
    contactpersoon: str = ""
    media: str = ""
    traditional_outreach: str = ""
    vestiging_gebruiksdoel: str = ""
    vestiging_hoofdvestiging: str = ""
    vestiging_kvk_non_mailing: str = ""
    vestiging_oppervlakte_min: str = ""
    vestiging_oppervlakte_max: str = ""
    overige_date_min: str = ""
    overige_date_max: str = ""
    overige_tradenames: str = ""
    sbi_main_codes: str = ""
    sbi_main_file: str = ""
    sbi_sub_codes: str = ""
    sbi_sub_file: str = ""
    sbi_all_codes: str = ""
    sbi_all_file: str = ""

    @classmethod
    def from_selected(cls, selected: Dict) -> "FilterColumns":
        sel = selected or {}
        economischactief = _format_list(sel.get("economischactief", []))
        rechtsvorm = _format_list(sel.get("rechtsvorm", []))
        location = _format_list(sel.get("location", []))
        contactpersoon = _format_list(sel.get("contactpersoon", []))
        media = _format_list(sel.get("media", []))
        traditional_outreach = _format_list(sel.get("traditional_outreach", []))

        working_min = ""
        working_max = ""
        working = sel.get("workingnumber")
        if isinstance(working, (list, tuple)) and working:
            working_min = _format_optional(working[0] if len(working) > 0 else "")
            working_max = _format_optional(working[1] if len(working) > 1 else "")

        vestiging_tokens = sel.get("vestiging") or []
        vg = []
        hv = []
        nm = []
        oppmin = ""
        oppmax = ""
        for token in vestiging_tokens:
            token = str(token)
            if token.startswith("gd="):
                vg.append(token.split("=", 1)[1])
            elif token.startswith("hv="):
                hv.append(token.split("=", 1)[1])
            elif token.startswith("nm="):
                nm.append(token.split("=", 1)[1])
            elif token.startswith("oppmin="):
                oppmin = token.split("=", 1)[1]
            elif token.startswith("oppmax="):
                oppmax = token.split("=", 1)[1]
        vestiging_gebruiksdoel = _format_list(vg)
        vestiging_hoofdvestiging = _format_list(hv)
        vestiging_kvk_non_mailing = _format_list(nm)
        vestiging_oppervlakte_min = oppmin
        vestiging_oppervlakte_max = oppmax

        overige_tokens = sel.get("overige") or []
        date_min = ""
        date_max = ""
        tradenames = []
        for token in overige_tokens:
            token = str(token)
            if token.startswith("date_min="):
                date_min = token.split("=", 1)[1]
            elif token.startswith("date_max="):
                date_max = token.split("=", 1)[1]
            elif token.startswith("tn="):
                tradenames.append(token.split("=", 1)[1])
        overige_date_min = date_min
        overige_date_max = date_max
        overige_tradenames = _format_list(tradenames)

        def parse_sbi(bucket: Dict | None) -> Tuple[str, str]:
            if not isinstance(bucket, dict):
                return "", ""
            codes_raw = bucket.get("codes")
            if isinstance(codes_raw, (list, tuple, set)):
                codes = _format_list(list(codes_raw))
            else:
                codes = ""
            file_val = bucket.get("file")
            file_name = str(file_val).strip() if isinstance(file_val, str) else ""
            return codes, file_name

        sbi_sel = sel.get("sbi") or {}
        sbi_main_codes, sbi_main_file = parse_sbi(sbi_sel.get("main"))
        sbi_sub_codes, sbi_sub_file = parse_sbi(sbi_sel.get("sub"))
        sbi_all_codes, sbi_all_file = parse_sbi(sbi_sel.get("all"))

        return cls(
            economischactief=economischactief,
            rechtsvorm=rechtsvorm,
            location=location,
            working_min=working_min,
            working_max=working_max,
            contactpersoon=contactpersoon,
            media=media,
            traditional_outreach=traditional_outreach,
            vestiging_gebruiksdoel=vestiging_gebruiksdoel,
            vestiging_hoofdvestiging=vestiging_hoofdvestiging,
            vestiging_kvk_non_mailing=vestiging_kvk_non_mailing,
            vestiging_oppervlakte_min=vestiging_oppervlakte_min,
            vestiging_oppervlakte_max=vestiging_oppervlakte_max,
            overige_date_min=overige_date_min,
            overige_date_max=overige_date_max,
            overige_tradenames=overige_tradenames,
            sbi_main_codes=sbi_main_codes,
            sbi_main_file=sbi_main_file,
            sbi_sub_codes=sbi_sub_codes,
            sbi_sub_file=sbi_sub_file,
            sbi_all_codes=sbi_all_codes,
            sbi_all_file=sbi_all_file,
        )


TRACKING_COLUMNS: Tuple[str, ...] = (
    "id",
    "kvknumber",
    "campaign",
    "subcampaign",
    "economischactief",
    "rechtsvorm",
    "location",
    "working_min",
    "working_max",
    "contactpersoon",
    "media",
    "traditional_outreach",
    "vestiging_gebruiksdoel",
    "vestiging_hoofdvestiging",
    "vestiging_kvk_non_mailing",
    "vestiging_oppervlakte_min",
    "vestiging_oppervlakte_max",
    "overige_date_min",
    "overige_date_max",
    "overige_tradenames",
    "sbi_main_codes",
    "sbi_main_file",
    "sbi_sub_codes",
    "sbi_sub_file",
    "sbi_all_codes",
    "sbi_all_file",
    "thruai",
    "message",
    "message_send",
    "status",
    "website_text",
    "before",
)


def _read_existing_tracking(path: Path) -> Tuple[List[Dict[str, str]], set[str]]:
    if not path.exists():
        return [], set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=CSV_DELIMITER)
        header = next(reader, None)
        if not header:
            return [], set()
        if header and header[0]:
            header[0] = header[0].lstrip("\ufeff")
        rows: List[Dict[str, str]] = []
        used_ids: set[str] = set()
        for row in reader:
            data: Dict[str, str] = {}
            for idx, col in enumerate(header):
                if idx < len(row):
                    data[col] = row[idx]
                else:
                    data[col] = ""
            if "id" in data and data["id"]:
                used_ids.add(data["id"])
            rows.append(data)
        return rows, used_ids


def _write_tracking_rows(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    ensure_tracking_dir()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=CSV_ENCODING, newline="") as handle:
        handle.write("\ufeff")
        writer = csv.writer(
            handle,
            delimiter=CSV_DELIMITER,
            quotechar='"',
            lineterminator="\r\n",
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
        )
        writer.writerow(TRACKING_COLUMNS)
        for row in rows:
            writer.writerow([row.get(col, "") for col in TRACKING_COLUMNS])


def record_campaign_run(selected: Dict, advanced: Dict, destinations: List[Dict]) -> None:
    if not destinations:
        return
    first = destinations[0]
    payload = {
        "selected": selected or {},
        "advanced": advanced or {},
        "directory": first.get("directory", ""),
        "base_name": first.get("base_name") or first.get("baseName") or first.get("safe_base", ""),
        "destinations": destinations,
    }
    save_latest_campaign(payload)


def create_or_update_tracking_csv(
    *,
    selected: Dict,
    advanced: Dict,
    campaign_directory: str | Path,
    subcampaign_base: str,
    mode: str,
    target_path: str | Path | None = None,
) -> Dict[str, object]:
    header, rows_iter = combinator.iter_filtered_rows(selected or {}, advanced or {})
    kvk_idx = combinator._find_kvk_index(header)
    if kvk_idx is None:
        raise ValueError("Could not find a KVK column in the source CSV.")

    campaign_dir = Path(campaign_directory or "").expanduser()
    if not str(campaign_dir):
        raise ValueError("Campaign directory is required. Run a custom save first.")

    base_name = (subcampaign_base or "").strip()
    if not base_name:
        raise ValueError("Base filename is required to derive campaign metadata.")

    mode_norm = (mode or "create").strip().lower()
    if target_path:
        target = Path(target_path).expanduser().resolve()
    else:
        target = default_tracking_path(campaign_dir, base_name)

    existing_rows, existing_ids = _read_existing_tracking(target)
    existing_kvks: set[str] = set()
    for row in existing_rows:
        kvk = (row.get("kvknumber") or row.get("kvknummer") or "").strip()
        if kvk:
            existing_kvks.add(kvk)
            if not row.get("kvknumber"):
                row["kvknumber"] = kvk
        if not row.get("id"):
            row["id"] = _generate_unique_id(existing_ids)

    if mode_norm == "update" and not target.exists() and not existing_rows:
        raise ValueError("Tracking CSV does not exist yet. Use create to generate it first.")

    filters = FilterColumns.from_selected(selected)
    duplicates_folder_raw = _extract_duplicates_folder(advanced or {})
    duplicates_folder_resolved = ""
    duplicates_lookup: Optional[set[str]] = None
    if duplicates_folder_raw:
        folder_path = Path(duplicates_folder_raw).expanduser()
        duplicates_lookup = combinator._load_existing_kvk_numbers(folder_path)
        duplicates_folder_resolved = str(folder_path.resolve())

    seen_kvk: set[str] = set()
    rows_data: List[Dict[str, str]] = []
    for row in rows_iter:
        if kvk_idx >= len(row):
            continue
        kvk = (row[kvk_idx] or "").strip()
        if not kvk or kvk in seen_kvk:
            continue
        seen_kvk.add(kvk)
        previously_used = False
        if duplicates_lookup is not None and kvk in duplicates_lookup:
            previously_used = True
        elif kvk in existing_kvks:
            previously_used = True
        before_val = "TRUE" if previously_used else "FALSE"
        rows_data.append({
            "kvknumber": kvk,
            "campaign": campaign_dir.name,
            "subcampaign": base_name,
            "economischactief": filters.economischactief,
            "rechtsvorm": filters.rechtsvorm,
            "location": filters.location,
            "working_min": filters.working_min,
            "working_max": filters.working_max,
            "contactpersoon": filters.contactpersoon,
            "media": filters.media,
            "traditional_outreach": filters.traditional_outreach,
            "vestiging_gebruiksdoel": filters.vestiging_gebruiksdoel,
            "vestiging_hoofdvestiging": filters.vestiging_hoofdvestiging,
            "vestiging_kvk_non_mailing": filters.vestiging_kvk_non_mailing,
            "vestiging_oppervlakte_min": filters.vestiging_oppervlakte_min,
            "vestiging_oppervlakte_max": filters.vestiging_oppervlakte_max,
            "overige_date_min": filters.overige_date_min,
            "overige_date_max": filters.overige_date_max,
            "overige_tradenames": filters.overige_tradenames,
            "sbi_main_codes": filters.sbi_main_codes,
            "sbi_main_file": filters.sbi_main_file,
            "sbi_sub_codes": filters.sbi_sub_codes,
            "sbi_sub_file": filters.sbi_sub_file,
            "sbi_all_codes": filters.sbi_all_codes,
            "sbi_all_file": filters.sbi_all_file,
            "thruai": "",
            "message": "",
            "message_send": "",
            "status": "",
            "website_text": "",
            "before": before_val,
        })

    if not rows_data:
        raise ValueError("No rows found for the current selection.")

    all_rows: List[Dict[str, str]] = list(existing_rows)
    for payload in rows_data:
        payload["id"] = _generate_unique_id(existing_ids)
        all_rows.append(payload)

    ordered_rows = sorted(
        all_rows,
        key=lambda item: (
            item.get("kvknumber") or "",
            item.get("id") or "",
        ),
    )
    _write_tracking_rows(target, ordered_rows)

    return {
        "path": str(target),
        "rows": len(ordered_rows),
        "new_rows": len(rows_data),
        "duplicates_folder": duplicates_folder_resolved,
    }
