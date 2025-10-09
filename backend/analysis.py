"""On-demand statistical analysis for filtered CSV rows.

This module streams the filtered dataset, calculates the same signals as
the offline aggregation script, and compares the outcome with reference
CSV summaries stored inside the local ``bigdata`` directory.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .config import CSV_DELIMITER, CSV_ENCODING
from .filterscripts import location_filter

# Paths ---------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
BIGDATA_DIR = REPO_ROOT / "bigdata"
SUMMARY_FILE = BIGDATA_DIR / "per_rechtsvorm_summary.csv"
PROVINCE_FILE = BIGDATA_DIR / "per_rechtsvorm_province.csv"
SBI_FILE = BIGDATA_DIR / "per_rechtsvorm_sbi.csv"


# Constants -----------------------------------------------------------------

MEDIA_FIELDS: Sequence[str] = (
    "email",
    "facebook",
    "instagram",
    "linkedin",
    "pinterest",
    "twitter",
    "youtube",
    "internetaddress",
)

GEBRUIKSDOEL_BUCKETS: Sequence[str] = (
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
    "unknown",
    "celfunctie",
)

PROVINCES: Sequence[str] = (
    "Drenthe",
    "Flevoland",
    "Fryslân",
    "Gelderland",
    "Groningen",
    "Limburg",
    "Noord-Brabant",
    "Noord-Holland",
    "Overijssel",
    "Utrecht",
    "Zeeland",
    "Zuid-Holland",
)


# Mapping of metric keys to percentage columns in the baseline summary.
SUMMARY_PCT_COLUMNS: Dict[str, str] = {
    "contactpersoon": "contactpersoon_pct",
    "economischactief_true": "economischactief_true_pct",
    "phone": "phone_pct",
    "fax": "fax_pct",
}
SUMMARY_PCT_COLUMNS.update({field: f"{field}_pct" for field in MEDIA_FIELDS})
for bucket in GEBRUIKSDOEL_BUCKETS:
    safe = bucket.replace(" ", "_")
    SUMMARY_PCT_COLUMNS[f"gebruiksdoel_{safe}"] = f"gebruiksdoel_{bucket}_pct"
# Explicit fix for the header that contains a space in the column name.
SUMMARY_PCT_COLUMNS["gebruiksdoel_overige_gebruiksfunctie"] = "gebruiksdoel_overige gebruiksfunctie_pct"


AVERAGE_COLUMNS: Sequence[Tuple[str, str, str]] = (
    ("oppervlakte_avg_m2", "Gemiddelde oppervlakte (m²)", "m²"),
    ("working_min_avg", "Gemiddeld minimum aantal werknemers", ""),
    ("working_max_avg", "Gemiddeld maximum aantal werknemers", ""),
)


# Helper lookups -------------------------------------------------------------


def _sanitize_column(name: str) -> str:
    slug = name.strip().lower()
    slug = slug.replace(" ", "_").replace("-", "_")
    slug = re.sub(r"[^a-z0-9_]+", "", slug)
    return slug


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _has_value(cell: Optional[str]) -> bool:
    if cell is None:
        return False
    s = str(cell).strip()
    if not s:
        return False
    return s not in {"[]", "{}", "null", "None", "NULL", "none"}


def _truthy(cell: Optional[str]) -> bool:
    if cell is None:
        return False
    return str(cell).strip().lower() in {"true", "1", "yes", "ja", "y", "t"}


def _to_int(cell: Optional[str]) -> Optional[int]:
    try:
        return int(str(cell).strip())
    except (TypeError, ValueError):
        try:
            return int(float(str(cell).strip()))
        except (TypeError, ValueError):
            return None


def _to_float(cell: Optional[str]) -> Optional[float]:
    try:
        return float(str(cell).strip())
    except (TypeError, ValueError):
        return None


def _parse_date_any(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None

    iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if iso:
        y, m, d = map(int, iso.groups())
        try:
            return date(y, m, d)
        except ValueError:
            return None

    compact = re.match(r"^(\d{4})(\d{2})(\d{2})$", text)
    if compact:
        y, m, d = map(int, compact.groups())
        try:
            return date(y, m, d)
        except ValueError:
            return None

    nl_months = {
        "januari": 1,
        "februari": 2,
        "maart": 3,
        "april": 4,
        "mei": 5,
        "juni": 6,
        "juli": 7,
        "augustus": 8,
        "september": 9,
        "oktober": 10,
        "november": 11,
        "december": 12,
    }
    match = re.match(r"^(\d{1,2})\s+([A-Za-zÀ-ÿ\-]+)\s+(\d{4})$", text.lower())
    if match:
        d = int(match.group(1))
        month = nl_months.get(match.group(2))
        y = int(match.group(3))
        if month:
            try:
                return date(y, month, d)
            except ValueError:
                return None
    return None


def _parse_sbi_cell(cell: Optional[str]) -> List[str]:
    if not cell:
        return []
    s = str(cell).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        vals = re.findall(r"[0-9A-Za-z]+", s)
        return [v for v in vals if v]
    if "," in s:
        return [part.strip() for part in s.split(",") if part.strip()]
    return [s]


def _normalize_province_key(name: str) -> str:
    base = unicodedata.normalize("NFKD", name)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = base.replace("-", " ").replace("'", "")
    base = re.sub(r"\s+", " ", base)
    return base


PROVINCE_KEY_MAP = {_normalize_province_key(p): p for p in PROVINCES}


def _canonical_province(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return PROVINCE_KEY_MAP.get(_normalize_province_key(name))


def _find_index(header: Sequence[str], candidates: Sequence[str]) -> Optional[int]:
    normalized = {str(col).strip().lower(): idx for idx, col in enumerate(header)}
    for cand in candidates:
        if cand in normalized:
            return normalized[cand]
    return None


# Province locator -----------------------------------------------------------


class ProvinceLocator:
    """Resolve provinces using the cached geometries from the location filter."""

    def __init__(self, header: Sequence[str], warnings: List[str]):
        self._warnings = warnings
        self._lon_idx = _find_index(header, location_filter.LON_CANDS)
        self._lat_idx = _find_index(header, location_filter.LAT_CANDS)
        self._tree = None
        self._geoms: List = []
        self._names: List[str] = []
        self._point_cls = None
        self.enabled = False

        if self._lon_idx is None or self._lat_idx is None:
            warnings.append("Latitude/longitude columns missing — province breakdown skipped.")
            return

        try:
            location_filter.distinct_values()
            from shapely.geometry import Point
            from shapely.strtree import STRtree

            self._geoms = list(getattr(location_filter, "_PROV_GEOMS", []))
            self._names = list(getattr(location_filter, "_PROV_NAMES", []))
            if not self._geoms or not self._names:
                warnings.append("Province polygons unavailable — province breakdown skipped.")
                return
            self._tree = STRtree(self._geoms)
            self._point_cls = Point
            self.enabled = True
        except Exception as exc:  # pragma: no cover - depends on shapely availability
            warnings.append(f"Shapely unavailable ({exc}) — province breakdown skipped.")

    def resolve(self, row: Sequence[str]) -> Optional[str]:
        if not self.enabled or self._tree is None or self._point_cls is None:
            return None
        try:
            lon = float(row[self._lon_idx])
            lat = float(row[self._lat_idx])
        except (ValueError, TypeError, IndexError):
            return None
        try:
            pt = self._point_cls(lon, lat)
        except Exception:
            return None
        try:
            hits = self._tree.query(pt)
            geoms = self._tree.geometries
            for h in hits:
                idx = int(h)
                poly = geoms[idx]
                try:
                    if poly.intersects(pt):
                        name = self._names[idx]
                        return _canonical_province(name) or name
                except Exception:
                    continue
        except Exception:
            return None
        return None


# Streaming aggregator -------------------------------------------------------


class StreamingAnalyzer:
    """Collect statistics over streamed rows."""

    COLUMN_CANDIDATES: Dict[str, Sequence[str]] = {
        "rechtsvorm": ("rechtsvorm", "legal_form", "rechts_vorm"),
        "economischactief": ("economischactief", "is_economisch_actief"),
        "contactpersoon": ("contactpersoon", "contact_person", "contacten", "contactperson"),
        "kvk": ("kvk", "kvknummer", "kvk_nummer", "kvk-nummer"),
        "vestiging": ("vestigingnummer", "vestiging_nummer", "vnr", "vestiging_id"),
        "email": ("email", "e-mail", "mail"),
        "facebook": ("facebook",),
        "instagram": ("instagram",),
        "linkedin": ("linkedin",),
        "pinterest": ("pinterest",),
        "twitter": ("twitter", "x_twitter"),
        "youtube": ("youtube", "you_tube"),
        "internetaddress": ("internetaddress", "website", "homepage", "internetadres", "url"),
        "phone": ("phonenumber_formatted", "phone", "phone_number", "telefoon", "telefoonnummer"),
        "fax": ("faxnumber_formatted", "fax", "faxnummer"),
        "gebruiksdoel": ("gebruiksdoelverblijfsobject", "gebruiksdoel", "gebruiksfunctie"),
        "oppervlakte": ("oppervlakteverblijfsobject", "oppervlakte", "area_m2"),
        "working_min": ("workingminimum", "working_minimum", "min_employees", "werknemers_min"),
        "working_max": ("workingmaximum", "working_maximum", "max_employees", "werknemers_max"),
        "oprichtingsdatum": ("oprichtingsdatum", "foundationdate", "oprichting", "datum_oprichting", "foundation_date"),
        "allsbi": ("allsbi", "sbi_all", "all_sbi", "sbicodes", "sbi_codes", "sbi"),
    }

    def __init__(self, header: Sequence[str]):
        self.header = list(header)
        self.total_rows = 0
        self.contact_count = 0
        self.econ_true = 0
        self.media_counts = {field: 0 for field in MEDIA_FIELDS}
        self.phone_count = 0
        self.fax_count = 0
        self.gebruiks_counts: Counter[str] = Counter()
        self.surface_sum = 0.0
        self.surface_n = 0
        self.wmin_sum = 0
        self.wmin_n = 0
        self.wmax_sum = 0
        self.wmax_n = 0
        self.date_sum = 0
        self.date_n = 0
        self.kvk_counts: Dict[str, int] = defaultdict(int)
        self.rechtsvorm_counts: Counter[str] = Counter()
        self.province_counts: Counter[str] = Counter()
        self.sbi_counts: Counter[str] = Counter()
        self.warnings: List[str] = []

        self.indices: Dict[str, Optional[int]] = {
            key: _find_index(self.header, cands)
            for key, cands in self.COLUMN_CANDIDATES.items()
        }
        self.province_locator = ProvinceLocator(self.header, self.warnings)

    # ------------------------------------------------------------------
    # Row consumption
    # ------------------------------------------------------------------

    def consume(self, row: Sequence[str]) -> None:
        self.total_rows += 1

        rv_idx = self.indices.get("rechtsvorm")
        rv = (
            row[rv_idx].strip() if rv_idx is not None and rv_idx < len(row) and row[rv_idx] else "UNKNOWN"
        )
        if not rv:
            rv = "UNKNOWN"
        self.rechtsvorm_counts[rv] += 1

        contact_idx = self.indices.get("contactpersoon")
        if contact_idx is not None and contact_idx < len(row) and _has_value(row[contact_idx]):
            val = str(row[contact_idx]).strip()
            if val not in {"[]", "{}"}:
                self.contact_count += 1

        econ_idx = self.indices.get("economischactief")
        if econ_idx is not None and econ_idx < len(row) and _truthy(row[econ_idx]):
            self.econ_true += 1

        for field in MEDIA_FIELDS:
            idx = self.indices.get(field)
            if idx is not None and idx < len(row) and _has_value(row[idx]):
                self.media_counts[field] += 1

        phone_idx = self.indices.get("phone")
        if phone_idx is not None and phone_idx < len(row) and _has_value(row[phone_idx]):
            self.phone_count += 1

        fax_idx = self.indices.get("fax")
        if fax_idx is not None and fax_idx < len(row) and _has_value(row[fax_idx]):
            self.fax_count += 1

        gd_idx = self.indices.get("gebruiksdoel")
        if gd_idx is not None and gd_idx < len(row):
            gd_raw = str(row[gd_idx]).strip().lower()
            bucket = gd_raw if gd_raw in GEBRUIKSDOEL_BUCKETS else ("unknown" if gd_raw else "unknown")
            self.gebruiks_counts[bucket] += 1

        opp_idx = self.indices.get("oppervlakte")
        if opp_idx is not None and opp_idx < len(row):
            area = _to_float(row[opp_idx])
            if area is not None and area > 0:
                self.surface_sum += area
                self.surface_n += 1

        wmin_idx = self.indices.get("working_min")
        if wmin_idx is not None and wmin_idx < len(row):
            v = _to_int(row[wmin_idx])
            if v is not None:
                self.wmin_sum += v
                self.wmin_n += 1

        wmax_idx = self.indices.get("working_max")
        if wmax_idx is not None and wmax_idx < len(row):
            v = _to_int(row[wmax_idx])
            if v is not None:
                if v == 999_999_999:
                    v = 0
                self.wmax_sum += v
                self.wmax_n += 1

        date_idx = self.indices.get("oprichtingsdatum")
        if date_idx is not None and date_idx < len(row):
            dt = _parse_date_any(row[date_idx])
            if dt:
                self.date_sum += dt.toordinal()
                self.date_n += 1

        kvk_idx = self.indices.get("kvk")
        if kvk_idx is not None and kvk_idx < len(row):
            kvk = row[kvk_idx].strip()
            if kvk:
                self.kvk_counts[kvk] += 1

        province = self.province_locator.resolve(row)
        if province:
            self.province_counts[province] += 1

        sbi_idx = self.indices.get("allsbi")
        if sbi_idx is not None and sbi_idx < len(row):
            codes = _parse_sbi_cell(row[sbi_idx])
            for code in codes:
                if code:
                    self.sbi_counts[code] += 1

    # ------------------------------------------------------------------
    # Final result
    # ------------------------------------------------------------------

    def finalize(self) -> Dict[str, object]:
        total = self.total_rows
        pct = (lambda count: round(100.0 * count / total, 6) if total else 0.0)

        metrics: Dict[str, Dict[str, float]] = {}

        metrics["contactpersoon"] = {"abs": self.contact_count, "pct": pct(self.contact_count)}
        metrics["economischactief_true"] = {"abs": self.econ_true, "pct": pct(self.econ_true)}

        for field in MEDIA_FIELDS:
            count = self.media_counts[field]
            metrics[field] = {"abs": count, "pct": pct(count)}

        metrics["phone"] = {"abs": self.phone_count, "pct": pct(self.phone_count)}
        metrics["fax"] = {"abs": self.fax_count, "pct": pct(self.fax_count)}

        for bucket in GEBRUIKSDOEL_BUCKETS:
            safe = bucket.replace(" ", "_")
            count = self.gebruiks_counts.get(bucket, 0)
            metrics[f"gebruiksdoel_{safe}"] = {"abs": count, "pct": pct(count)}

        unique_kvk = len(self.kvk_counts)
        multi_kvk = sum(1 for val in self.kvk_counts.values() if val > 1)
        multi_pct = round(100.0 * multi_kvk / unique_kvk, 6) if unique_kvk else 0.0

        averages: Dict[str, Optional[float]] = {}
        averages["oppervlakte_avg_m2"] = (
            round(self.surface_sum / self.surface_n, 6) if self.surface_n else None
        )
        averages["working_min_avg"] = (
            round(self.wmin_sum / self.wmin_n, 6) if self.wmin_n else None
        )
        averages["working_max_avg"] = (
            round(self.wmax_sum / self.wmax_n, 6) if self.wmax_n else None
        )

        avg_date_value = None
        avg_date_ordinal: Optional[int] = None
        if self.date_n:
            avg_date_ordinal = int(round(self.date_sum / self.date_n))
            try:
                avg_date_value = date.fromordinal(avg_date_ordinal).isoformat()
            except ValueError:
                avg_date_value = None

        return {
            "total_rows": total,
            "metrics": metrics,
            "averages": averages,
            "avg_oprichtingsdatum": {
                "value": avg_date_value,
                "ordinal": avg_date_ordinal,
                "count": self.date_n,
            },
            "multi_kvk": {
                "unique": unique_kvk,
                "multi": multi_kvk,
                "pct": multi_pct,
            },
            "rechtsvorm": {
                key: {"abs": count, "pct": pct(count)}
                for key, count in self.rechtsvorm_counts.items()
            },
            "province": {
                key: {"abs": count, "pct": pct(count)}
                for key, count in self.province_counts.items()
            },
            "sbi": {
                key: {"abs": count, "pct": pct(count)}
                for key, count in self.sbi_counts.items()
            },
            "warnings": self.warnings,
        }


# Baseline data --------------------------------------------------------------


@dataclass
class BaselineData:
    total_rows: int
    summary_values: Dict[str, object]
    rechtsvorm_totals: Dict[str, int]
    province_pct: Dict[str, float]
    sbi_pct: Dict[str, float]
    avg_oprichtingsdatum: Optional[str]
    avg_oprichtingsdatum_ordinal: Optional[int]


_BASELINE_CACHE: Optional[BaselineData] = None


def _load_baseline() -> BaselineData:
    global _BASELINE_CACHE
    if _BASELINE_CACHE is not None:
        return _BASELINE_CACHE

    if not SUMMARY_FILE.exists():
        raise FileNotFoundError(
            f"Reference summary not found: {SUMMARY_FILE}. Place the bigdata folder next to the repository."
        )
    if not PROVINCE_FILE.exists():
        raise FileNotFoundError(
            f"Reference province summary not found: {PROVINCE_FILE}."
        )
    if not SBI_FILE.exists():
        raise FileNotFoundError(
            f"Reference SBI summary not found: {SBI_FILE}."
        )

    summary_values: Dict[str, object] = {}
    rechtsvorm_totals: Dict[str, int] = {}
    total_rows = 0
    avg_date = None
    avg_date_ordinal = None

    with SUMMARY_FILE.open("r", encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle, delimiter=CSV_DELIMITER)
        for row in reader:
            rv = (row.get("rechtsvorm") or "").strip()
            total = _parse_int(row.get("total_rows")) or 0
            if rv == "ALL":
                total_rows = total
                for col, value in row.items():
                    key = _sanitize_column(col)
                    if key == "rechtsvorm":
                        continue
                    if key == "avg_oprichtingsdatum_yyyy_mm_dd":
                        summary_values[key] = value.strip() if value else ""
                        if value:
                            dt = _parse_date_any(value)
                            if dt:
                                avg_date = dt.isoformat()
                                avg_date_ordinal = dt.toordinal()
                        continue
                    if key.endswith("_pct") or key.endswith("_avg"):
                        summary_values[key] = _parse_float(value) or 0.0
                    elif key in {"unique_kvk", "multi_kvk_abs"}:
                        summary_values[key] = _parse_int(value) or 0
                continue
            if rv:
                rechtsvorm_totals[rv] = total

    province_pct: Dict[str, float] = {}
    with PROVINCE_FILE.open("r", encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle, delimiter=CSV_DELIMITER)
        for row in reader:
            if (row.get("rechtsvorm") or "").strip() != "ALL":
                continue
            province = row.get("province") or ""
            pct = _parse_float(row.get("pct_of_all_rows")) or 0.0
            if province:
                province_pct[province] = pct

    sbi_pct: Dict[str, float] = {}
    with SBI_FILE.open("r", encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle, delimiter=CSV_DELIMITER)
        for row in reader:
            if (row.get("rechtsvorm") or "").strip() != "ALL":
                continue
            code = row.get("sbi_code") or ""
            pct = _parse_float(row.get("pct_of_all_rows")) or 0.0
            if code:
                sbi_pct[code] = pct

    _BASELINE_CACHE = BaselineData(
        total_rows=total_rows,
        summary_values=summary_values,
        rechtsvorm_totals=rechtsvorm_totals,
        province_pct=province_pct,
        sbi_pct=sbi_pct,
        avg_oprichtingsdatum=avg_date,
        avg_oprichtingsdatum_ordinal=avg_date_ordinal,
    )
    return _BASELINE_CACHE


# Comparison -----------------------------------------------------------------


SUMMARY_LABELS: Dict[str, str] = {
    "contactpersoon": "Contactpersoon aanwezig",
    "economischactief_true": "Economisch actief (ja)",
    "phone": "Telefoonnummer aanwezig",
    "fax": "Faxnummer aanwezig",
}

for field in MEDIA_FIELDS:
    SUMMARY_LABELS[field] = f"{field.capitalize()} aanwezig"

for bucket in GEBRUIKSDOEL_BUCKETS:
    safe = bucket.replace(" ", "_")
    label = bucket.replace(" ", " ")
    SUMMARY_LABELS[f"gebruiksdoel_{safe}"] = f"Gebruiksdoel: {bucket}"


def _expected_count(total: int, pct_value: float) -> float:
    return (pct_value / 100.0) * total if total else 0.0


def _compare_summary(filtered: Dict[str, object], baseline: BaselineData) -> Dict[str, object]:
    total = filtered["total_rows"]
    metrics = filtered["metrics"]
    summary_rows: List[Dict[str, object]] = []

    for key, label in SUMMARY_LABELS.items():
        metric = metrics.get(key)
        if not metric:
            continue
        col = SUMMARY_PCT_COLUMNS.get(key) or f"{key}_pct"
        baseline_pct = baseline.summary_values.get(_sanitize_column(col))
        if baseline_pct is None:
            continue
        if not isinstance(baseline_pct, (int, float)):
            continue
        filtered_pct = float(metric.get("pct", 0.0))
        filtered_abs = int(metric.get("abs", 0))
        diff = filtered_pct - float(baseline_pct)
        summary_rows.append(
            {
                "key": key,
                "label": label,
                "filtered_pct": filtered_pct,
                "filtered_abs": filtered_abs,
                "baseline_pct": float(baseline_pct),
                "expected_abs": _expected_count(total, float(baseline_pct)),
                "diff_pct": diff,
                "abs_diff_pct": abs(diff),
                "direction": "higher" if diff > 0 else ("lower" if diff < 0 else "same"),
            }
        )

    summary_rows.sort(key=lambda item: item["abs_diff_pct"], reverse=True)

    positives = [row for row in summary_rows if row["diff_pct"] > 0][:3]
    negatives = [row for row in summary_rows if row["diff_pct"] < 0][:3]

    averages_out: List[Dict[str, object]] = []
    averages = filtered["averages"]
    for key, label, unit in AVERAGE_COLUMNS:
        value = averages.get(key)
        base_key = _sanitize_column(key)
        baseline_value = baseline.summary_values.get(base_key)
        diff = None
        if isinstance(value, (int, float)) and isinstance(baseline_value, (int, float)):
            diff = value - baseline_value
        averages_out.append(
            {
                "key": key,
                "label": label,
                "unit": unit,
                "filtered": value,
                "baseline": baseline_value,
                "diff": diff,
            }
        )

    avg_date = filtered["avg_oprichtingsdatum"]
    date_comparison = {
        "filtered": avg_date.get("value"),
        "baseline": baseline.avg_oprichtingsdatum,
        "diff_days": None,
        "direction": None,
    }
    if avg_date.get("ordinal") and baseline.avg_oprichtingsdatum_ordinal:
        diff_days = avg_date["ordinal"] - baseline.avg_oprichtingsdatum_ordinal
        date_comparison["diff_days"] = diff_days
        if diff_days > 0:
            date_comparison["direction"] = "newer"
        elif diff_days < 0:
            date_comparison["direction"] = "older"
        else:
            date_comparison["direction"] = "same"

    multi = filtered["multi_kvk"]
    baseline_multi_pct = baseline.summary_values.get("multi_kvk_pct", 0.0)
    multi_out = {
        "unique": multi.get("unique", 0),
        "multi": multi.get("multi", 0),
        "filtered_pct": multi.get("pct", 0.0),
        "baseline_pct": baseline_multi_pct if isinstance(baseline_multi_pct, (int, float)) else 0.0,
    }
    if multi_out["unique"]:
        multi_out["expected_multi"] = (
            multi_out["unique"] * (multi_out["baseline_pct"] / 100.0)
        )
    else:
        multi_out["expected_multi"] = 0.0
    multi_out["diff_pct"] = multi_out["filtered_pct"] - multi_out["baseline_pct"]

    return {
        "metrics": summary_rows,
        "averages": averages_out,
        "avg_oprichtingsdatum": date_comparison,
        "multi_kvk": multi_out,
        "highlights": {
            "positive": positives,
            "negative": negatives,
        },
    }


def _compare_distribution(
    filtered_map: Dict[str, Dict[str, float]],
    baseline_map: Dict[str, float],
    total_filtered: int,
    label: str,
    max_rows: int = 40,
) -> Dict[str, object]:
    rows: List[Dict[str, object]] = []
    seen = set()

    for value, info in filtered_map.items():
        filtered_pct = float(info.get("pct", 0.0))
        filtered_abs = int(info.get("abs", 0))
        baseline_pct = float(baseline_map.get(value, 0.0))
        diff = filtered_pct - baseline_pct
        rows.append(
            {
                "value": value,
                "filtered_abs": filtered_abs,
                "filtered_pct": filtered_pct,
                "baseline_pct": baseline_pct,
                "expected_abs": _expected_count(total_filtered, baseline_pct),
                "diff_pct": diff,
                "abs_diff_pct": abs(diff),
                "direction": "higher" if diff > 0 else ("lower" if diff < 0 else "same"),
            }
        )
        seen.add(value)

    for value, baseline_pct in baseline_map.items():
        if value in seen:
            continue
        if baseline_pct <= 0:
            continue
        diff = -baseline_pct
        rows.append(
            {
                "value": value,
                "filtered_abs": 0,
                "filtered_pct": 0.0,
                "baseline_pct": baseline_pct,
                "expected_abs": _expected_count(total_filtered, baseline_pct),
                "diff_pct": diff,
                "abs_diff_pct": abs(diff),
                "direction": "lower",
            }
        )

    rows.sort(key=lambda item: item["abs_diff_pct"], reverse=True)
    omitted = max(0, len(rows) - max_rows)
    rows = rows[:max_rows]
    return {"label": label, "rows": rows, "omitted": omitted}


def compare_with_baseline(
    filtered: Dict[str, object],
    baseline: BaselineData,
    dimensions: Sequence[str],
) -> Dict[str, object]:
    dims = set(dimensions) | {"summary"}
    result: Dict[str, object] = {
        "total_rows": filtered["total_rows"],
        "baseline_total_rows": baseline.total_rows,
        "dimensions": sorted(dims),
        "summary": _compare_summary(filtered, baseline),
        "groups": {},
        "warnings": filtered.get("warnings", []),
    }

    total_filtered = filtered["total_rows"]

    if "rechtsvorm" in dims:
        base_totals = baseline.rechtsvorm_totals
        base_total = baseline.total_rows or 1
        baseline_pct = {
            rv: (count / base_total * 100.0) if base_total else 0.0
            for rv, count in base_totals.items()
        }
        group = _compare_distribution(
            filtered.get("rechtsvorm", {}),
            baseline_pct,
            total_filtered,
            "Rechtsvorm",
        )
        result["groups"]["rechtsvorm"] = group

    if "province" in dims:
        group = _compare_distribution(
            filtered.get("province", {}),
            baseline.province_pct,
            total_filtered,
            "Province",
            max_rows=len(PROVINCES),
        )
        result["groups"]["province"] = group

    if "sbi" in dims:
        group = _compare_distribution(
            filtered.get("sbi", {}),
            baseline.sbi_pct,
            total_filtered,
            "SBI",
            max_rows=50,
        )
        result["groups"]["sbi"] = group

    return result


# Entry point ----------------------------------------------------------------


def perform_analysis(header: Sequence[str], rows: Iterable[Sequence[str]], dimensions: Sequence[str]) -> Dict[str, object]:
    analyzer = StreamingAnalyzer(header)
    for row in rows:
        analyzer.consume(row)
    filtered = analyzer.finalize()
    baseline = _load_baseline()
    comparison = compare_with_baseline(filtered, baseline, dimensions)
    return comparison

