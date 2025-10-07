"""
Location filter:
- Lists Dutch provinces from provincies_wgs84.geojson (EPSG:4326).
- Also lists ANY *.geojson inside data/custom_aoi/ as "custom:<filename>".
- On filtering: keeps rows whose (lon, lat) point intersects any selected polygon.

Design notes:
- We log what we load so /api/filters logs become meaningful.
- We tolerate empty properties on custom AOIs (common from QGIS scratch-layer exports).
- Shapely 2.x STRtree.query returns indices; we dereference before intersects().
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Iterable, List, Generator, Optional, Dict, Tuple

from shapely.geometry import shape, Point
from shapely.strtree import STRtree

FILTER_KEY = "location"
DATA_DIR   = Path(__file__).with_name("data")
PROV_FILE  = DATA_DIR / "provincies_wgs84.geojson"
CUSTOM_DIR = DATA_DIR / "custom_aoi"

# Column candidates (case-insensitive)
LON_CANDS = ["longitude", "lon", "lng", "x"]
LAT_CANDS = ["latitude", "lat", "y"]

# In-memory cache
_CACHE_READY = False
_PROV_GEOMS: List = []
_PROV_NAMES: List[str] = []
_CUST_GEOMS: List = []
_CUST_NAMES: List[str] = []

def name() -> str:
    return FILTER_KEY

def _log(msg: str) -> None:
    print(f"[LOCATION] {msg}")

def invalidate_cache() -> None:
    """Force reload of province/custom geometries on next access."""
    global _CACHE_READY, _PROV_GEOMS, _PROV_NAMES, _CUST_GEOMS, _CUST_NAMES
    _CACHE_READY = False
    _PROV_GEOMS, _PROV_NAMES = [], []
    _CUST_GEOMS, _CUST_NAMES = [], []
    _log("cache invalidated")

def _load_all() -> None:
    """Load provinces and custom AOIs once (with logging)."""
    global _CACHE_READY, _PROV_GEOMS, _PROV_NAMES, _CUST_GEOMS, _CUST_NAMES
    if _CACHE_READY:
        return

    _PROV_GEOMS, _PROV_NAMES = [], []
    _CUST_GEOMS, _CUST_NAMES = [], []

    # Provinces
    if PROV_FILE.exists():
        try:
            js = json.loads(PROV_FILE.read_text(encoding="utf-8"))
            feats = js.get("features", []) if isinstance(js, dict) else []
            for ft in feats:
                geom = ft.get("geometry")
                if not geom: 
                    continue
                try:
                    g = shape(geom)
                except Exception as e:
                    _log(f"skip province geometry parse error: {e}")
                    continue
                props = (ft.get("properties") or {})
                # Try various keys; fallback to any string; final fallback to "Province <idx>"
                pname = None
                for k in ("provincienaam","naam","name","PROV_NAAM","provincie","Provincienaam","Provincie"):
                    if k in props and props[k]:
                        pname = str(props[k]); break
                if not pname:
                    for v in props.values():
                        if isinstance(v, str) and v.strip():
                            pname = v.strip(); break
                if not pname:
                    pname = f"Province {_PROV_GEOMS.__len__()+1}"
                _PROV_GEOMS.append(g)
                _PROV_NAMES.append(pname)
        except Exception as e:
            _log(f"ERROR reading provinces file: {PROV_FILE} -> {e}")
    else:
        _log(f"WARNING: provinces file not found: {PROV_FILE}")

    # Custom AOIs
    if CUSTOM_DIR.exists():
        for f in sorted(CUSTOM_DIR.glob("*.geojson")):
            try:
                js = json.loads(f.read_text(encoding="utf-8"))
                # Accept FeatureCollection / Feature / bare Polygon/MultiPolygon
                feats = []
                if isinstance(js, dict):
                    t = js.get("type")
                    if t == "FeatureCollection":
                        feats = js.get("features", [])
                    elif t == "Feature":
                        feats = [js]
                    elif t in ("Polygon","MultiPolygon"):
                        feats = [{"type":"Feature","geometry":js,"properties":{}}]
                ok = False
                for ft in feats:
                    geom = ft.get("geometry")
                    if not geom: 
                        continue
                    try:
                        g = shape(geom)
                    except Exception as e:
                        _log(f"skip custom {f.name} geometry parse error: {e}")
                        continue
                    # Use file stem as the stable display name
                    nm = f"custom:{f.stem}"
                    _CUST_GEOMS.append(g)
                    _CUST_NAMES.append(nm)
                    ok = True
                if not ok:
                    _log(f"custom file has no valid polygon features: {f.name}")
            except Exception as e:
                _log(f"ERROR reading custom file {f.name}: {e}")
    else:
        CUSTOM_DIR.mkdir(parents=True, exist_ok=True)

    _log(f"loaded provinces: {len(_PROV_NAMES)}, custom AOIs: {len(_CUST_NAMES)}")
    _CACHE_READY = True

def distinct_values(*_args, **_kw) -> List[str]:
    """Return provinces then custom AOIs (sorted)."""
    try:
        _load_all()
        prov = sorted(_PROV_NAMES)
        cust = sorted(_CUST_NAMES)
        return prov + cust
    except Exception as e:
        _log(f"distinct_values error: {e}")
        return []

def _find_idx(header: List[str], cands: List[str]) -> Optional[int]:
    norm = [h.strip().lower() for h in header]
    for c in cands:
        if c in norm:
            return norm.index(c)
    return None

def _collect_targets(selected: List[str]) -> List:
    name_to_geom: Dict[str, object] = {}
    for n, g in zip(_PROV_NAMES, _PROV_GEOMS):
        name_to_geom[n] = g
    for n, g in zip(_CUST_NAMES, _CUST_GEOMS):
        name_to_geom[n] = g
    return [name_to_geom[s] for s in selected if s in name_to_geom]

def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values: List[str]) -> Generator[List[str], None, None]:
    """Stream rows; yield only if point-in-selected-polygons."""
    if not selected_values:
        yield from rows_iter
        return

    try:
        _load_all()
    except Exception as e:
        _log(f"apply load error: {e}")
        return

    lon_i = _find_idx(header, LON_CANDS)
    lat_i = _find_idx(header, LAT_CANDS)
    if lon_i is None or lat_i is None:
        _log("missing latitude/longitude columns; skipping filter")
        return

    targets = _collect_targets(selected_values)
    if not targets:
        _log(f"no geometries matched selected values: {selected_values}")
        return

    # Small set -> build STRtree per request for simplicity
    tree = STRtree(targets)

    for row in rows_iter:
        try:
            lon = float(row[lon_i]); lat = float(row[lat_i])
        except Exception:
            continue
        pt = Point(lon, lat)
        try:
            idxs = tree.query(pt)      # indices in 'targets'
            geoms = tree.geometries
            for i in idxs:
                poly = geoms[int(i)]
                if poly.intersects(pt):
                    yield row
                    break
        except Exception:
            # Be resilient: if STRtree misbehaves, fall back to linear scan
            for poly in targets:
                try:
                    if poly.intersects(pt):
                        yield row
                        break
                except Exception:
                    continue
