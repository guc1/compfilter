# AGENTS.md — Compfilter

**Mission:** Provide a fast, safe, streaming filter pipeline over very large CSVs (millions of rows), exposed via a tiny Flask backend and a no-build static frontend. Data remains local; only code goes to GitHub.

---

## High-level architecture

Compfilter/
frontend/ # vanilla HTML/CSS/JS (no bundler)
backend/
app.py # Flask API (preview, download, filters listing, uploads)
config.py # HOST/PORT/paths
filterscripts/ # streaming filters (stateless; pure functions)
combinator.py # orchestrates filter chain + CSV read/write
<feature>_filter.py
data/
provincies_wgs84.geojson # province polygons (WGS84) — local only
custom_aoi/ # user-uploaded AOIs — local only
other/ # reserved utilities / later modules

markdown
Copy code

**Data model assumptions**
- Main file is **UTF-8**, **semicolon** (`;`) delimited, with a consistent header row.
- Columns used by filters (case-insensitive candidates handled in code):
  - `rechtsvorm` (legal form)
  - `economischactief` (TRUE/FALSE)
  - `workingminimum`, `workingmaximum` (ints; `999999999` means *unknown*)
  - `contactpersoon` (JSON-ish string; non-empty ⇒ has contact)
  - Media toggles: `email`, `facebook`, `instagram`, `linkedin`, `pinterest`, `twitter`, `youtube`, `internetaddress` (non-empty ⇒ TRUE)
  - Traditional outreach: `phonenumber_formatted`, `faxnumber_formatted`, `postaladdress`
  - Vestiging: `gebruiksdoelverblijfsobject` + `hoofdvestiging` + `kvk_non_mailing_indicator` + `oppervlakteverblijfsobject`
  - Overige: `oprichtingsdatum` (YYYY-MM-DD or Dutch text parsed upstream), `tradenames`
  - Location: `latitude`, `longitude` (WGS84, decimal degrees)

**Performance constraints**
- Files can be 3–4M rows, 2–3 GB.
- **Never** load the whole CSV into memory.
- All filters must work in a **streaming** style: generator in → generator out.
- Preview = count items of the stream; Download = stream write to response file (CSV safe).

---

## Backend API (current)

- `GET /api/filters` → `{"filters":[…], "options":{"rechtsvorm":[…], "location":[…], ...}}`  
  - Options may be computed lazily; for streaming-only filters return `[]`.
- `POST /api/preview` → accepts `{"selected": {...}}`, returns `{"count": N}`.
- `POST /api/download` → same selection; streams a filtered CSV file (semicolon-delimited, UTF-8, header preserved).
- `POST /api/location/upload` → multipart form (`file=.geojson`), stores under `backend/filterscripts/data/custom_aoi/`, invalidates in-memory cache.
- `GET /api/location/list` → debug; lists what `location_filter` currently exposes.

**HTTP rules**
- Request/response JSON only; no sessions.
- Always log meaningful events (e.g., `[UPLOAD]`, `[LOCATION] loaded provinces: X, custom AOIs: Y`).

---

## Filter framework

Each filter module in `backend/filterscripts/` implements:

```python
FILTER_KEY = "my_filter"

def name() -> str: ...
def distinct_values(*args, **kwargs) -> List[str]:      # for multiselect filters; [] if N/A or dynamic
def apply(rows_iter: Iterable[List[str]], header: List[str], selected_values) -> Generator[List[str], None, None]:
    """Yield only rows that pass the filter. If nothing selected, yield all rows (passthrough)."""
combinator.py responsibilities

Read input CSV with csv.reader(delimiter=';') as a generator.

Apply enabled filters in a deterministic order.

CSV-safety: write with csv.writer(delimiter=';') preserving header & column order.

Preview = sum(1 for _ in filtered_stream).

Register filters in a FILTERS dict and expose FILTER_META describing UI type (multiselect, range, bool, number).

Streaming best practices

Never parse the whole row into objects; operate on the raw list[str] and lookup indices once (header index).

Handle missing/empty as falsy; treat '999999999' in workingmaximum as unknown (do not pass filter unless x=ignore).

For contactpersoon, treat any non-empty string as TRUE. (Upstream may provide JSON-like text.)

Location filter specifics
File expectations:

backend/filterscripts/data/provincies_wgs84.geojson (EPSG:4326 – WGS 84)

backend/filterscripts/data/custom_aoi/*.geojson (Polygon/MultiPolygon/Feature/FeatureCollection)

distinct_values() lists provinces then custom AOIs as custom:<filename>.

apply():

Detect longitude/latitude (candidates: lon|lng|x and lat|y).

Build a STRtree(targets) (Shapely 2.x). Dereference indices from query() before .intersects(pt).

Boundary-inclusive: use intersects (faster and includes edges).

invalidate_cache() resets in-memory lists; the upload endpoint must call it after saving a new AOI.

Frontend conventions
No build step (plain index.html, styles.css, script.js).

Filter UI is data-driven from /api/filters.

Each filter is a button → panel:

multiselect checklists with Select all / Clear,

number/range inputs for working numbers & surface,

date pickers for oprichtingsdatum (YYYY-MM-DD; the UI can accept manual typing too),

Location panel includes a Custom area (GeoJSON) uploader.

Chips on dashboard show active filters with × to clear that filter quickly.

How to add a new filter
Create backend/filterscripts/<feature>_filter.py with name, distinct_values, apply (streaming).

Register it in combinator.py:

Import it in the block with other filters.

Add to FILTERS mapping.

Add UX metadata to FILTER_META (label, type).

Frontend: no change for multiselect/bool/number – the UI renders from FILTER_META automatically.
If you need a special control (e.g., uploader), add a small block in script.js inside the panel open handler.

Keep everything streaming; do not import pandas.

Local-only data policy
Never commit:

compenv/

large datasets (CSV/Excel/Parquet),

AOIs and province GeoJSON.

.gitignore enforces that (see repo root).

For sharing large data, use a private artifact store or Git LFS (opt-in).

Quality & performance checklist
 Streams only; no full-file pandas usage.

 Header preserved; delimiter ;; encoding UTF-8 on write.

 Index columns once (header → indices dict).

 Treat 999999999 as unknown for workingmaximum.

 All boolean filters accept “has value” versus “empty”.

 Location: CRS is EPSG:4326 and point coords are (lon, lat).

 Preview returns fast even with multiple filters (linear pass).

 Download writes safely (quote where needed via csv.writer).

Dev commands
bash
Copy code
# run
source compenv/bin/activate
python -m backend.app  # http://127.0.0.1:3004

# snapshot of code structure (local)
./make_snapshot.sh  # or the one-liner used in chat

# backup (without venv)
rsync -a --delete --exclude 'compenv/' ./ ../Compfilter_backup_$(date +%Y%m%d_%H%M)/
Known edge cases / gotchas
If you see AttributeError: 'numpy.int64' object has no attribute 'intersects' in location filtering:

You are likely using Shapely 2.x; ensure we dereference indices from STRtree.query() before calling .intersects().

If upload works but custom AOI doesn’t appear:

Ensure /api/location/upload logs [UPLOAD] saved to: ... and [UPLOAD] invalidate_cache: done.

Hit GET /api/location/list to see what the backend exposes now.
