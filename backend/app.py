import csv
import json
import re
from datetime import datetime
from pathlib import Path

from flask import Flask, send_from_directory, jsonify, request, Response
from backend.filterscripts import combinator

app = Flask(__name__, static_folder="../frontend", static_url_path="")

REPO_ROOT = Path(__file__).resolve().parents[1]
PREFERENCES_DIR = REPO_ROOT / "bigdata" / "preferences"


def ensure_preferences_dir() -> Path:
    """Create the preferences directory if it doesn't exist."""
    PREFERENCES_DIR.mkdir(parents=True, exist_ok=True)
    return PREFERENCES_DIR


def sanitize_preference_name(raw_name: str | None) -> str:
    """Return a safe filename for a preference export."""
    base = (raw_name or "").strip()
    if base:
        base = base.replace("\\", "/").split("/")[-1]
        base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
    if not base:
        base = f"preference_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if not base.lower().endswith(".csv"):
        base = f"{base}.csv"
    return base


def ensure_unique_path(path: Path) -> Path:
    """Return a unique path if the target already exists."""
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 1
    while True:
        candidate = path.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1

@app.route("/")
def index():
    return send_from_directory("../frontend", "index.html")

@app.route("/styles.css")
def styles():
    return send_from_directory("../frontend", "styles.css")

@app.route("/script.js")
def script():
    return send_from_directory("../frontend", "script.js")

@app.route("/api/filters", methods=["GET"])
def api_filters():
    print("/api/filters served — using registry in combinator.py")
    try:
        loc_opts = combinator.distinct_values('location') if hasattr(combinator,'distinct_values') else []
    except Exception:
        loc_opts = []
    print('   location opts:', len(loc_opts))
    return jsonify({
        "filters": combinator.list_filters(),
        "options": combinator.get_filter_options(),
    })

@app.route("/api/preview", methods=["POST"])
def api_preview():
    payload = request.get_json(silent=True) or {}
    sel = payload.get("selected", {})
    advanced = payload.get("advanced", {})
    try:
        count = combinator.preview_count(sel, advanced)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500
    return jsonify({"ok": True, "count": int(count)})


@app.route("/api/analysis", methods=["POST"])
def api_analysis():
    payload = request.get_json(silent=True) or {}
    sel = payload.get("selected", {})
    advanced = payload.get("advanced", {})
    dims_raw = payload.get("dimensions")
    if isinstance(dims_raw, list):
        dims = [str(item) for item in dims_raw if isinstance(item, str)]
    else:
        dims = []
    try:
        result = combinator.statistical_analysis(sel, advanced, dims)
    except FileNotFoundError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500
    response = {"ok": True}
    response.update(result)
    return jsonify(response)

@app.route("/api/download", methods=["POST"])
def api_download():
    payload = request.get_json(silent=True) or {}
    sel = payload.get("selected", {})
    advanced = payload.get("advanced", {})
    try:
        stream = combinator.stream_filtered_csv(sel, advanced)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500

    def gen():
        for chunk in stream:
            yield chunk
    headers = {
        "Content-Disposition": "attachment; filename=filtered_results.csv",
        "Content-Type": "text/csv; charset=utf-8",
        "Cache-Control": "no-store"
    }
    return Response(gen(), headers=headers)


@app.route("/api/save", methods=["POST"])
def api_save():
    payload = request.get_json(silent=True) or {}
    sel = payload.get("selected", {})
    advanced = payload.get("advanced", {})
    directory_raw = str(payload.get("directory") or "").strip()
    base_name = str(payload.get("baseName") or payload.get("basename") or "").strip()
    max_rows_raw = payload.get("maxRowsPerFile") or payload.get("max_rows_per_file")
    destinations_payload = payload.get("destinations")

    def build_response(files, total_rows, details, fallback_directory=None, fallback_max_rows=None):
        response = {
            "ok": True,
            "files": [str(p) for p in files],
            "created_files": len(files),
            "total_rows": total_rows,
        }
        if details:
            response["destinations"] = details
            response["directory"] = details[0]["directory"]
            response["max_rows_per_file"] = details[0]["max_rows_per_file"]
        else:
            if fallback_directory:
                response["directory"] = str(Path(fallback_directory).expanduser().resolve())
            if fallback_max_rows is not None:
                response["max_rows_per_file"] = fallback_max_rows
        return response

    if isinstance(destinations_payload, list) and len(destinations_payload) > 0:
        parsed_dests = []
        rest_seen = False
        for idx, entry in enumerate(destinations_payload):
            directory_val = entry.get("directory")
            if directory_val is None or str(directory_val).strip() == "":
                directory_val = directory_raw
            base_val = entry.get("baseName") or entry.get("basename")
            if base_val is None or str(base_val).strip() == "":
                base_val = base_name
            max_rows_val = entry.get("maxRowsPerFile") or entry.get("max_rows_per_file")
            if max_rows_val is None:
                max_rows_val = max_rows_raw

            rows_val = entry.get("rows")
            if rows_val is None:
                rows_val = entry.get("rows_requested") or entry.get("rowsRequested")
            mode_val = str(entry.get("mode") or "").strip().lower()

            directory_val = str(directory_val or "").strip()
            if not directory_val:
                return jsonify({"ok": False, "error": f"Destination {idx + 1}: directory is required"}), 400

            base_val = str(base_val or "").strip()

            if max_rows_val is None:
                return jsonify({"ok": False, "error": f"Destination {idx + 1}: max rows per file is required"}), 400
            try:
                max_rows = int(max_rows_val)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": f"Destination {idx + 1}: max rows per file must be an integer"}), 400
            if max_rows <= 0:
                return jsonify({"ok": False, "error": f"Destination {idx + 1}: max rows per file must be greater than zero"}), 400

            if isinstance(rows_val, str) and rows_val.strip().upper() == "R":
                rows_val = None
                mode_val = "rest"

            is_rest = False
            if mode_val == "rest":
                is_rest = True
            elif mode_val == "fixed":
                is_rest = False
            elif rows_val is None:
                is_rest = True

            if is_rest:
                if rest_seen:
                    return jsonify({"ok": False, "error": "Only one destination can use R (rest)."}), 400
                rest_seen = True
                requested_rows = None
            else:
                try:
                    requested_rows = int(rows_val)
                except (TypeError, ValueError):
                    return jsonify({"ok": False, "error": f"Destination {idx + 1}: amount saved must be a positive integer or R"}), 400
                if requested_rows <= 0:
                    return jsonify({"ok": False, "error": f"Destination {idx + 1}: amount saved must be greater than zero"}), 400

            parsed_dests.append({
                "directory": directory_val,
                "base_name": base_val,
                "max_rows_per_file": max_rows,
                "rows_requested": requested_rows,
            })

        try:
            files, total_rows, details = combinator.save_filtered_csv_multi(sel, parsed_dests, advanced)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except PermissionError as exc:
            return jsonify({"ok": False, "error": f"Permission denied: {exc}"}), 403
        except OSError as exc:
            return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500

        return jsonify(build_response(files, total_rows, details))

    # Fallback to legacy single-destination payload
    if not directory_raw:
        return jsonify({"ok": False, "error": "Target directory is required"}), 400
    if not base_name:
        return jsonify({"ok": False, "error": "Base filename is required"}), 400
    try:
        max_rows = int(max_rows_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Max rows per file must be an integer"}), 400

    single_dest = [{
        "directory": directory_raw,
        "base_name": base_name,
        "max_rows_per_file": max_rows,
        "rows_requested": None,
    }]

    try:
        files, total_rows, details = combinator.save_filtered_csv_multi(sel, single_dest, advanced)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except PermissionError as exc:
        return jsonify({"ok": False, "error": f"Permission denied: {exc}"}), 403
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500

    return jsonify(build_response(files, total_rows, details, fallback_directory=directory_raw, fallback_max_rows=max_rows))


@app.route("/api/preferences", methods=["GET"])
def api_preferences_list():
    directory = ensure_preferences_dir()
    items = []
    try:
        directory_resolved = directory.resolve()
    except OSError:
        directory_resolved = directory
    for path in directory.glob("*.csv"):
        try:
            stat = path.stat()
        except OSError:
            continue
        item = {
            "name": path.name,
            "path": str(path.resolve()),
            "size": stat.st_size,
        }
        try:
            item["modified"] = datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds")
        except Exception:
            item["modified"] = None
        items.append(item)
    items.sort(key=lambda row: row.get("modified") or "", reverse=True)
    return jsonify({"ok": True, "preferences": items, "directory": str(directory_resolved)})


@app.route("/api/preferences/create", methods=["POST"])
def api_preferences_create():
    payload = request.get_json(silent=True) or {}
    selected = payload.get("selected")
    advanced = payload.get("advanced")
    name_raw = payload.get("name") or payload.get("filename") or payload.get("file")

    if not isinstance(selected, dict):
        selected = {}
    if not isinstance(advanced, dict):
        advanced = {}

    filename = sanitize_preference_name(str(name_raw) if name_raw is not None else None)
    directory = ensure_preferences_dir()
    target = ensure_unique_path(directory / filename)

    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["section", "key", "value"])
            writer.writerow(["meta", "version", "1"])
            writer.writerow(["meta", "saved_at", timestamp])
            writer.writerow(["advanced", "payload", json.dumps(advanced, ensure_ascii=False, sort_keys=True)])
            for key in sorted(selected.keys()):
                writer.writerow(["selected", key, json.dumps(selected[key], ensure_ascii=False, sort_keys=True)])
    except (OSError, TypeError, ValueError, csv.Error) as exc:
        return jsonify({"ok": False, "error": f"Failed to write preference: {exc}"}), 500

    return jsonify({
        "ok": True,
        "file": target.name,
        "path": str(target.resolve()),
        "saved_at": timestamp,
    })


@app.route("/api/preferences/load", methods=["POST"])
def api_preferences_load():
    payload = request.get_json(silent=True) or {}
    name_raw = payload.get("name") or payload.get("filename") or payload.get("file")
    if not name_raw:
        return jsonify({"ok": False, "error": "Preference name is required"}), 400

    filename = sanitize_preference_name(str(name_raw))
    directory = ensure_preferences_dir()
    target = directory / filename
    if not target.exists():
        return jsonify({"ok": False, "error": "Preference not found"}), 404

    selected = {}
    advanced_payload = {}
    metadata = {}

    try:
        with target.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            _header = next(reader, None)
            for row in reader:
                if len(row) < 3:
                    continue
                section = (row[0] or "").strip().lower()
                key = row[1]
                raw_value = row[2]
                if section == "selected":
                    try:
                        selected[key] = json.loads(raw_value)
                    except json.JSONDecodeError:
                        selected[key] = raw_value
                elif section == "advanced":
                    if key == "payload":
                        try:
                            parsed = json.loads(raw_value)
                        except json.JSONDecodeError:
                            parsed = {}
                        if isinstance(parsed, dict):
                            advanced_payload.update(parsed)
                        else:
                            advanced_payload[key] = parsed
                    else:
                        try:
                            advanced_payload[key] = json.loads(raw_value)
                        except json.JSONDecodeError:
                            advanced_payload[key] = raw_value
                elif section == "meta":
                    metadata[key] = raw_value
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "Preference not found"}), 404
    except csv.Error as exc:
        return jsonify({"ok": False, "error": f"Failed to parse preference: {exc}"}), 400
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Filesystem error: {exc}"}), 500

    response = {
        "ok": True,
        "file": target.name,
        "path": str(target.resolve()),
        "selected": selected,
        "advanced": advanced_payload,
        "meta": metadata,
    }
    if "saved_at" in metadata:
        response["saved_at"] = metadata["saved_at"]
    return jsonify(response)


@app.route("/api/location/upload", methods=["POST"])
def api_location_upload():
    from flask import jsonify
    import json, re
    from pathlib import Path as _P

    save_dir = _P(__file__).with_name('filterscripts') / 'data' / 'custom_aoi'
    save_dir.mkdir(parents=True, exist_ok=True)

    if 'file' not in request.files:
        return jsonify({'ok': False, 'error': 'No file part'}), 400
    f = request.files['file']
    if not f or not f.filename:
        return jsonify({'ok': False, 'error': 'No selected file'}), 400
    name = f.filename
    if not name.lower().endswith('.geojson'):
        return jsonify({'ok': False, 'error': 'Must be a .geojson'}), 400

    stem = re.sub(r'[^A-Za-z0-9._-]+','_', _P(name).stem)[:80]
    out = save_dir / f"{stem}.geojson"

    try:
        content = f.read().decode('utf-8')
        js = json.loads(content)
        t = js.get('type') if isinstance(js, dict) else None
        if t not in ('FeatureCollection','Feature','Polygon','MultiPolygon'):
            return jsonify({'ok': False, 'error': 'Invalid GeoJSON type'}), 400
        out.write_text(content, encoding='utf-8')
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Parse/save failed: {e}'}), 400

    try:
        # Nudge the location filter to reload custom AOIs immediately
        from backend.filterscripts import location_filter
        location_filter.invalidate_cache()
    except Exception as _e:
        print('[UPLOAD] invalidate_cache warning:', _e)
    return jsonify({'ok': True, 'stored_as': out.name})


@app.route("/api/location/delete", methods=["POST"])
def api_location_delete():
    from flask import jsonify
    import re
    from pathlib import Path as _P

    payload = request.get_json(silent=True) or {}
    label = str(payload.get('label') or '').strip()
    if not label.startswith('custom:'):
        return jsonify({'ok': False, 'error': 'Only custom areas can be removed'}), 400

    stem = label.split('custom:', 1)[1]
    if not stem:
        return jsonify({'ok': False, 'error': 'Missing custom area name'}), 400
    if not re.fullmatch(r'[A-Za-z0-9._-]+', stem):
        return jsonify({'ok': False, 'error': 'Invalid custom area name'}), 400

    save_dir = _P(__file__).with_name('filterscripts') / 'data' / 'custom_aoi'
    target = save_dir / f"{stem}.geojson"
    if not target.exists():
        return jsonify({'ok': False, 'error': 'Custom area not found'}), 404

    try:
        target.unlink()
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Failed to remove file: {e}'}), 500

    try:
        from backend.filterscripts import location_filter
        location_filter.invalidate_cache()
    except Exception as _e:
        print('[DELETE] invalidate_cache warning:', _e)

    return jsonify({'ok': True, 'removed': target.name})


@app.route("/api/sbi/files", methods=["GET"])
def api_sbi_files():
    try:
        from backend.filterscripts import sbi_filter
        files = sbi_filter.list_uploaded_files()
        return jsonify({"ok": True, "files": files})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sbi/upload", methods=["POST"])
def api_sbi_upload():
    import re

    bucket = (request.form.get('bucket') or '').strip().lower()
    try:
        from backend.filterscripts import sbi_filter
        if bucket not in sbi_filter.BUCKETS:
            return jsonify({'ok': False, 'error': 'Invalid SBI bucket'}), 400
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500

    if 'file' not in request.files:
        return jsonify({'ok': False, 'error': 'No file part'}), 400
    f = request.files['file']
    if not f or not f.filename:
        return jsonify({'ok': False, 'error': 'No selected file'}), 400

    name = f.filename
    if not re.search(r"\.(csv|txt)$", name, re.IGNORECASE):
        return jsonify({'ok': False, 'error': 'Upload must be a CSV file'}), 400

    try:
        stored = sbi_filter.save_uploaded_csv(bucket, name, f.read())
    except ValueError as ve:
        return jsonify({'ok': False, 'error': str(ve)}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Failed to save file: {e}'}), 500

    return jsonify({'ok': True, 'stored_as': stored, 'bucket': bucket})

if __name__ == "__main__":
    print("▶ Starting Compfilter on http://127.0.0.1:3004")
    app.run(host="127.0.0.1", port=3004, debug=True)


@app.route("/api/location/list", methods=["GET"])
def api_location_list():
    try:
        from backend.filterscripts import location_filter
        vals = location_filter.distinct_values()
        return jsonify({"ok": True, "values": vals, "count": len(vals)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
