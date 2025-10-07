from flask import Flask, send_from_directory, jsonify, request, Response
from backend.filterscripts import combinator

app = Flask(__name__, static_folder="../frontend", static_url_path="")

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
    sel = (request.get_json(silent=True) or {}).get("selected", {})
    count = combinator.preview_count(sel)
    return jsonify({"count": int(count)})

@app.route("/api/download", methods=["POST"])
def api_download():
    sel = (request.get_json(silent=True) or {}).get("selected", {})
    def gen():
        for chunk in combinator.stream_filtered_csv(sel):
            yield chunk
    headers = {
        "Content-Disposition": "attachment; filename=filtered_results.csv",
        "Content-Type": "text/csv; charset=utf-8",
        "Cache-Control": "no-store"
    }
    return Response(gen(), headers=headers)

@app.route("/api/location/upload", methods=["POST"])
def api_location_upload():
    from flask import request, jsonify
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
