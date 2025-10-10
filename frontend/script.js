const $ = (sel) => document.querySelector(sel);
const API = (path) => `${location.origin}${path}`;

let FILTERS_META = [];     // [{key,label,type}]
let FILTER_OPTIONS = {};   // key -> [options]
let SELECTED = {};         // key -> [] OR custom per filter
let ACTIVE_KEY = null;
let SBI_FILES = { main: [], sub: [], all: [] };
let LAST_PREVIEW_COUNT = null;
let PREVIEW_DIRTY = true;
let ANALYSIS_SELECTION = new Set(["summary"]);
let PREFERENCE_FILE_INPUT = null;
let TRACKING_CONTEXT = null;

const ADVANCED_STATE = {
  duplicatesPath: "",
  filterDuplicates: false,
};
const ADVANCED_PATH_STORAGE_KEY = "compfilter.advanced.duplicatesPath";

function getAdvancedPayload(){
  return {
    filterDuplicates: Boolean(ADVANCED_STATE.filterDuplicates),
    duplicatesPath: ADVANCED_STATE.duplicatesPath || "",
  };
}

function setAdvancedStatus(message, isError = false){
  const statusEl = document.getElementById("advancedStatus");
  if(!statusEl) return;
  statusEl.textContent = message || "";
  statusEl.classList.toggle("status-error", Boolean(isError));
}

function updateFilterDubsButton(){
  const btn = document.getElementById("filterDubsBtn");
  if(!btn) return;
  const active = Boolean(ADVANCED_STATE.filterDuplicates);
  btn.classList.toggle("is-active", active);
  btn.setAttribute("aria-pressed", active ? "true" : "false");
  btn.textContent = active ? "Filter dubs (on)" : "Filter dubs";
}

function setDuplicatesPath(rawValue){
  const value = typeof rawValue === "string" ? rawValue.trim() : "";
  ADVANCED_STATE.duplicatesPath = value;
  try {
    if(value){
      localStorage.setItem(ADVANCED_PATH_STORAGE_KEY, value);
    } else {
      localStorage.removeItem(ADVANCED_PATH_STORAGE_KEY);
    }
  } catch(_err) {
    // Ignore storage issues (private mode, etc.)
  }
}

function handleDuplicatesPathInput(event){
  const value = event && event.target ? event.target.value : "";
  setDuplicatesPath(value);
  if(!ADVANCED_STATE.filterDuplicates){
    setAdvancedStatus("");
  }
}

function handleDuplicatesPathChange(event){
  const value = event && event.target ? event.target.value : "";
  setDuplicatesPath(value);
  if(ADVANCED_STATE.filterDuplicates){
    if(ADVANCED_STATE.duplicatesPath){
      doPreview();
    } else {
      setAdvancedStatus("Provide a folder path before filtering duplicates.", true);
    }
  }
}

async function toggleFilterDubs(){
  const turningOn = !ADVANCED_STATE.filterDuplicates;
  if(turningOn && !ADVANCED_STATE.duplicatesPath){
    setAdvancedStatus("Provide a folder path before filtering duplicates.", true);
    const input = document.getElementById("duplicatesPath");
    if(input){
      input.focus();
    }
    return;
  }
  ADVANCED_STATE.filterDuplicates = !ADVANCED_STATE.filterDuplicates;
  updateFilterDubsButton();
  if(!ADVANCED_STATE.filterDuplicates){
    setAdvancedStatus("");
  }
  const success = await doPreview();
  if(turningOn && !success){
    ADVANCED_STATE.filterDuplicates = false;
    updateFilterDubsButton();
    await doPreview();
  }
}

function setTrackingStatus(message, isError = false){
  const statusEl = document.getElementById("trackingStatus");
  if(!statusEl) return;
  statusEl.textContent = message || "";
  statusEl.classList.toggle("status-error", Boolean(isError));
}

function renderTrackingLatest(latest){
  const host = document.getElementById("trackingLatest");
  if(!host) return;
  if(!latest){
    host.textContent = "No campaign saved yet. Run a custom save to capture metadata.";
    return;
  }
  const lines = [];
  if(latest.campaign){
    lines.push(`Campaign: ${latest.campaign}`);
  }
  if(latest.base_name){
    lines.push(`Subcampaign: ${latest.base_name}`);
  }
  if(latest.directory){
    lines.push(`Directory: ${latest.directory}`);
  }
  if(latest.timestamp){
    const stamp = new Date(latest.timestamp);
    if(!Number.isNaN(stamp.getTime())){
      lines.push(`Saved: ${stamp.toLocaleString()}`);
    }
  }
  if(latest.default_path){
    const existsText = latest.default_exists ? " (exists)" : "";
    lines.push(`Default tracking file: ${latest.default_path}${existsText}`);
  }
  host.innerHTML = "";
  const heading = document.createElement("div");
  heading.className = "tracking-latest-heading";
  heading.textContent = "Latest campaign";
  host.appendChild(heading);
  const list = document.createElement("ul");
  list.className = "tracking-latest-list";
  lines.forEach((text) => {
    const item = document.createElement("li");
    item.textContent = text;
    list.appendChild(item);
  });
  if(lines.length === 0){
    const item = document.createElement("li");
    item.textContent = "No additional details available.";
    list.appendChild(item);
  }
  host.appendChild(list);
}

async function refreshTrackingLatest(){
  const host = document.getElementById("trackingLatest");
  if(host){
    host.textContent = "Loading latest campaign…";
  }
  try{
    const res = await fetch(API("/api/tracking/latest"));
    let data = {};
    try{ data = await res.json(); } catch(_err){ data = {}; }
    if(!res.ok || (data && data.ok === false)){
      throw new Error((data && data.error) || res.statusText || "Failed to load campaign metadata");
    }
    if(!data.latest){
      TRACKING_CONTEXT = null;
      if(host){
        host.textContent = "No campaign saved yet. Run a custom save to capture metadata.";
      }
      return;
    }
    TRACKING_CONTEXT = data.latest;
    renderTrackingLatest(data.latest);
    const pathInput = document.getElementById("trackingPath");
    if(pathInput && typeof data.latest.default_path === "string" && !pathInput.value){
      pathInput.value = data.latest.default_path;
    }
    setTrackingStatus("");
  }catch(err){
    TRACKING_CONTEXT = null;
    if(host){
      host.textContent = `Failed to load latest campaign: ${err && err.message ? err.message : err}`;
    }
  }
}

function openTrackingModal(){
  const modal = document.getElementById("trackingModal");
  if(!modal) return;
  modal.classList.remove("hidden");
  setTrackingStatus("");
  refreshTrackingLatest();
}

function closeTrackingModal(){
  const modal = document.getElementById("trackingModal");
  if(!modal) return;
  modal.classList.add("hidden");
}

async function runTracking(mode){
  if(!TRACKING_CONTEXT){
    await refreshTrackingLatest();
    if(!TRACKING_CONTEXT){
      setTrackingStatus("No campaign metadata available. Run a custom save first.", true);
      return;
    }
  }
  const ctx = TRACKING_CONTEXT || {};
  const pathInput = document.getElementById("trackingPath");
  const createBtn = document.getElementById("trackingCreate");
  const updateBtn = document.getElementById("trackingUpdate");
  const targetPath = pathInput && pathInput.value ? pathInput.value.trim() : "";
  if(mode === "update" && !targetPath){
    setTrackingStatus("Provide the tracking CSV path to update.", true);
    if(pathInput){ pathInput.focus(); }
    return;
  }
  if(createBtn) createBtn.disabled = true;
  if(updateBtn) updateBtn.disabled = true;
  setTrackingStatus(mode === "update" ? "Updating…" : "Creating…");
  try{
    const payload = {
      mode,
      selected: SELECTED,
      advanced: getAdvancedPayload(),
      campaignDirectory: ctx.directory || ctx.campaign || "",
      baseName: ctx.base_name || ctx.baseName || "",
    };
    if(targetPath){
      payload.targetPath = targetPath;
    }
    const res = await fetch(API("/api/tracking/run"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    let data = {};
    try{ data = await res.json(); } catch(_err){ data = {}; }
    if(!res.ok || (data && data.ok === false)){
      throw new Error((data && data.error) || res.statusText || "Tracking failed");
    }
    if(pathInput && typeof data.path === "string"){
      pathInput.value = data.path;
    }
    const parts = [];
    if(typeof data.rows === "number"){
      parts.push(`${data.rows.toLocaleString()} total rows`);
    }
    if(typeof data.new_rows === "number"){
      parts.push(`${data.new_rows.toLocaleString()} from current selection`);
    }
    if(data.path){
      parts.push(`Saved to ${data.path}`);
    }
    setTrackingStatus(parts.join(" · "));
  }catch(err){
    setTrackingStatus(err && err.message ? err.message : String(err), true);
  }finally{
    if(createBtn) createBtn.disabled = false;
    if(updateBtn) updateBtn.disabled = false;
  }
}

const SBI_BUCKETS = [
  { id: "main", label: "Main SBI" },
  { id: "sub",  label: "Sub SBI"  },
  { id: "all",  label: "All SBI"  }
];

const ANALYSIS_DIMENSIONS = [
  { key: "summary", label: "Overall signals", requires: null, always: true },
  { key: "rechtsvorm", label: "Rechtsvorm distribution", requires: "rechtsvorm" },
  { key: "province", label: "Province distribution", requires: "location" },
  { key: "sbi", label: "SBI distribution", requires: "sbi" },
];

function getMeta(key){
  return FILTERS_META.find(f => f.key === key) || { key, label: key, type: "multiselect" };
}

function ensureSbiState(){
  SELECTED.sbi = normalizeSbiSelection(SELECTED.sbi);
}

function clearSelection(key){
  const meta = getMeta(key);
  if(meta.type === "sbi"){
    ensureSbiState();
    SELECTED[key] = baseSbiSelection();
  } else {
    SELECTED[key] = [];
  }
  markPreviewDirty();
}

function baseSbiSelection(){
  return {
    main: { codes: [], file: null },
    sub:  { codes: [], file: null },
    all:  { codes: [], file: null }
  };
}

function normalizeSbiSelection(sel){
  const base = baseSbiSelection();
  if(!sel || typeof sel !== "object") return base;
  SBI_BUCKETS.forEach(({id}) => {
    const bucket = sel[id];
    if(bucket && typeof bucket === "object"){
      const codes = Array.isArray(bucket.codes) ? bucket.codes.filter(v => typeof v === "string" && v.trim()) : [];
      base[id].codes = codes.map(v => v.trim());
      const file = bucket.file;
      base[id].file = (typeof file === "string" && file.trim()) ? file.trim() : null;
    }
  });
  return base;
}

function hasSbiSelection(sel){
  const norm = normalizeSbiSelection(sel);
  return SBI_BUCKETS.some(({id}) => (norm[id].codes && norm[id].codes.length) || norm[id].file);
}

function summarizeSbiSelection(sel){
  const norm = normalizeSbiSelection(sel);
  const parts = [];
  SBI_BUCKETS.forEach(({id, label}) => {
    const codes = norm[id].codes ? norm[id].codes.length : 0;
    const file = norm[id].file ? 1 : 0;
    if(codes || file){
      const detail = [];
      if(codes) detail.push(`${codes} code${codes === 1 ? "" : "s"}`);
      if(file) detail.push(`file`);
      parts.push(`${label.split(" ")[0]}: ${detail.join(" + ")}`);
    }
  });
  return parts.length ? parts.join(" · ") : "x";
}

function parseSbiManualCodes(raw){
  if(!raw) return [];
  const parts = raw.split(/[\s,;]+/).map(v => v.trim()).filter(Boolean);
  const seen = new Set();
  const out = [];
  parts.forEach(code => {
    if(!seen.has(code)){
      seen.add(code);
      out.push(code);
    }
  });
  return out;
}

function formatNumber(value, decimals = 0){
  if(typeof value !== "number" || Number.isNaN(value)){
    return decimals > 0 ? Number(0).toFixed(decimals) : "0";
  }
  if(decimals > 0){
    return value.toLocaleString(undefined, {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }
  return value.toLocaleString();
}

function formatPercent(value){
  if(typeof value !== "number" || Number.isNaN(value)){
    return "0.00%";
  }
  return `${value.toFixed(2)}%`;
}

function createDeltaBadge(diff){
  const span = document.createElement("span");
  const value = typeof diff === "number" && !Number.isNaN(diff) ? diff : 0;
  let cls = "delta delta-neutral";
  if(value > 0.000001){
    cls = "delta delta-positive";
  } else if(value < -0.000001){
    cls = "delta delta-negative";
  }
  span.className = cls;
  const rounded = value.toFixed(2);
  span.textContent = `${value > 0 ? "+" : value < 0 ? "" : ""}${rounded} pp`;
  return span;
}

function markPreviewDirty(){
  PREVIEW_DIRTY = true;
  updateAnalysisButtonState();
  const modal = document.getElementById("analysisModal");
  if(modal && !modal.classList.contains("hidden")){
    const status = document.getElementById("analysisStatus");
    if(status){
      status.textContent = "Run a new preview to enable analysis.";
      status.classList.remove("status-error");
    }
  }
}

function updateAnalysisButtonState(){
  const btn = document.getElementById("analysisBtn");
  if(!btn) return;
  const ready = typeof LAST_PREVIEW_COUNT === "number" && LAST_PREVIEW_COUNT > 0 && !PREVIEW_DIRTY;
  btn.disabled = !ready;
  btn.classList.toggle("is-disabled", !ready);
}

function ensureAnalysisSelection(){
  if(!(ANALYSIS_SELECTION instanceof Set)){
    ANALYSIS_SELECTION = new Set();
  }
  ANALYSIS_SELECTION.add("summary");
}

function hasActiveSelection(key){
  const meta = getMeta(key);
  const sel = SELECTED[key];
  if(meta.type === "number"){
    if(!Array.isArray(sel)) return false;
    const [mn, mx] = sel;
    return Boolean((mn && `${mn}`.trim()) || (mx && `${mx}`.trim()));
  }
  if(meta.type === "sbi"){
    return hasSbiSelection(sel);
  }
  if(Array.isArray(sel)){
    return sel.length > 0;
  }
  if(sel && typeof sel === "object"){
    if(key === "sbi") return hasSbiSelection(sel);
    return Object.values(sel).some(Boolean);
  }
  return Boolean(sel);
}

function setSbiSelectOptions(selectEl, bucket, selected){
  if(!selectEl) return;
  const opts = Array.isArray(SBI_FILES[bucket]) ? SBI_FILES[bucket] : [];
  selectEl.innerHTML = "";
  const none = document.createElement("option");
  none.value = "";
  none.textContent = "-- None --";
  selectEl.appendChild(none);
  opts.forEach(name => {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    if(name === selected) opt.selected = true;
    selectEl.appendChild(opt);
  });
  if(selected && !opts.includes(selected)){
    const opt = document.createElement("option");
    opt.value = selected;
    opt.textContent = selected;
    opt.selected = true;
    selectEl.appendChild(opt);
  }
  if(!selected) selectEl.value = "";
}

async function refreshSbiFiles(){
  try{
    const res = await fetch(API("/api/sbi/files"));
    const data = await res.json();
    if(data && data.ok && data.files){
      SBI_FILES = {
        main: Array.isArray(data.files.main) ? data.files.main : [],
        sub: Array.isArray(data.files.sub) ? data.files.sub : [],
        all: Array.isArray(data.files.all) ? data.files.all : []
      };
    }
  }catch(err){
    console.error("Failed to load SBI files", err);
  }
}

async function uploadSbiFile(bucket, file){
  const form = new FormData();
  form.append("bucket", bucket);
  form.append("file", file);
  const res = await fetch(API("/api/sbi/upload"), {
    method: "POST",
    body: form
  });
  let data = {};
  try{ data = await res.json(); } catch(_){ data = {}; }
  if(!res.ok || !data.ok){
    throw new Error(data.error || res.statusText || "Upload failed");
  }
  await refreshSbiFiles();
  return data.stored_as;
}

function summarizeSelection(key){
  const meta = getMeta(key);
  if(meta.type === "sbi"){
    return summarizeSbiSelection(SELECTED[key]);
  }
  const sel = Array.isArray(SELECTED[key]) ? SELECTED[key] : [];
  if(meta.type === "number"){
    const mn = sel[0] ?? "";
    const mx = sel[1] ?? "";
    if(!mn && !mx) return "x";
    if(mn && mx) return `${mn}–${mx}`;
    if(mn) return `≥ ${mn}`;
    return `≤ ${mx}`;
  }
  if(meta.type === "group"){
    if(key === "overige"){
      const tn = sel.find(t => t === "tn=TRUE" || t === "tn=FALSE");
      const dmin = (sel.find(t => t.startsWith("date_min=")) || "").split("=")[1] || "";
      const dmax = (sel.find(t => t.startsWith("date_max=")) || "").split("=")[1] || "";
      const parts = [];
      if(tn) parts.push(tn.replace("tn=", "tn:"));
      if(dmin || dmax){
        parts.push(dmin && dmax ? `date:${dmin}–${dmax}` : (dmin ? `date:≥ ${dmin}` : `date:≤ ${dmax}`));
      }
      return parts.length ? parts.join(" · ") : "x";
    }
    const gd = sel.filter(t => t.startsWith("gd=")).length;
    const hv = sel.find(t => t === "hv=TRUE" || t === "hv=FALSE");
    const nm = sel.find(t => t === "nm=TRUE" || t === "nm=FALSE");
    const omin = sel.find(t => t.startsWith("oppmin="));
    const omax = sel.find(t => t.startsWith("oppmax="));
    const parts = [];
    if(gd > 0) parts.push(`gd:${gd}`);
    if(hv) parts.push(hv.replace("hv=", "hv:"));
    if(nm) parts.push(nm.replace("nm=", "nm:"));
    if(omin || omax){
      const a = omin ? omin.split("=")[1] : "";
      const b = omax ? omax.split("=")[1] : "";
      parts.push(a && b ? `opp:${a}–${b}` : (a ? `opp:≥ ${a}` : `opp:≤ ${b}`));
    }
    return parts.length ? parts.join(" · ") : "x";
  }
  return `${sel.length} selected`;
}

function renderDashboard(){
  const btnHost = $("#filterButtons");
  if(btnHost) btnHost.innerHTML = "";
  FILTERS_META.forEach(f => {
    const btn = document.createElement("button");
    btn.className = "filter-btn";
    btn.dataset.key = f.key;
    btn.innerHTML = `<span>${f.label}</span><span class="badge">${summarizeSelection(f.key)}</span>`;
    if(f.key === ACTIVE_KEY){
      btn.classList.add("is-active");
      btn.setAttribute("aria-pressed", "true");
    } else {
      btn.setAttribute("aria-pressed", "false");
    }
    btn.addEventListener("click", () => openPanel(f.key));
    btnHost.appendChild(btn);
  });

  const summaryHost = $("#activeSummary");
  if(!summaryHost) return;
  const chips = [];
  Object.entries(SELECTED).forEach(([k,v]) => {
    const meta = getMeta(k);
    let active = false;
    if(meta.type === "sbi"){
      active = hasSbiSelection(v);
    } else {
      active = Array.isArray(v) && v.length > 0;
    }
    if(!active) return;
    chips.push(`<span class="chip" data-key="${k}">${meta.label}: ${summarizeSelection(k)}<button class="chip-x" data-clearkey="${k}" title="Clear">×</button></span>`);
  });
  const hasAny = chips.length > 0;
  summaryHost.innerHTML = `<div class="chips">${chips.join(" ")}${hasAny ? '<button class="chip clear-all" id="clearAllBtn">Clear all</button>' : ''}</div>`;
}

function updateAoiUploaderVisibility(activeKey){
  const box = document.getElementById("aoiUploaderContainer");
  if(!box) return;
  box.classList.toggle("hidden", activeKey !== "location");
}

function openPanel(key, force=false){
  const panelEl = document.getElementById("panel");
  if(!force && ACTIVE_KEY === key && panelEl && !panelEl.classList.contains("hidden")){
    closePanel(false);
    return;
  }

  ACTIVE_KEY = key;
  document.querySelectorAll(".filter-btn").forEach(btn => {
    if(!(btn instanceof HTMLElement)) return;
    btn.classList.toggle("is-active", btn.dataset.key === key);
    btn.setAttribute("aria-pressed", btn.dataset.key === key ? "true" : "false");
  });
  updateAoiUploaderVisibility(key);

  const meta = getMeta(key);
  const titleEl = $("#panelTitle");
  if(titleEl) titleEl.textContent = meta.label;
  const host = $("#panelOptions");
  if(!host) return;
  host.innerHTML = "";

  if(meta.type === "number"){
    const current = SELECTED[key] || [];
    const curMin = current[0] || "";
    const curMax = current[1] || "";
    const wrap = document.createElement("div");
    wrap.innerHTML = `
      <div class="group">
        <label class="checkbox" style="display:block"><span>Enter working number range (employees). Use 'x' or leave blank to ignore a side.</span></label>
      </div>
      <div class="grid" style="grid-template-columns:1fr 1fr;">
        <div>
          <div class="muted" style="margin-bottom:4px;">Min</div>
          <input id="wnumMin" type="text" inputmode="numeric" pattern="[0-9xX]*" placeholder="x" value="${curMin}">
        </div>
        <div>
          <div class="muted" style="margin-bottom:4px;">Max</div>
          <input id="wnumMax" type="text" inputmode="numeric" pattern="[0-9xX]*" placeholder="x" value="${curMax}">
        </div>
      </div>
    `;
    host.appendChild(wrap);
    $("#panelHint").textContent = "Row kept if [row_min,row_max] overlaps your [Min, Max]. 999999999 is treated as ∞.";
  } else if (meta.type === "group"){
    if(key === "overige"){
      const current = new Set(SELECTED[key] || []);
      const wrap = document.createElement("div");
      wrap.innerHTML = `
        <div class="card" style="padding:12px;">
          <div class="muted" style="margin-bottom:6px;">Oprichtingsdatum</div>
          <div class="grid" style="grid-template-columns:1fr 1fr;">
            <div>
              <div class="muted" style="margin-bottom:4px;">Min</div>
              <input id="dateMin" type="date" value="${(Array.from(current).find(t=>t.startsWith('date_min='))||'').split('=')[1]||''}">
            </div>
            <div>
              <div class="muted" style="margin-bottom:4px;">Max</div>
              <input id="dateMax" type="date" value="${((Array.from(current).find(t=>t.startsWith('date_max='))||'').split('=')[1]||'')}">
            </div>
          </div>
        </div>
        <div class="card" style="padding:12px;">
          <div class="muted" style="margin-bottom:6px;">Tradenames</div>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="tn=TRUE" ${current.has('tn=TRUE')?'checked':''}> TRUE</label>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="tn=FALSE" ${current.has('tn=FALSE')?'checked':''}> FALSE</label>
          <div class="muted" style="font-size:12px; margin-top:6px;">(Pick both to ignore)</div>
        </div>
      `;
      host.appendChild(wrap);
      document.getElementById('panelHint').textContent = 'Pick date range and/or tradenames presence. Save selection to apply.';
    } else {
      const options = FILTER_OPTIONS[key] || [];
      const current = new Set(SELECTED[key] || []);

      const toolbar = document.createElement("div");
      toolbar.className = "mini-actions";
      toolbar.innerHTML = `
        <strong>Gebruiksdoel</strong>
        <div class="spacer"></div>
        <button id="selectAllBtn" type="button">Select all</button>
        <button id="clearAllBtn" type="button">Clear</button>
      `;
      host.appendChild(toolbar);

      const gdWrap = document.createElement("div");
      gdWrap.className = "grid";
      options.forEach(opt => {
        const token = "gd=" + opt;
        const id = `gd__${opt.replace(/\s+/g,'_')}`;
        const wrap = document.createElement("label");
        wrap.className = "checkbox";
        wrap.innerHTML = `<input type="checkbox" class="panel-opt-gd" id="${id}" ${current.has(token)?'checked':''} data-token="${token}"> <span>${opt}</span>`;
        gdWrap.appendChild(wrap);
      });
      host.appendChild(gdWrap);

      const bools = document.createElement("div");
      bools.className = "grid";
      bools.style.gridTemplateColumns = "repeat(auto-fill,minmax(220px,1fr))";
      bools.innerHTML = `
        <div class="card" style="padding:12px;">
          <div class="muted" style="margin-bottom:6px;">Hoofdvestiging</div>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="hv=TRUE" ${current.has('hv=TRUE')?'checked':''}> TRUE</label>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="hv=FALSE" ${current.has('hv=FALSE')?'checked':''}> FALSE</label>
          <div class="muted" style="font-size:12px; margin-top:6px;">(Pick both to ignore)</div>
        </div>
        <div class="card" style="padding:12px;">
          <div class="muted" style="margin-bottom:6px;">KVK non-mailing</div>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="nm=TRUE" ${current.has('nm=TRUE')?'checked':''}> TRUE</label>
          <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="nm=FALSE" ${current.has('nm=FALSE')?'checked':''}> FALSE</label>
          <div class="muted" style="font-size:12px; margin-top:6px;">(Pick both to ignore)</div>
        </div>
        <div class="card" style="padding:12px;">
          <div class="muted" style="margin-bottom:6px;">Oppervlakte verblijfsobject</div>
          <div class="grid" style="grid-template-columns:1fr 1fr;">
            <div>
              <div class="muted" style="margin-bottom:4px;">Min</div>
              <input id="oppMin" type="text" inputmode="numeric" pattern="[0-9xX]*" placeholder="x" value="${(Array.from(current).find(t=>t.startsWith('oppmin='))||'').split('=')[1]||''}">
            </div>
            <div>
              <div class="muted" style="margin-bottom:4px;">Max</div>
              <input id="oppMax" type="text" inputmode="numeric" pattern="[0-9xX]*" placeholder="x" value="${(Array.from(current).find(t=>t.startsWith('oppmax='))||'').split('=')[1]||''}">
            </div>
          </div>
          <div class="muted" style="font-size:12px; margin-top:6px;">Leave blank/x to ignore a side.</div>
        </div>
      `;
      host.appendChild(bools);

      toolbar.querySelector("#selectAllBtn").addEventListener("click", () => {
        host.querySelectorAll("input.panel-opt-gd").forEach(ch => ch.checked = true);
      });
      toolbar.querySelector("#clearAllBtn").addEventListener("click", () => {
        host.querySelectorAll("input.panel-opt-gd").forEach(ch => ch.checked = false);
      });

      $("#panelHint").textContent = "Pick gebruiksdoel + flags + range as needed. Save selection to apply.";
    }
  } else if (meta.type === "sbi"){
    ensureSbiState();
    const current = normalizeSbiSelection(SELECTED[key]);
    const wrap = document.createElement("div");
    wrap.className = "sbi-panel-grid";
    SBI_BUCKETS.forEach(({id, label}) => {
      const section = document.createElement("div");
      section.className = "card sbi-section";

      const title = document.createElement("div");
      title.className = "sbi-section-title";
      title.textContent = label;
      section.appendChild(title);

      const manualLabel = document.createElement("div");
      manualLabel.className = "muted";
      manualLabel.textContent = "Manual codes (comma or newline separated)";
      section.appendChild(manualLabel);

      const textarea = document.createElement("textarea");
      textarea.className = "sbi-codes";
      textarea.dataset.bucket = id;
      textarea.placeholder = "e.g. 73110";
      textarea.value = (current[id].codes || []).join("\n");
      section.appendChild(textarea);

      const fileLabel = document.createElement("div");
      fileLabel.className = "muted sbi-file-label";
      fileLabel.textContent = "Use uploaded list";
      section.appendChild(fileLabel);

      const fileRow = document.createElement("div");
      fileRow.className = "sbi-file-row";
      const select = document.createElement("select");
      select.className = "sbi-file-select";
      select.dataset.bucket = id;
      setSbiSelectOptions(select, id, current[id].file);
      fileRow.appendChild(select);
      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "sbi-clear-file";
      clearBtn.dataset.bucket = id;
      clearBtn.textContent = "Clear";
      clearBtn.addEventListener("click", () => {
        select.value = "";
      });
      fileRow.appendChild(clearBtn);
      section.appendChild(fileRow);

      const uploadRow = document.createElement("div");
      uploadRow.className = "sbi-upload-row";
      const fileInput = document.createElement("input");
      fileInput.type = "file";
      fileInput.accept = ".csv,.txt";
      fileInput.className = "sbi-upload-input";
      fileInput.dataset.bucket = id;
      uploadRow.appendChild(fileInput);
      const uploadBtn = document.createElement("button");
      uploadBtn.type = "button";
      uploadBtn.className = "sbi-upload-btn";
      uploadBtn.dataset.bucket = id;
      uploadBtn.textContent = "Upload";
      uploadRow.appendChild(uploadBtn);
      section.appendChild(uploadRow);

      const hint = document.createElement("div");
      hint.className = "muted sbi-upload-hint";
      hint.textContent = "Upload a CSV with the codes in the first column.";
      section.appendChild(hint);

      const status = document.createElement("div");
      status.className = "muted sbi-status";
      status.dataset.bucket = id;
      section.appendChild(status);

      uploadBtn.addEventListener("click", async () => {
        const file = fileInput.files && fileInput.files[0];
        if(!file){
          status.textContent = "Select a CSV file first.";
          return;
        }
        status.textContent = "Uploading…";
        try{
          const stored = await uploadSbiFile(id, file);
          setSbiSelectOptions(select, id, stored);
          fileInput.value = "";
          status.textContent = `Uploaded as ${stored}.`;
        }catch(err){
          console.error("SBI upload failed", err);
          status.textContent = err && err.message ? err.message : "Upload failed";
        }
      });

      wrap.appendChild(section);
    });
    host.appendChild(wrap);
    $("#panelHint").textContent = "Enter codes or upload CSV lists. Save selection to apply.";
  } else {
    const toolbar = document.createElement("div");
    toolbar.className = "mini-actions";
    toolbar.innerHTML = `
      <strong>Quick actions</strong>
      <div class="spacer"></div>
      <button id="selectAllBtn" type="button">Select all</button>
      <button id="clearAllBtn" type="button">Clear</button>
    `;
    host.appendChild(toolbar);

    const options = FILTER_OPTIONS[key] || [];
    const selected = new Set(SELECTED[key] || []);
    const isLocation = key === "location";
    const customOptions = isLocation ? options.filter(opt => opt.startsWith("custom:")) : [];
    const baseOptions = isLocation ? options.filter(opt => !opt.startsWith("custom:")) : options;

    const makeCheckbox = (opt) => {
      const id = `${key}__${opt.replace(/\s+/g,'_')}`;
      const wrap = document.createElement("label");
      wrap.className = "checkbox";
      wrap.innerHTML = `<input type="checkbox" class="panel-opt" id="${id}" ${selected.has(opt)?'checked':''} data-value="${opt}"> <span>${opt}</span>`;
      return wrap;
    };

    if(isLocation){
      if(baseOptions.length){
        const header = document.createElement("div");
        header.className = "muted custom-section-title";
        header.textContent = "Provinces";
        host.appendChild(header);
        const baseWrap = document.createElement("div");
        baseWrap.className = "grid";
        baseOptions.forEach(opt => baseWrap.appendChild(makeCheckbox(opt)));
        host.appendChild(baseWrap);
      }

      const customHeader = document.createElement("div");
      customHeader.className = "muted custom-section-title";
      customHeader.textContent = "Custom areas";
      host.appendChild(customHeader);

      if(customOptions.length){
        const customWrap = document.createElement("div");
        customWrap.className = "custom-aoi-list";
        customOptions.forEach(opt => {
          const row = document.createElement("div");
          row.className = "custom-aoi-row";
          const checkbox = makeCheckbox(opt);
          checkbox.classList.add("custom-aoi-checkbox");
          row.appendChild(checkbox);
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "custom-remove-btn";
          btn.textContent = "Remove";
          btn.dataset.label = opt;
          btn.addEventListener("click", (evt) => {
            evt.preventDefault();
            evt.stopPropagation();
            removeCustomArea(opt);
          });
          row.appendChild(btn);
          customWrap.appendChild(row);
        });
        host.appendChild(customWrap);
      } else {
        const none = document.createElement("div");
        none.className = "muted custom-empty";
        none.textContent = "No custom areas uploaded yet.";
        host.appendChild(none);
      }
    } else {
      const optionsWrap = document.createElement("div");
      optionsWrap.className = "grid";
      baseOptions.forEach(opt => optionsWrap.appendChild(makeCheckbox(opt)));
      host.appendChild(optionsWrap);
    }

    toolbar.querySelector("#selectAllBtn").addEventListener("click", () => {
      host.querySelectorAll("input.panel-opt").forEach(ch => ch.checked = true);
    });
    toolbar.querySelector("#clearAllBtn").addEventListener("click", () => {
      host.querySelectorAll("input.panel-opt").forEach(ch => ch.checked = false);
    });

    if(isLocation){
      $("#panelHint").textContent = "Tick provinces or custom AOIs. Use Remove to delete uploads.";
    } else {
      $("#panelHint").textContent = "Use Select all / Clear, then Save selection.";
    }
  }

  $("#dashboard").classList.add("hidden");
  $("#panel").classList.remove("hidden");
}

function closePanel(save=false){
  if(save && ACTIVE_KEY){
    const meta = getMeta(ACTIVE_KEY);
    if(meta.type === "number"){
      const mn = ($("#wnumMin")?.value || "").trim();
      const mx = ($("#wnumMax")?.value || "").trim();
      const norm = (v) => (v.toLowerCase && v.toLowerCase() === "x") ? "" : v;
      const a = norm(mn), b = norm(mx);
      SELECTED[ACTIVE_KEY] = (!a && !b) ? [] : [a, b];
    } else if (meta.type === "sbi"){
      ensureSbiState();
      const payload = baseSbiSelection();
      SBI_BUCKETS.forEach(({id}) => {
        const textarea = document.querySelector(`#panelOptions textarea.sbi-codes[data-bucket="${id}"]`);
        const select = document.querySelector(`#panelOptions select.sbi-file-select[data-bucket="${id}"]`);
        payload[id].codes = parseSbiManualCodes(textarea ? textarea.value : "");
        const selectedFile = select && typeof select.value === "string" ? select.value.trim() : "";
        payload[id].file = selectedFile ? selectedFile : null;
      });
      SELECTED[ACTIVE_KEY] = payload;
    } else if (meta.type === "group"){
      if(ACTIVE_KEY === 'overige'){
        const tokens = [];
        const dmin = (document.getElementById('dateMin')?.value || '').trim();
        const dmax = (document.getElementById('dateMax')?.value || '').trim();
        if(dmin) tokens.push('date_min=' + dmin);
        if(dmax) tokens.push('date_max=' + dmax);
        document.querySelectorAll('#panelOptions .panel-opt-bool').forEach(ch => {
          if(ch.checked) tokens.push(ch.dataset.token);
        });
        SELECTED[ACTIVE_KEY] = tokens;
      } else {
        const tokens = [];
        $("#panelOptions").querySelectorAll("input.panel-opt-gd").forEach(ch => {
          if(ch.checked) tokens.push(ch.dataset.token);
        });
        $("#panelOptions").querySelectorAll("input.panel-opt-bool").forEach(ch => {
          if(ch.checked) tokens.push(ch.dataset.token);
        });
        const omin = ($("#oppMin")?.value || "").trim();
        const omax = ($("#oppMax")?.value || "").trim();
        const norm = (v) => (v.toLowerCase && v.toLowerCase() === "x") ? "" : v;
        const a = norm(omin), b = norm(omax);
        if(a) tokens.push("oppmin=" + a);
        if(b) tokens.push("oppmax=" + b);
        SELECTED[ACTIVE_KEY] = tokens;
      }
    } else {
      const chosen = [];
      $("#panelOptions").querySelectorAll("input.panel-opt").forEach(ch => {
        if(ch.checked) chosen.push(ch.dataset.value);
      });
      SELECTED[ACTIVE_KEY] = chosen;
    }
    markPreviewDirty();
  }
  ACTIVE_KEY = null;
  updateAoiUploaderVisibility(null);
  $("#panel").classList.add("hidden");
  $("#dashboard").classList.remove("hidden");
  renderDashboard();
}

async function loadFilters(){
  const res = await fetch(API("/api/filters"));
  const data = await res.json();
  FILTERS_META = data.filters || [];
  FILTER_OPTIONS = data.options || {};
  FILTERS_META.forEach(f => {
    if(SELECTED[f.key] === undefined){
      if(f.type === "sbi"){
        SELECTED[f.key] = baseSbiSelection();
      } else {
        SELECTED[f.key] = [];
      }
    } else if(f.type === "sbi"){
      SELECTED[f.key] = normalizeSbiSelection(SELECTED[f.key]);
    }
  });
  if(Array.isArray(SELECTED.location)){
    const valid = new Set(FILTER_OPTIONS.location || []);
    SELECTED.location = SELECTED.location.filter(v => valid.has(v));
  }
  await refreshSbiFiles();
  renderDashboard();
  markPreviewDirty();
}

async function doPreview(){
  $("#previewOut").textContent = "…";
  let success = false;
  try{
    const res = await fetch(API("/api/preview"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        selected: SELECTED,
        advanced: getAdvancedPayload(),
      })
    });
    const data = await res.json();
    if(!res.ok || (data && data.ok === false)){
      throw new Error((data && data.error) || res.statusText || "Preview failed");
    }
    const count = typeof data.count === "number" ? data.count : Number.parseInt(data.count, 10) || 0;
    LAST_PREVIEW_COUNT = count;
    $("#previewOut").textContent = `${count.toLocaleString()} rows`;
    if(ADVANCED_STATE.filterDuplicates){
      const folder = ADVANCED_STATE.duplicatesPath;
      const msg = folder ? `Filtering duplicates from: ${folder}` : "Filtering duplicates enabled.";
      setAdvancedStatus(msg, false);
    } else {
      setAdvancedStatus("");
    }
    PREVIEW_DIRTY = false;
    updateAnalysisButtonState();
    success = true;
  }catch(err){
    console.error('Preview failed', err);
    LAST_PREVIEW_COUNT = null;
    $("#previewOut").textContent = "Error";
    setAdvancedStatus(err && err.message ? err.message : 'Preview failed', true);
    PREVIEW_DIRTY = true;
    updateAnalysisButtonState();
  }
  updateCustomSaveEstimate();
  return success;
}

async function doDownload(){
  try{
    const res = await fetch(API("/api/download"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        selected: SELECTED,
        advanced: getAdvancedPayload(),
      })
    });
    if(!res.ok){
      let message = res.statusText || 'Download failed';
      try{
        const data = await res.json();
        message = data && data.error ? data.error : message;
      }catch(_err){
        // ignore body parse errors
      }
      setAdvancedStatus(message, true);
      alert('Download failed: ' + message);
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "filtered_results.csv";
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }catch(err){
    console.error('Download failed', err);
    setAdvancedStatus(err && err.message ? err.message : 'Download failed', true);
  }
}

function defaultPreferenceName(){
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `preference_${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

async function createPreference(){
  const suggested = defaultPreferenceName();
  const input = window.prompt("Enter a name for this preference (stored under bigdata/preferences):", suggested);
  if(input === null){
    return;
  }
  const name = (input.trim() || suggested).replace(/[\\/]/g, "");
  const payload = {
    name,
    selected: SELECTED,
    advanced: getAdvancedPayload(),
  };
  try{
    const res = await fetch(API("/api/preferences/create"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if(!res.ok || (data && data.ok === false)){
      throw new Error((data && data.error) || res.statusText || "Failed to save preference");
    }
    const filename = data.file || name;
    alert(`Preference saved as ${filename}.`);
  }catch(err){
    console.error("Failed to create preference", err);
    alert(`Save failed: ${err && err.message ? err.message : err}`);
  }
}

async function applyPreferencePayload(payload){
  const incoming = payload && typeof payload.selected === "object" && payload.selected !== null ? payload.selected : {};
  const nextSelected = {};
  Object.keys(incoming).forEach((key) => {
    nextSelected[key] = incoming[key];
  });
  SELECTED = nextSelected;

  if(Array.isArray(FILTERS_META)){
    FILTERS_META.forEach((meta) => {
      if(SELECTED[meta.key] === undefined){
        if(meta.type === "sbi"){
          SELECTED[meta.key] = baseSbiSelection();
        } else {
          SELECTED[meta.key] = [];
        }
      } else if(meta.type === "sbi"){
        SELECTED[meta.key] = normalizeSbiSelection(SELECTED[meta.key]);
      }
    });
  }
  ensureSbiState();

  if(Array.isArray(SELECTED.location)){
    const valid = new Set(FILTER_OPTIONS.location || []);
    if(valid.size > 0){
      SELECTED.location = SELECTED.location.filter((value) => valid.has(value));
    }
  }

  const advanced = payload && typeof payload.advanced === "object" && payload.advanced !== null ? payload.advanced : {};
  let handledDupPath = false;
  if(Object.prototype.hasOwnProperty.call(advanced, "duplicatesPath")){
    const dupVal = typeof advanced.duplicatesPath === "string" ? advanced.duplicatesPath : "";
    setDuplicatesPath(dupVal);
    const input = document.getElementById("duplicatesPath");
    if(input){
      input.value = dupVal;
    }
    handledDupPath = true;
  }
  if(!handledDupPath){
    setDuplicatesPath("");
    const input = document.getElementById("duplicatesPath");
    if(input){
      input.value = "";
    }
  }

  if(Object.prototype.hasOwnProperty.call(advanced, "filterDuplicates")){
    ADVANCED_STATE.filterDuplicates = Boolean(advanced.filterDuplicates);
  } else {
    ADVANCED_STATE.filterDuplicates = false;
  }
  updateFilterDubsButton();
  if(!ADVANCED_STATE.filterDuplicates){
    setAdvancedStatus("");
  }

  markPreviewDirty();
  renderDashboard();
  updateAnalysisButtonState();
  return await doPreview();
}

function parseSimpleCsv(text){
  if(typeof text !== "string") return [];
  let source = text.replace(/^[\uFEFF]/, "");
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  for(let i = 0; i < source.length; i += 1){
    const char = source[i];
    if(inQuotes){
      if(char === '"'){
        if(source[i + 1] === '"'){
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      continue;
    }
    if(char === '"'){
      inQuotes = true;
      continue;
    }
    if(char === ','){
      row.push(field);
      field = "";
      continue;
    }
    if(char === '\n'){
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if(char === '\r'){
      if(source[i + 1] !== '\n'){
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
      }
      continue;
    }
    field += char;
  }
  if(inQuotes){
    throw new Error("Malformed CSV: unmatched quote");
  }
  if(field !== "" || row.length){
    row.push(field);
    rows.push(row);
  }
  return rows.filter((cells) => cells.some((cell) => cell !== ""));
}

function parsePreferenceCsv(text){
  const rows = parseSimpleCsv(typeof text === "string" ? text : "");
  if(rows.length === 0){
    throw new Error("Preference file is empty");
  }
  const header = rows[0];
  if(header.length < 3){
    throw new Error("Preference file is missing columns");
  }
  const selected = {};
  const advanced = {};
  const meta = {};
  for(let i = 1; i < rows.length; i += 1){
    const row = rows[i];
    if(row.length < 3){
      continue;
    }
    const section = (row[0] || "").trim().toLowerCase();
    const key = row[1];
    const raw = row[2];
    if(section === "selected"){
      try{
        selected[key] = JSON.parse(raw);
      }catch(_err){
        selected[key] = raw;
      }
    } else if(section === "advanced"){
      if(key === "payload"){
        let parsed = {};
        try{
          parsed = JSON.parse(raw);
        }catch(_err){
          parsed = {};
        }
        if(parsed && typeof parsed === "object" && !Array.isArray(parsed)){
          Object.assign(advanced, parsed);
        }
      } else {
        try{
          advanced[key] = JSON.parse(raw);
        }catch(_err){
          advanced[key] = raw;
        }
      }
    } else if(section === "meta"){
      meta[key] = raw;
    }
  }
  return { selected, advanced, meta };
}

async function handlePreferenceFileSelection(event){
  const input = event && event.target ? event.target : null;
  const files = input && input.files ? Array.from(input.files) : [];
  const file = files.length > 0 ? files[0] : null;
  if(!file){
    if(input){
      input.value = "";
    }
    return;
  }
  try{
    if(!Array.isArray(FILTERS_META) || FILTERS_META.length === 0){
      await loadFilters();
    }
    const text = await file.text();
    const payload = parsePreferenceCsv(text);
    const combined = { ...payload, selected: payload.selected || {}, advanced: payload.advanced || {} };
    const success = await applyPreferencePayload(combined);
    if(!success){
      alert(`Preference \u201c${file.name}\u201d loaded, but preview failed. Adjust settings if needed.`);
    } else {
      alert(`Preference \u201c${file.name}\u201d loaded.`);
    }
  }catch(err){
    console.error("Failed to load preference", err);
    alert(`Load failed: ${err && err.message ? err.message : err}`);
  }finally{
    if(input){
      input.value = "";
    }
  }
}

function loadPreference(){
  if(!PREFERENCE_FILE_INPUT){
    PREFERENCE_FILE_INPUT = document.getElementById("preferenceFileInput");
    if(PREFERENCE_FILE_INPUT){
      PREFERENCE_FILE_INPUT.addEventListener("change", handlePreferenceFileSelection);
    }
  }
  if(!PREFERENCE_FILE_INPUT){
    alert("Preference file input is not available.");
    return;
  }
  PREFERENCE_FILE_INPUT.value = "";
  PREFERENCE_FILE_INPUT.click();
}

let CUSTOM_SAVE_COUNTER = 0;

function renderAnalysisOptions(){
  ensureAnalysisSelection();
  const host = document.getElementById("analysisOptions");
  if(!host) return;
  host.innerHTML = "";
  ANALYSIS_DIMENSIONS.forEach((dim) => {
    const disabled = dim.requires ? hasActiveSelection(dim.requires) : false;
    if(dim.always){
      ANALYSIS_SELECTION.add(dim.key);
    }
    if(disabled){
      ANALYSIS_SELECTION.delete(dim.key);
    }
    const label = document.createElement("label");
    label.className = "analysis-option" + (disabled ? " disabled" : "");

    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = dim.key;
    input.checked = ANALYSIS_SELECTION.has(dim.key);
    input.disabled = Boolean(disabled || dim.always);
    input.addEventListener("change", () => {
      if(input.checked){
        ANALYSIS_SELECTION.add(dim.key);
      } else {
        if(dim.always){
          ANALYSIS_SELECTION.add(dim.key);
          input.checked = true;
        } else {
          ANALYSIS_SELECTION.delete(dim.key);
        }
      }
    });
    label.appendChild(input);

    const textWrap = document.createElement("div");
    textWrap.className = "analysis-option-text";
    const title = document.createElement("strong");
    title.textContent = dim.label;
    textWrap.appendChild(title);
    if(dim.always){
      const hint = document.createElement("span");
      hint.className = "muted smallprint";
      hint.textContent = "Always included";
      textWrap.appendChild(hint);
    } else if(disabled && dim.requires){
      const hint = document.createElement("span");
      hint.className = "muted smallprint";
      hint.textContent = "Already filtered";
      textWrap.appendChild(hint);
    }
    label.appendChild(textWrap);

    host.appendChild(label);
  });
}

function openAnalysisModal(){
  if(typeof LAST_PREVIEW_COUNT !== "number" || LAST_PREVIEW_COUNT <= 0 || PREVIEW_DIRTY){
    return;
  }
  renderAnalysisOptions();
  const status = document.getElementById("analysisStatus");
  if(status){
    status.textContent = "";
    status.classList.remove("status-error");
  }
  const modal = document.getElementById("analysisModal");
  if(modal){
    modal.classList.remove("hidden");
  }
}

function closeAnalysisModal(){
  const modal = document.getElementById("analysisModal");
  if(modal){
    modal.classList.add("hidden");
  }
}

function selectAllAnalysisDimensions(){
  ensureAnalysisSelection();
  ANALYSIS_DIMENSIONS.forEach((dim) => {
    const disabled = dim.requires ? hasActiveSelection(dim.requires) : false;
    if(!disabled || dim.always){
      ANALYSIS_SELECTION.add(dim.key);
    }
  });
  renderAnalysisOptions();
}

function clearAnalysisDimensions(){
  ANALYSIS_SELECTION = new Set(["summary"]);
  renderAnalysisOptions();
}

async function runAnalysis(){
  ensureAnalysisSelection();
  const statusEl = document.getElementById("analysisStatus");
  const runBtn = document.getElementById("analysisRun");
  const dims = Array.from(ANALYSIS_SELECTION);
  if(!dims.includes("summary")){
    dims.push("summary");
  }
  if(dims.length === 0){
    if(statusEl){
      statusEl.textContent = "Select at least one breakdown.";
      statusEl.classList.add("status-error");
    }
    return;
  }
  if(statusEl){
    statusEl.textContent = "Running analysis…";
    statusEl.classList.remove("status-error");
  }
  if(runBtn){
    runBtn.disabled = true;
  }

  try{
    const res = await fetch(API("/api/analysis"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        selected: SELECTED,
        advanced: getAdvancedPayload(),
        dimensions: dims,
      }),
    });
    const data = await res.json();
    if(!res.ok || (data && data.ok === false)){
      throw new Error((data && data.error) || res.statusText || "Analysis failed");
    }
    renderAnalysisResults(data);
    if(statusEl){
      const total = typeof data.total_rows === "number" ? data.total_rows : null;
      statusEl.textContent = total !== null ? `Completed for ${formatNumber(total)} row${total === 1 ? "" : "s"}.` : "Analysis completed.";
    }
  }catch(err){
    console.error("Analysis failed", err);
    if(statusEl){
      statusEl.textContent = err && err.message ? err.message : "Analysis failed";
      statusEl.classList.add("status-error");
    }
  }finally{
    if(runBtn){
      runBtn.disabled = false;
    }
  }
}

function renderAnalysisResults(data){
  const host = document.getElementById("analysisResults");
  if(!host){
    return;
  }
  host.innerHTML = "";
  if(!data || typeof data.total_rows !== "number"){
    host.classList.add("hidden");
    return;
  }
  host.classList.remove("hidden");

  const total = data.total_rows;
  const baselineTotal = typeof data.baseline_total_rows === "number" ? data.baseline_total_rows : null;

  const countsWrap = document.createElement("div");
  countsWrap.className = "analysis-counts";

  const filteredCard = document.createElement("div");
  filteredCard.className = "count-card";
  filteredCard.innerHTML = `<div class="count-label">Filtered rows</div><div class="count-value">${formatNumber(total)}</div>`;
  countsWrap.appendChild(filteredCard);

  if(baselineTotal !== null){
    const baselineCard = document.createElement("div");
    baselineCard.className = "count-card";
    baselineCard.innerHTML = `<div class="count-label">Baseline (ALL)</div><div class="count-value">${formatNumber(baselineTotal)}</div>`;
    countsWrap.appendChild(baselineCard);
  }

  host.appendChild(countsWrap);

  const summary = data.summary || {};
  const metrics = Array.isArray(summary.metrics) ? summary.metrics : [];
  const highlights = summary.highlights || {};
  const positives = Array.isArray(highlights.positive) ? highlights.positive : [];
  const negatives = Array.isArray(highlights.negative) ? highlights.negative : [];

  if((positives && positives.length) || (negatives && negatives.length)){
    const highlightWrap = document.createElement("div");
    highlightWrap.className = "analysis-highlights";

    const makeHighlightGroup = (titleText, items) => {
      if(!items || !items.length) return null;
      const group = document.createElement("div");
      group.className = "analysis-highlight-group";
      const title = document.createElement("h4");
      title.textContent = titleText;
      group.appendChild(title);
      const list = document.createElement("div");
      list.className = "analysis-highlight-list";
      items.slice(0, 3).forEach((item) => {
        const row = document.createElement("div");
        row.className = "analysis-highlight";
        const content = document.createElement("div");
        content.className = "analysis-highlight-content";
        const strong = document.createElement("strong");
        strong.textContent = item.label || item.key;
        content.appendChild(strong);
        const detail = document.createElement("span");
        const filteredPct = typeof item.filtered_pct === "number" ? formatPercent(item.filtered_pct) : "–";
        const baselinePct = typeof item.baseline_pct === "number" ? formatPercent(item.baseline_pct) : "–";
        detail.textContent = `${filteredPct} vs ${baselinePct}`;
        content.appendChild(detail);
        row.appendChild(content);
        row.appendChild(createDeltaBadge(item.diff_pct));
        list.appendChild(row);
      });
      group.appendChild(list);
      return group;
    };

    const posGroup = makeHighlightGroup("Biggest increases", positives);
    const negGroup = makeHighlightGroup("Biggest decreases", negatives);
    if(posGroup) highlightWrap.appendChild(posGroup);
    if(negGroup) highlightWrap.appendChild(negGroup);
    if(highlightWrap.children.length){
      host.appendChild(highlightWrap);
    }
  }

  if(metrics.length){
    const block = document.createElement("div");
    block.className = "analysis-block";
    const heading = document.createElement("h4");
    heading.textContent = "Overall signals";
    block.appendChild(heading);
    const table = document.createElement("table");
    table.className = "analysis-table";
    table.innerHTML = `<thead><tr><th>Metric</th><th>Filtered</th><th>Baseline</th><th>Δ</th></tr></thead>`;
    const tbody = document.createElement("tbody");
    metrics.forEach((metric) => {
      const tr = document.createElement("tr");
      const nameTd = document.createElement("td");
      nameTd.textContent = metric.label || metric.key;
      tr.appendChild(nameTd);

      const filteredTd = document.createElement("td");
      const filteredPct = typeof metric.filtered_pct === "number" ? formatPercent(metric.filtered_pct) : "–";
      const filteredAbs = typeof metric.filtered_abs === "number" ? formatNumber(metric.filtered_abs) : "0";
      filteredTd.innerHTML = `<strong>${filteredPct}</strong><div class="muted smallprint">${filteredAbs} rows</div>`;
      tr.appendChild(filteredTd);

      const baselineTd = document.createElement("td");
      const baselinePct = typeof metric.baseline_pct === "number" ? formatPercent(metric.baseline_pct) : "–";
      const expected = typeof metric.expected_abs === "number" ? formatNumber(Math.round(metric.expected_abs)) : "0";
      baselineTd.innerHTML = `<strong>${baselinePct}</strong><div class="muted smallprint">expected ${expected}</div>`;
      tr.appendChild(baselineTd);

      const diffTd = document.createElement("td");
      diffTd.appendChild(createDeltaBadge(metric.diff_pct));
      tr.appendChild(diffTd);

      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    block.appendChild(table);

    const averages = Array.isArray(summary.averages) ? summary.averages.filter(item => item && (item.filtered !== null || item.baseline !== null)) : [];
    if(averages.length){
      const avgHeading = document.createElement("h4");
      avgHeading.textContent = "Averages";
      block.appendChild(avgHeading);
      const avgTable = document.createElement("table");
      avgTable.className = "analysis-table";
      avgTable.innerHTML = `<thead><tr><th>Metric</th><th>Filtered</th><th>Baseline</th><th>Δ</th></tr></thead>`;
      const avgBody = document.createElement("tbody");
      averages.forEach((row) => {
        const tr = document.createElement("tr");
        const nameTd = document.createElement("td");
        nameTd.textContent = row.label || row.key;
        tr.appendChild(nameTd);
        const filtered = (typeof row.filtered === "number" && !Number.isNaN(row.filtered)) ? `${formatNumber(row.filtered, 2)}${row.unit ? ` ${row.unit}` : ""}` : "–";
        const baseline = (typeof row.baseline === "number" && !Number.isNaN(row.baseline)) ? `${formatNumber(row.baseline, 2)}${row.unit ? ` ${row.unit}` : ""}` : "–";
        const diffVal = (typeof row.diff === "number" && !Number.isNaN(row.diff)) ? `${row.diff >= 0 ? "+" : ""}${formatNumber(row.diff, 2)}${row.unit ? ` ${row.unit}` : ""}` : "–";
        const filteredTd = document.createElement("td");
        filteredTd.textContent = filtered;
        const baselineTd = document.createElement("td");
        baselineTd.textContent = baseline;
        const diffTd = document.createElement("td");
        diffTd.textContent = diffVal;
        tr.appendChild(filteredTd);
        tr.appendChild(baselineTd);
        tr.appendChild(diffTd);
        avgBody.appendChild(tr);
      });
      avgTable.appendChild(avgBody);
      block.appendChild(avgTable);
    }

    const multi = summary.multi_kvk || null;
    if(multi){
      const multiRow = document.createElement("div");
      multiRow.className = "analysis-highlight";
      const content = document.createElement("div");
      content.className = "analysis-highlight-content";
      const title = document.createElement("strong");
      title.textContent = "Multi-vestigingen";
      content.appendChild(title);
      const detail = document.createElement("span");
      const multiPct = typeof multi.filtered_pct === "number" ? formatPercent(multi.filtered_pct) : "–";
      const basePct = typeof multi.baseline_pct === "number" ? formatPercent(multi.baseline_pct) : "–";
      const unique = typeof multi.unique === "number" ? formatNumber(multi.unique) : "0";
      const multiCount = typeof multi.multi === "number" ? formatNumber(multi.multi) : "0";
      detail.textContent = `${multiPct} vs ${basePct} (${multiCount} of ${unique} unique KVKs)`;
      content.appendChild(detail);
      if(typeof multi.expected_multi === "number"){
        const expect = document.createElement("span");
        expect.textContent = `Baseline expectation: ${formatNumber(Math.round(multi.expected_multi))}`;
        content.appendChild(expect);
      }
      multiRow.appendChild(content);
      multiRow.appendChild(createDeltaBadge(multi.diff_pct));
      block.appendChild(multiRow);
    }

    const avgDate = summary.avg_oprichtingsdatum || {};
    if(avgDate && (avgDate.filtered || avgDate.baseline)){
      const dateRow = document.createElement("div");
      dateRow.className = "analysis-highlight";
      const content = document.createElement("div");
      content.className = "analysis-highlight-content";
      const title = document.createElement("strong");
      title.textContent = "Gemiddelde oprichtingsdatum";
      content.appendChild(title);
      const detail = document.createElement("span");
      const filteredText = avgDate.filtered || "–";
      const baselineText = avgDate.baseline || "–";
      detail.textContent = `${filteredText} vs ${baselineText}`;
      content.appendChild(detail);
      if(typeof avgDate.diff_days === "number" && avgDate.diff_days !== 0){
        const diffSpan = document.createElement("span");
        const absDays = Math.abs(avgDate.diff_days);
        diffSpan.textContent = avgDate.diff_days > 0 ? `${absDays} dagen later` : `${absDays} dagen eerder`;
        content.appendChild(diffSpan);
      }
      dateRow.appendChild(content);
      if(typeof avgDate.diff_days === "number"){
        const badge = document.createElement("span");
        let cls = "delta delta-neutral";
        if(avgDate.diff_days > 0){
          cls = "delta delta-positive";
        } else if(avgDate.diff_days < 0){
          cls = "delta delta-negative";
        }
        badge.className = cls;
        const sign = avgDate.diff_days > 0 ? "+" : avgDate.diff_days < 0 ? "-" : "";
        badge.textContent = `${sign}${Math.abs(avgDate.diff_days)} dagen`;
        dateRow.appendChild(badge);
      }
      block.appendChild(dateRow);
    }

    host.appendChild(block);
  }

  const groups = data.groups || {};
  Object.entries(groups).forEach(([key, group]) => {
    if(!group || !Array.isArray(group.rows) || !group.rows.length){
      return;
    }
    const block = document.createElement("div");
    block.className = "analysis-block";
    const title = document.createElement("h4");
    title.textContent = group.label || key;
    block.appendChild(title);
    const table = document.createElement("table");
    table.className = "analysis-table";
    table.innerHTML = `<thead><tr><th>Value</th><th>Filtered</th><th>Baseline</th><th>Δ</th></tr></thead>`;
    const tbody = document.createElement("tbody");
    group.rows.forEach((row) => {
      const tr = document.createElement("tr");
      const nameTd = document.createElement("td");
      nameTd.textContent = row.value || "(n/a)";
      tr.appendChild(nameTd);
      const filteredTd = document.createElement("td");
      const filteredPct = typeof row.filtered_pct === "number" ? formatPercent(row.filtered_pct) : "–";
      const filteredAbs = typeof row.filtered_abs === "number" ? formatNumber(row.filtered_abs) : "0";
      filteredTd.innerHTML = `<strong>${filteredPct}</strong><div class="muted smallprint">${filteredAbs} rows</div>`;
      tr.appendChild(filteredTd);
      const baselineTd = document.createElement("td");
      const baselinePct = typeof row.baseline_pct === "number" ? formatPercent(row.baseline_pct) : "–";
      const expected = typeof row.expected_abs === "number" ? formatNumber(Math.round(row.expected_abs)) : "0";
      baselineTd.innerHTML = `<strong>${baselinePct}</strong><div class="muted smallprint">expected ${expected}</div>`;
      tr.appendChild(baselineTd);
      const diffTd = document.createElement("td");
      diffTd.appendChild(createDeltaBadge(row.diff_pct));
      tr.appendChild(diffTd);
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    block.appendChild(table);
    if(typeof group.omitted === "number" && group.omitted > 0){
      const note = document.createElement("div");
      note.className = "analysis-note";
      note.textContent = `+ ${group.omitted} additional entr${group.omitted === 1 ? "y" : "ies"} not shown.`;
      block.appendChild(note);
    }
    host.appendChild(block);
  });

  const warnings = Array.isArray(data.warnings) ? data.warnings.filter(Boolean) : [];
  if(warnings.length){
    const warn = document.createElement("div");
    warn.className = "analysis-warnings";
    warn.innerHTML = `<strong>Warnings</strong>`;
    const list = document.createElement("ul");
    warnings.forEach((msg) => {
      const li = document.createElement("li");
      li.textContent = msg;
      list.appendChild(li);
    });
    warn.appendChild(list);
    host.appendChild(warn);
  }
}

function createCustomSaveDestinationElement(initial = {}){
  CUSTOM_SAVE_COUNTER += 1;
  const dest = document.createElement('div');
  dest.className = 'custom-save-destination';
  dest.dataset.destId = String(CUSTOM_SAVE_COUNTER);
  dest.innerHTML = `
    <div class="custom-save-grid">
      <label class="custom-save-field">
        <span class="label">Save directory</span>
        <input type="text" class="custom-save-dir" placeholder="/path/to/folder" />
      </label>
      <label class="custom-save-field">
        <span class="label">Base filename</span>
        <input type="text" class="custom-save-base" placeholder="results" />
      </label>
      <label class="custom-save-field">
        <span class="label">Max rows per file</span>
        <input type="number" min="1" step="1" class="custom-save-max" placeholder="50000" />
      </label>
      <label class="custom-save-field">
        <span class="label">Amount saved here</span>
        <input type="text" class="custom-save-amount" placeholder="e.g. 150000 or R" />
      </label>
    </div>
    <div class="custom-save-destination-actions">
      <button type="button" class="btn ghost custom-save-remove">Remove</button>
    </div>
  `;

  const dirInput = dest.querySelector('.custom-save-dir');
  if(dirInput && initial.directory){ dirInput.value = initial.directory; }
  const baseInput = dest.querySelector('.custom-save-base');
  if(baseInput && initial.baseName){ baseInput.value = initial.baseName; }
  const maxInput = dest.querySelector('.custom-save-max');
  if(maxInput && initial.maxRows){ maxInput.value = initial.maxRows; }
  const amountInput = dest.querySelector('.custom-save-amount');
  if(amountInput && initial.amount){ amountInput.value = initial.amount; }

  return dest;
}

function ensureCustomSaveDefault(){
  const container = document.getElementById('customSaveDestinations');
  if(!container) return;
  if(container.querySelector('.custom-save-destination')) return;
  container.appendChild(createCustomSaveDestinationElement());
  refreshCustomSaveRemovers();
}

function refreshCustomSaveRemovers(){
  const container = document.getElementById('customSaveDestinations');
  if(!container) return;
  const nodes = Array.from(container.querySelectorAll('.custom-save-destination'));
  nodes.forEach((node, idx) => {
    const removeBtn = node.querySelector('.custom-save-remove');
    if(removeBtn){
      const disable = nodes.length <= 1;
      removeBtn.classList.toggle('hidden', disable);
      removeBtn.disabled = disable;
    }
  });
}

function addCustomSaveDestination(copyDirectory = true){
  const container = document.getElementById('customSaveDestinations');
  if(!container) return;
  ensureCustomSaveDefault();
  const firstDirInput = container.querySelector('.custom-save-destination:first-child .custom-save-dir');
  const initial = {};
  if(copyDirectory && firstDirInput && firstDirInput.value){
    initial.directory = firstDirInput.value;
  }
  const dest = createCustomSaveDestinationElement(initial);
  container.appendChild(dest);
  refreshCustomSaveRemovers();
  updateCustomSaveEstimate();
  setCustomSaveStatus('');
  const focusTarget = dest.querySelector('.custom-save-base');
  if(focusTarget){ focusTarget.focus(); }
}

function parseCustomSaveDestinations(options = {}){
  const requireFull = Boolean(options.requireFull);
  const container = document.getElementById('customSaveDestinations');
  const nodes = Array.from(container ? container.querySelectorAll('.custom-save-destination') : []);
  const destinations = [];
  const errors = [];
  let restCount = 0;
  let hasMissingMax = false;
  let hasMissingAmount = false;

  nodes.forEach((node, idx) => {
    const indexLabel = `Destination ${idx + 1}`;
    const dirInput = node.querySelector('.custom-save-dir');
    const baseInput = node.querySelector('.custom-save-base');
    const maxInput = node.querySelector('.custom-save-max');
    const amountInput = node.querySelector('.custom-save-amount');

    const directory = dirInput && dirInput.value ? dirInput.value.trim() : '';
    const baseName = baseInput && baseInput.value ? baseInput.value.trim() : '';
    const maxRowsRaw = maxInput && maxInput.value ? maxInput.value.trim() : '';
    const amountRaw = amountInput && amountInput.value ? amountInput.value.trim() : '';

    if(requireFull && !directory){
      errors.push(`${indexLabel}: Provide a save directory.`);
    }
    if(requireFull && !baseName){
      errors.push(`${indexLabel}: Provide a base filename.`);
    }

    let maxRows = null;
    if(maxRowsRaw){
      const parsedMax = Number.parseInt(maxRowsRaw, 10);
      if(Number.isFinite(parsedMax) && parsedMax > 0){
        maxRows = parsedMax;
      } else {
        errors.push(`${indexLabel}: Max rows per file must be a positive number.`);
      }
    } else if(requireFull){
      errors.push(`${indexLabel}: Max rows per file is required.`);
    } else {
      hasMissingMax = true;
    }

    let rowsRequested = null;
    let isRest = false;
    if(amountRaw){
      if(amountRaw.toUpperCase() === 'R'){
        isRest = true;
        restCount += 1;
      } else {
        const parsedAmount = Number.parseInt(amountRaw, 10);
        if(Number.isFinite(parsedAmount) && parsedAmount > 0){
          rowsRequested = parsedAmount;
        } else {
          errors.push(`${indexLabel}: Amount saved here must be a positive number or R.`);
        }
      }
    } else if(requireFull){
      errors.push(`${indexLabel}: Enter amount saved here or use R for the remainder.`);
    } else {
      hasMissingAmount = true;
    }

    destinations.push({
      directory,
      baseName,
      maxRows,
      rowsRequested,
      isRest,
      amountProvided: Boolean(amountRaw),
    });
  });

  if(restCount > 1){
    errors.push('Only one destination can use R (rest).');
  }

  const totalRequested = destinations.reduce((sum, dest) => sum + (dest.rowsRequested || 0), 0);

  return { destinations, errors, restCount, hasMissingMax, hasMissingAmount, totalRequested };
}

function setCustomSaveVisible(show){
  const panel = document.getElementById('customSavePanel');
  if(!panel) return;
  panel.classList.toggle('hidden', !show);
  if(show){
    ensureCustomSaveDefault();
    refreshCustomSaveRemovers();
    setCustomSaveStatus('');
    updateCustomSaveEstimate();
  }
}

function toggleCustomSavePanel(){
  const panel = document.getElementById('customSavePanel');
  if(!panel) return;
  const willShow = panel.classList.contains('hidden');
  setCustomSaveVisible(willShow);
}

function updateCustomSaveEstimate(){
  const out = document.getElementById('customFileEstimate');
  if(!out) return;
  ensureCustomSaveDefault();
  const parsed = parseCustomSaveDestinations();
  if(parsed.errors.length){
    out.textContent = parsed.errors[0];
    return;
  }
  if(parsed.destinations.length === 0){
    out.textContent = 'Add at least one save destination.';
    return;
  }
  if(parsed.hasMissingMax){
    out.textContent = 'Enter max rows per file for each destination to estimate.';
    return;
  }
  if(parsed.hasMissingAmount){
    out.textContent = 'Enter amount saved here (or R) for each destination to estimate.';
    return;
  }
  if(typeof LAST_PREVIEW_COUNT !== 'number'){
    out.textContent = 'Run a preview to estimate file counts.';
    return;
  }

  const totalRows = LAST_PREVIEW_COUNT;
  let remaining = totalRows;
  const entries = parsed.destinations.map(dest => ({ dest, rows: 0 }));
  const fixedIndices = entries.map((entry, idx) => entry.dest.isRest ? null : idx).filter(idx => idx !== null);
  fixedIndices.forEach(idx => {
    const entry = entries[idx];
    const allowed = entry.dest.rowsRequested || 0;
    const take = Math.min(allowed, Math.max(remaining, 0));
    entry.rows = take;
    remaining = Math.max(remaining - take, 0);
  });
  const restIndices = entries.map((entry, idx) => entry.dest.isRest ? idx : null).filter(idx => idx !== null);
  restIndices.forEach(idx => {
    const entry = entries[idx];
    entry.rows = Math.max(remaining, 0);
    remaining = 0;
  });

  const lines = [`Preview rows: ${totalRows.toLocaleString()}`];
  entries.forEach((entry, idx) => {
    const { dest, rows } = entry;
    const labelBase = dest.baseName || `results${idx + 1}`;
    const label = dest.isRest ? `${labelBase} (rest)` : labelBase;
    const perFile = dest.maxRows && dest.maxRows > 0 ? dest.maxRows : 1;
    const files = rows > 0 ? Math.ceil(rows / perFile) : 0;
    const fileText = `${files} file${files === 1 ? '' : 's'}`;
    const rowText = `${rows.toLocaleString()} row${rows === 1 ? '' : 's'}`;
    lines.push(`${label}: ${fileText} · ${rowText}`);
  });
  if(remaining > 0){
    lines.push(`Not allocated: ${remaining.toLocaleString()} row${remaining === 1 ? '' : 's'}. Add a rest destination or increase the amounts.`);
  }
  out.textContent = lines.join('\n');
}

function setCustomSaveStatus(message, isError = false){
  const statusEl = document.getElementById('customSaveStatus');
  if(!statusEl) return;
  statusEl.textContent = message || '';
  statusEl.classList.toggle('status-error', Boolean(isError));
}

async function doCustomSave(){
  const runBtn = document.getElementById('customSaveRun');
  ensureCustomSaveDefault();
  const parsed = parseCustomSaveDestinations({ requireFull: true });
  if(parsed.errors.length){
    setCustomSaveStatus(parsed.errors[0], true);
    return;
  }
  if(parsed.destinations.length === 0){
    setCustomSaveStatus('Add at least one save destination.', true);
    return;
  }

  const previewTotal = typeof LAST_PREVIEW_COUNT === 'number' ? LAST_PREVIEW_COUNT : null;
  if(previewTotal !== null && parsed.restCount === 0 && parsed.totalRequested < previewTotal){
    setCustomSaveStatus(`Allocated rows (${parsed.totalRequested.toLocaleString()}) are less than the preview count (${previewTotal.toLocaleString()}). Increase the amounts or add an R destination.`, true);
    return;
  }

  const destinationsPayload = parsed.destinations.map(dest => ({
    directory: dest.directory,
    baseName: dest.baseName,
    maxRowsPerFile: dest.maxRows,
    mode: dest.isRest ? 'rest' : 'fixed',
    rows: dest.isRest ? null : dest.rowsRequested,
  }));

  const first = destinationsPayload[0] || null;
  const payload = {
    selected: SELECTED,
    advanced: getAdvancedPayload(),
    destinations: destinationsPayload,
  };
  if(first && first.directory){ payload.directory = first.directory; }
  if(first && first.baseName){ payload.baseName = first.baseName; }
  if(first && typeof first.maxRowsPerFile === 'number'){ payload.maxRowsPerFile = first.maxRowsPerFile; }

  setCustomSaveStatus('Saving…');
  if(runBtn) runBtn.disabled = true;

  try{
    const res = await fetch(API('/api/save'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    let data = {};
    try{ data = await res.json(); } catch(_){ data = {}; }
    if(!res.ok || !data.ok){
      throw new Error(data.error || res.statusText || 'Save failed');
    }
    const files = Array.isArray(data.files) ? data.files : [];
    const created = typeof data.created_files === 'number' ? data.created_files : files.length;
    const totalRows = typeof data.total_rows === 'number' ? data.total_rows : null;
    const destDetails = Array.isArray(data.destinations) ? data.destinations : [];
    const parts = [];
    parts.push(`Saved ${created} file${created === 1 ? '' : 's'}`);
    if(typeof totalRows === 'number'){
      parts.push(`${totalRows.toLocaleString()} rows total`);
    }
    if(destDetails.length > 1){
      const per = destDetails.map(d => {
        const label = d.base_name || d.baseName || 'results';
        const rows = typeof d.rows_written === 'number' ? d.rows_written : (typeof d.rows === 'number' ? d.rows : null);
        if(typeof rows === 'number'){
          return `${label}: ${rows.toLocaleString()} rows`;
        }
        return label;
      });
      if(per.length){
        parts.push(per.join(' · '));
      }
    }
    setCustomSaveStatus(parts.join(' · '));
    if(ADVANCED_STATE.filterDuplicates){
      const folder = ADVANCED_STATE.duplicatesPath;
      if(folder){
        setAdvancedStatus(`Filtering duplicates from: ${folder}`, false);
      }
    }
  }catch(err){
    console.error('Custom save failed', err);
    setCustomSaveStatus('Save failed: ' + (err && err.message ? err.message : err), true);
    setAdvancedStatus(err && err.message ? err.message : 'Save failed', true);
  }finally{
    if(runBtn) runBtn.disabled = false;
  }
}

function ensureLocationSelection(label){
  if(!Array.isArray(SELECTED.location)){
    SELECTED.location = [];
  }
  if(!SELECTED.location.includes(label)){
    SELECTED.location.push(label);
  }
}

async function handleLocationUpload(){
  const fileInput = document.getElementById('aoiFile');
  const uploadBtn = document.getElementById('aoiUploadBtn');
  if(!fileInput || !uploadBtn){
    return;
  }
  if(!fileInput.files || !fileInput.files[0]){
    alert('Pick a .geojson first');
    return;
  }

  const file = fileInput.files[0];
  const originalText = uploadBtn.textContent;
  uploadBtn.disabled = true;
  uploadBtn.textContent = 'Uploading…';

  try{
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch('/api/location/upload', { method: 'POST', body: fd });
    let data = {};
    try { data = await res.json(); } catch(_){ data = {}; }
    if(!res.ok || !data.ok){
      alert('Upload failed: ' + (data.error || res.statusText || 'unknown'));
      return;
    }

    const stem = String(data.stored_as || '').replace(/\.geojson$/i, '');
    const label = 'custom:' + stem;
    ensureLocationSelection(label);

    await loadFilters();
    openPanel('location', true);
  }catch(err){
    console.error('Upload error', err);
    alert('Upload error: ' + (err && err.message ? err.message : err));
  }finally{
    uploadBtn.disabled = false;
    uploadBtn.textContent = originalText;
    if(fileInput) fileInput.value = '';
  }
}

async function removeCustomArea(label){
  if(!label || !label.startsWith('custom:')){
    return;
  }
  const confirmRemoval = window.confirm(`Remove ${label}? This deletes the uploaded file.`);
  if(!confirmRemoval){
    return;
  }
  try{
    const res = await fetch('/api/location/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label })
    });
    let data = {};
    try { data = await res.json(); } catch(_){ data = {}; }
    if(!res.ok || !data.ok){
      alert('Remove failed: ' + (data.error || res.statusText || 'unknown'));
      return;
    }
    if(Array.isArray(SELECTED.location)){
      SELECTED.location = SELECTED.location.filter(v => v !== label);
    }
    await loadFilters();
    openPanel('location', true);
  }catch(err){
    console.error('Remove error', err);
    alert('Remove error: ' + (err && err.message ? err.message : err));
  }
}

document.addEventListener("DOMContentLoaded", () => {
  loadFilters();
  $("#previewBtn")?.addEventListener("click", doPreview);
  $("#downloadBtn")?.addEventListener("click", doDownload);
  $("#createPreferenceBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    createPreference();
  });
  $("#loadPreferenceBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    loadPreference();
  });
  $("#trackingBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    openTrackingModal();
  });
  $("#trackingClose")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    closeTrackingModal();
  });
  $("#trackingCreate")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    runTracking("create");
  });
  $("#trackingUpdate")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    runTracking("update");
  });
  PREFERENCE_FILE_INPUT = document.getElementById("preferenceFileInput");
  if(PREFERENCE_FILE_INPUT){
    PREFERENCE_FILE_INPUT.addEventListener("change", handlePreferenceFileSelection);
  }
  $("#analysisBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    openAnalysisModal();
  });
  $("#analysisRun")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    runAnalysis();
  });
  $("#analysisSelectAll")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    selectAllAnalysisDimensions();
  });
  $("#analysisClear")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    clearAnalysisDimensions();
  });
  $("#analysisClose")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    closeAnalysisModal();
  });
  $("#filterDubsBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    toggleFilterDubs().catch((err) => {
      console.error('Toggle duplicates failed', err);
      setAdvancedStatus(err && err.message ? err.message : 'Failed to toggle duplicates', true);
    });
  });
  $("#customSaveToggle")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    toggleCustomSavePanel();
  });
  $("#customSaveRun")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    doCustomSave();
  });
  $("#customSaveAdd")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    addCustomSaveDestination(true);
  });
  const customContainer = document.getElementById('customSaveDestinations');
  if(customContainer){
    customContainer.addEventListener('input', () => {
      const statusEl = document.getElementById('customSaveStatus');
      if(statusEl && statusEl.classList.contains('status-error')){
        setCustomSaveStatus('');
      }
      updateCustomSaveEstimate();
    });
    customContainer.addEventListener('click', (ev) => {
      const removeBtn = ev.target.closest('.custom-save-remove');
      if(removeBtn){
        ev.preventDefault();
        const parent = removeBtn.closest('.custom-save-destination');
        if(parent){
          parent.remove();
          refreshCustomSaveRemovers();
          updateCustomSaveEstimate();
        }
      }
    });
  }
  $("#backBtn")?.addEventListener("click", () => closePanel(false));
  $("#saveBtn")?.addEventListener("click", () => closePanel(true));
  $("#aoiUploadBtn")?.addEventListener("click", handleLocationUpload);
  updateAoiUploaderVisibility(null);

  const analysisModal = document.getElementById("analysisModal");
  if(analysisModal){
    analysisModal.addEventListener("click", (ev) => {
      if(ev.target === analysisModal){
        closeAnalysisModal();
      }
    });
  }

  ensureCustomSaveDefault();
  refreshCustomSaveRemovers();
  updateCustomSaveEstimate();

  const duplicatesInput = document.getElementById("duplicatesPath");
  if(duplicatesInput){
    try{
      const stored = localStorage.getItem(ADVANCED_PATH_STORAGE_KEY);
      if(stored){
        duplicatesInput.value = stored;
        setDuplicatesPath(stored);
      } else {
        setDuplicatesPath(duplicatesInput.value || "");
      }
    }catch(_err){
      setDuplicatesPath(duplicatesInput.value || "");
    }
    duplicatesInput.addEventListener("input", handleDuplicatesPathInput);
    duplicatesInput.addEventListener("change", handleDuplicatesPathChange);
  } else {
    setDuplicatesPath("");
  }

  updateFilterDubsButton();
  setAdvancedStatus("");
  updateAnalysisButtonState();

  document.body.addEventListener("click", (e) => {
    const xBtn = e.target.closest(".chip-x");
    if(xBtn && xBtn.dataset.clearkey){
      const k = xBtn.dataset.clearkey;
      if(SELECTED[k] !== undefined){
        clearSelection(k);
      }
      renderDashboard();
      return;
    }
    const clrAll = e.target.closest("#clearAllBtn");
    if(clrAll){
      Object.keys(SELECTED).forEach(k => clearSelection(k));
      renderDashboard();
    }
  });
});

window.__CompfilterBooted = true;
