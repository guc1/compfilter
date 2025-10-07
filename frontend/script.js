const $ = (sel) => document.querySelector(sel);
const API = (path) => `${location.origin}${path}`;

let FILTERS_META = [];     // [{key,label,type}]
let FILTER_OPTIONS = {};   // key -> [options]
let SELECTED = {};         // key -> [] OR custom per filter
let ACTIVE_KEY = null;
let SBI_FILES = { main: [], sub: [], all: [] };

const SBI_BUCKETS = [
  { id: "main", label: "Main SBI" },
  { id: "sub",  label: "Sub SBI"  },
  { id: "all",  label: "All SBI"  }
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
}

async function doPreview(){
  $("#previewOut").textContent = "…";
  const res = await fetch(API("/api/preview"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selected: SELECTED })
  });
  const data = await res.json();
  $("#previewOut").textContent = `${data.count.toLocaleString()} rows`;
}

async function doDownload(){
  const res = await fetch(API("/api/download"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selected: SELECTED })
  });
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "filtered_results.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
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
  $("#backBtn")?.addEventListener("click", () => closePanel(false));
  $("#saveBtn")?.addEventListener("click", () => closePanel(true));
  $("#aoiUploadBtn")?.addEventListener("click", handleLocationUpload);
  updateAoiUploaderVisibility(null);

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
