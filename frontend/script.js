const $ = (sel) => document.querySelector(sel);
const API = (path) => `${location.origin}${path}`;

let FILTERS_META = [];     // [{key,label,type}]
let FILTER_OPTIONS = {};   // key -> [options]  (for group: these are gebruiksdoel choices)
let SELECTED = {};         // key -> [] OR ["min","max"] OR tokens for group
let ACTIVE_KEY = null;

function getMeta(key){ return FILTERS_META.find(f=>f.key===key) || {key, label:key, type:"multiselect"}; }

function summarizeSelection(key){
  const meta = getMeta(key);
  const sel = SELECTED[key] || [];
  if(meta.type==="number"){
    const mn = sel[0] ?? "";
    const mx = sel[1] ?? "";
    if(!mn && !mx) return "x";
    if(mn && mx) return `${mn}–${mx}`;
    if(mn) return `≥ ${mn}`;
    return `≤ ${mx}`;
  }
  if(meta.type==="group"){
    // tokens: gd=..., hv=TRUE/FALSE, nm=TRUE/FALSE, oppmin=, oppmax=
    let gd = sel.filter(t => t.startsWith("gd=")).length;
    const hv = sel.find(t => t==="hv=TRUE" || t==="hv=FALSE");
    const nm = sel.find(t => t==="nm=TRUE" || t==="nm=FALSE");
    const omin = sel.find(t => t.startsWith("oppmin="));
    const omax = sel.find(t => t.startsWith("oppmax="));
    let parts = [];
    if(gd>0) parts.push(`gd:${gd}`);
    if(hv) parts.push(hv.replace("hv=","hv:"));
    if(nm) parts.push(nm.replace("nm=","nm:"));
    if(omin || omax){
      const a = omin ? omin.split("=")[1] : "";
      const b = omax ? omax.split("=")[1] : "";
      parts.push(a && b ? `opp:${a}–${b}` : (a ? `opp:≥ ${a}` : `opp:≤ ${b}`));
    }
    return parts.length ? parts.join(" · ") : "x";
  }
  // multiselect
  return `${sel.length} selected`;
}

function renderDashboard(){
  const btnHost=$("#filterButtons"); btnHost.innerHTML="";
  FILTERS_META.forEach(f=>{
    const btn=document.createElement("button");
    btn.className="filter-btn";
    btn.dataset.key=f.key;
    const detail = summarizeSelection(f.key);
    btn.innerHTML=`<span>${f.label}</span><span class="badge">${detail}</span>`;
    btn.addEventListener("click",()=>openPanel(f.key));
    btnHost.appendChild(btn);
  });

  // chips summary with per-filter X and Clear all
  const s=$("#activeSummary");
  const chips = [];
  Object.entries(SELECTED).forEach(([k,v])=>{
    if(!v || v.length===0) return;
    const meta = getMeta(k);
    chips.push(`<span class="chip" data-key="${k}">${meta.label}: ${summarizeSelection(k)}<button class="chip-x" data-clearkey="${k}" title="Clear">×</button></span>`);
  });
  const hasAny = chips.length>0;
  s.innerHTML = `<div class="chips">${chips.join(" ")}${hasAny ? '<button class="chip clear-all" id="clearAllBtn">Clear all</button>' : ''}</div>`;
}

function openPanel(key){
  // Toggle: if same button clicked while panel is open, close without saving
  const panelEl = document.getElementById("panel");
  if (ACTIVE_KEY === key && panelEl && !panelEl.classList.contains("hidden")) {
    closePanel(false);
    return;
  }

  ACTIVE_KEY=key;
  const meta=getMeta(key);
  $("#panelTitle").textContent=meta.label;
  const host=$("#panelOptions");
  host.innerHTML="";

  if(meta.type==="number"){
    const current = SELECTED[key] || [];
    const curMin = current[0] || "";
    const curMax = current[1] || "";
    const wrap=document.createElement("div");
    wrap.innerHTML=`
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
    $("#panelHint").textContent="Row kept if [row_min,row_max] overlaps your [Min, Max]. 999999999 is treated as ∞.";
  } else if (meta.type==="group") {
    if (key === 'overige') {
      const current = new Set(SELECTED[key]||[]);
      const wrap=document.createElement('div');
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
      // Fill day/month/year selects with fast scrollable options
      __fillDateSelects('Min', SELECTED[key]);
      __fillDateSelects('Max', SELECTED[key]);
    } else {
    // VESTIGING PANEL
    const options = FILTER_OPTIONS[key] || []; // gebruiksdoel list
    const current = new Set(SELECTED[key] || []);

    // Toolbar
    const toolbar=document.createElement("div");
    toolbar.className="mini-actions";
    toolbar.innerHTML = `
      <strong>Gebruiksdoel</strong>
      <div class="spacer"></div>
      <button id="selectAllBtn" type="button">Select all</button>
      <button id="clearAllBtn" type="button">Clear</button>
    `;
    host.appendChild(toolbar);

    // Gebruiksdoel options
    const gdWrap=document.createElement("div");
    gdWrap.className="grid";
    options.forEach(opt=>{
      const token = "gd=" + opt;
      const id = `gd__${opt.replace(/\s+/g,'_')}`;
      const wrap=document.createElement("label"); wrap.className="checkbox";
      wrap.innerHTML = `<input type="checkbox" class="panel-opt-gd" id="${id}" ${current.has(token)?'checked':''} data-token="${token}"> <span>${opt}</span>`;
      gdWrap.appendChild(wrap);
    });
    host.appendChild(gdWrap);

    // Booleans
    const bools=document.createElement("div");
    bools.className="grid";
    bools.style.gridTemplateColumns="repeat(auto-fill,minmax(220px,1fr))";
    bools.innerHTML = `
      <div class="card" style="padding:12px;">
        <div class="muted" style="margin-bottom:6px;">Hoofdvestiging</div>
        <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="hv=TRUE" ${current.has("hv=TRUE")?'checked':''}> TRUE</label>
        <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="hv=FALSE" ${current.has("hv=FALSE")?'checked':''}> FALSE</label>
        <div class="muted" style="font-size:12px; margin-top:6px;">(Pick both to ignore)</div>
      </div>
      <div class="card" style="padding:12px;">
        <div class="muted" style="margin-bottom:6px;">KVK non-mailing</div>
        <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="nm=TRUE" ${current.has("nm=TRUE")?'checked':''}> TRUE</label>
        <label class="checkbox"><input type="checkbox" class="panel-opt-bool" data-token="nm=FALSE" ${current.has("nm=FALSE")?'checked':''}> FALSE</label>
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

    // Wire toolbar
    toolbar.querySelector("#selectAllBtn").addEventListener("click", ()=>{
      host.querySelectorAll("input.panel-opt-gd").forEach(ch => ch.checked = true);
    });
    toolbar.querySelector("#clearAllBtn").addEventListener("click", ()=>{
      host.querySelectorAll("input.panel-opt-gd").forEach(ch => ch.checked = false);
    });

    $("#panelHint").textContent="Pick gebruiksdoel + flags + range as needed. Save selection to apply.";
  } else {
    // default multiselect
    const toolbar=document.createElement("div");
    toolbar.className="mini-actions";
    toolbar.innerHTML = `
      <strong>Quick actions</strong>
      <div class="spacer"></div>
      <button id="selectAllBtn" type="button">Select all</button>
      <button id="clearAllBtn" type="button">Clear</button>
    `;
    host.appendChild(toolbar);

    const optionsWrap=document.createElement("div");
    optionsWrap.className="grid";
    const options=FILTER_OPTIONS[key]||[];
    const selected=new Set(SELECTED[key]||[]);
    options.forEach(opt=>{
      const id = `${key}__${opt.replace(/\s+/g,'_')}`;
      const wrap=document.createElement("label"); wrap.className="checkbox";
      wrap.innerHTML = `<input type="checkbox" class="panel-opt" id="${id}" ${selected.has(opt)?'checked':''} data-value="${opt}"> <span>${opt}</span>`;
      optionsWrap.appendChild(wrap);
    });
    host.appendChild(optionsWrap);

    toolbar.querySelector("#selectAllBtn").addEventListener("click", ()=>{
      host.querySelectorAll("input.panel-opt").forEach(ch => ch.checked = true);
    });
    toolbar.querySelector("#clearAllBtn").addEventListener("click", ()=>{
      host.querySelectorAll("input.panel-opt").forEach(ch => ch.checked = false);
    });

    $("#panelHint").textContent="Use Select all / Clear, then Save selection.";
  }

  $("#dashboard").classList.add("hidden");
  $("#panel").classList.remove("hidden");
}

function closePanel(save=false){
  if(save && ACTIVE_KEY){
    const meta=getMeta(ACTIVE_KEY);
    if(meta.type==="number"){
      const mn = ($("#wnumMin")?.value || "").trim();
      const mx = ($("#wnumMax")?.value || "").trim();
      const norm = (v)=> (v.toLowerCase && v.toLowerCase()==="x") ? "" : v;
      const a = norm(mn), b = norm(mx);
      if(a==="" && b==="") SELECTED[ACTIVE_KEY]=[];
      else SELECTED[ACTIVE_KEY]=[a,b];
    } else if (meta.type==="group"){
      const tokens = [];
      // gebruiksdoel
      $("#panelOptions").querySelectorAll("input.panel-opt-gd").forEach(ch=>{
        if(ch.checked) tokens.push(ch.dataset.token);
      });
      // booleans
      $("#panelOptions").querySelectorAll("input.panel-opt-bool").forEach(ch=>{
        if(ch.checked) tokens.push(ch.dataset.token);
      });
      // opp range
      const omin = ($("#oppMin")?.value || "").trim();
      const omax = ($("#oppMax")?.value || "").trim();
      const norm = (v)=> (v.toLowerCase && v.toLowerCase()==="x") ? "" : v;
      const a = norm(omin), b = norm(omax);
      if(a) tokens.push("oppmin=" + a);
      if(b) tokens.push("oppmax=" + b);
      SELECTED[ACTIVE_KEY] = tokens;
    } else {
      const chosen=[];
      $("#panelOptions").querySelectorAll("input.panel-opt").forEach(ch=>{ if(ch.checked) chosen.push(ch.dataset.value); });
      SELECTED[ACTIVE_KEY]=chosen;
    }
  }
  ACTIVE_KEY=null;
  $("#panel").classList.add("hidden");
  $("#dashboard").classList.remove("hidden");
  renderDashboard();
}

async function loadFilters(){
  const res=await fetch(API("/api/filters"));
  const data=await res.json();
  FILTERS_META=data.filters||[];
  FILTER_OPTIONS=data.options||{};
  FILTERS_META.forEach(f=>{ if(!SELECTED[f.key]) SELECTED[f.key]=[]; });
  renderDashboard();
}

async function doPreview(){
  $("#previewOut").textContent="…";
  const res=await fetch(API("/api/preview"),{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({selected:SELECTED})
  });
  const data=await res.json();
  $("#previewOut").textContent=`${data.count.toLocaleString()} rows`;
}

async function doDownload(){
  const res=await fetch(API("/api/download"),{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({selected:SELECTED})
  });
  const blob=await res.blob();
  const url=URL.createObjectURL(blob);
  const a=document.createElement("a");
  a.href=url; a.download="filtered_results.csv";
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

function __fillDateSelects(suffix, tokens){
  // parse existing tokens like date_min=YYYY-MM-DD / date_max=YYYY-MM-DD
  const key = suffix==='Min' ? 'date_min=' : 'date_max=';
  const tok = (tokens||[]).find(t=>t.startsWith(key)) || '';
  let y='',m='',d='';
  if(tok){ const v = tok.split('=')[1]; [y,m,d] = v.split('-'); }
  const days  = Array.from({length:31},(_,i)=>String(i+1).padStart(2,'0'));
  const months = [
    ['01','januari'],['02','februari'],['03','maart'],['04','april'],['05','mei'],['06','juni'],
    ['07','juli'],['08','augustus'],['09','september'],['10','oktober'],['11','november'],['12','december']
  ];
  const currentYear = new Date().getFullYear();
  const years = [];
  for(let yy=currentYear; yy>=1900; yy--){ years.push(String(yy)); }
  const selD = document.getElementById('date'+suffix+'Day');
  const selM = document.getElementById('date'+suffix+'Month');
  const selY = document.getElementById('date'+suffix+'Year');
  const fill = (el, items, useLabel=false)=>{
    el.innerHTML = '<option value="">x</option>' + items.map(([v,l])=>`<option value="${v}">${useLabel?l:v}</option>`).join('');
  };
  fill(selD, days.map(v=>[v,v]));
  fill(selM, months, true);
  fill(selY, years.map(v=>[v,v]));
  if(d) selD.value=d; if(m) selM.value=m; if(y) selY.value=y;
}

function __readDateFromSelects(prefix){
  const d = document.getElementById('date'+prefix+'Day')?.value||'';
  const m = document.getElementById('date'+prefix+'Month')?.value||'';
  const y = document.getElementById('date'+prefix+'Year')?.value||'';
  if(!y && !m && !d) return '';
  const dd = d||'01';
  const mm = m||'01';
  const yy = y||'1900';
  return `${yy}-${mm}-${dd}`;
}

document.addEventListener("DOMContentLoaded",()=>{
  loadFilters();
  $("#previewBtn").addEventListener("click",doPreview);
  $("#downloadBtn").addEventListener("click",doDownload);
  $("#backBtn").addEventListener("click",()=>closePanel(false));
  $("#saveBtn").addEventListener("click",()=>closePanel(true));

  // Delegated chip clear handlers (and Clear all)
  document.body.addEventListener("click",(e)=>{
    const xBtn = e.target.closest(".chip-x");
    if(xBtn && xBtn.dataset.clearkey){
      const k = xBtn.dataset.clearkey;
      if(SELECTED[k]) SELECTED[k] = [];
      renderDashboard();
      return;
    }
    const clrAll = e.target.closest("#clearAllBtn");
    if(clrAll){
      Object.keys(SELECTED).forEach(k => SELECTED[k]=[]);
      renderDashboard();
      return;
    }
  });
});
window.__CompfilterBooted = true;

/* CF_CUSTOM_AOI_WRAPPER */
(function(){
  if (typeof openPanel !== 'function') return;
  const __origOpenPanel = openPanel;
  window.openPanel = function(key){
    __origOpenPanel(key);
    try{
      if(key !== 'location') return;
      const host = document.querySelector('#panelOptions');
      if(!host) return;
      if(host.querySelector('#aoiUploadBtn')) return; // already injected
      const uploader = document.createElement('div');
      uploader.className = 'card';
      uploader.style.padding = '12px';
      uploader.innerHTML = `
        <div class="muted" style="margin-bottom:6px;">Custom area (GeoJSON, EPSG:4326)</div>
        <div class="grid" style="grid-template-columns:1fr auto;gap:8px;">
          <input id="aoiFile" type="file" accept=".geojson,application/geo+json" />
          <button id="aoiUploadBtn" type="button">Upload</button>
        </div>
        <div class="muted" style="font-size:12px;margin-top:6px;">After upload, your area appears as <em>custom:&lt;filename&gt;</em> in the list below.</div>
      `;
      host.prepend(uploader);
      uploader.querySelector('#aoiUploadBtn').addEventListener('click', async ()=>{
        const input = uploader.querySelector('#aoiFile');
        if(!input.files || !input.files[0]){ alert('Pick a .geojson first'); return; }
        const fd = new FormData(); fd.append('file', input.files[0]);
        const res = await fetch('/api/location/upload', {method:'POST', body: fd});
        const data = await res.json();
        if(!data.ok){ alert('Upload failed: ' + (data.error||'unknown')); return; }
        // reload filters to pick up new custom:* entry
        const fres = await fetch('/api/filters');
        const fdata = await fres.json();
        window.FILTERS_META = fdata.filters||[];
        window.FILTER_OPTIONS = fdata.options||{};
        // reopen location panel
        openPanel('location');
      });
    }catch(e){ console.warn('Custom AOI UI inject failed', e); }
  };
})();

/* CF_CUSTOM_AOI_OBSERVER */
(function(){
  function ensureUploader(){
    try{
      const panel = document.getElementById('panel');
      if(!panel || panel.classList.contains('hidden')) return;
      const titleEl = document.getElementById('panelTitle');
      if(!titleEl) return;
      const title = (titleEl.textContent||'').trim();
      if(title !== 'Location') return;

      const host = document.getElementById('panelOptions');
      if(!host || host.querySelector('#aoiUploadBtn')) return; // already present

      const uploader = document.createElement('div');
      uploader.className = 'card';
      uploader.style.padding = '12px';
      uploader.innerHTML = `
        <div class="muted" style="margin-bottom:6px;">Custom area (GeoJSON, EPSG:4326)</div>
        <div class="grid" style="grid-template-columns:1fr auto;gap:8px;">
          <input id="aoiFile" type="file" accept=".geojson,application/geo+json" />
          <button id="aoiUploadBtn" type="button">Upload</button>
        </div>
        <div class="muted" style="font-size:12px;margin-top:6px;">
          After upload, your area appears as <em>custom:&lt;filename&gt;</em> below. Tick it and Save.
        </div>
      `;
      host.prepend(uploader);

      uploader.querySelector('#aoiUploadBtn').addEventListener('click', async ()=>{
        const input = uploader.querySelector('#aoiFile');
        if(!input.files || !input.files[0]){ alert('Pick a .geojson first'); return; }
        const fd = new FormData(); fd.append('file', input.files[0]);
        const res = await fetch('/api/location/upload', {method:'POST', body: fd});
        let data = {};
        try { data = await res.json(); } catch(e){}
        if(!res.ok || !data.ok){
          alert('Upload failed: ' + (data.error || res.statusText || 'unknown')); 
          return;
        }
        // reload filters so custom:* shows up
        const fres = await fetch('/api/filters');
        const fjson = await fres.json();
        window.FILTERS_META = fjson.filters || [];
        window.FILTER_OPTIONS = fjson.options || {};
        // reopen Location to refresh the list
        if (typeof openPanel === 'function') openPanel('location');
        else {
          // fallback: switch to dashboard then click Location button again
          document.getElementById('backBtn')?.click();
          const btn = document.querySelector('button.filter-btn[data-key="location"]');
          btn?.click();
        }
      });
    }catch(e){ console.warn('AOI inject error', e); }
  }

  const mo = new MutationObserver(()=> ensureUploader());
  mo.observe(document.body, {subtree:true, childList:true, attributes:true, attributeFilter:['class']});
  document.addEventListener('DOMContentLoaded', ensureUploader);
})();


/* CF_CUSTOM_AOI_CLICK_INJECTOR */
(function(){
  function injectUploaderIfLocationPanel(){
    try{
      const panel = document.getElementById('panel');
      const titleEl = document.getElementById('panelTitle');
      const host = document.getElementById('panelOptions');
      if(!panel || panel.classList.contains('hidden') || !titleEl || !host) return;
      const title = (titleEl.textContent||'').trim();
      if(title !== 'Location') return;
      if(host.querySelector('#aoiUploadBtn')) return; // already injected

      const uploader = document.createElement('div');
      uploader.className = 'card';
      uploader.style.padding = '12px';
      uploader.innerHTML = `
        <div class="muted" style="margin-bottom:6px;">Custom area (GeoJSON, EPSG:4326)</div>
        <div class="grid" style="grid-template-columns:1fr auto;gap:8px;">
          <input id="aoiFile" type="file" accept=".geojson,application/geo+json" />
          <button id="aoiUploadBtn" type="button">Upload</button>
        </div>
        <div class="muted" style="font-size:12px;margin-top:6px;">
          After upload, your area appears below as <em>custom:&lt;filename&gt;</em>. Tick it and Save.
        </div>
      `;
      host.prepend(uploader);

      uploader.querySelector('#aoiUploadBtn').addEventListener('click', async ()=>{
        const input = uploader.querySelector('#aoiFile');
        if(!input.files || !input.files[0]){ alert('Pick a .geojson first'); return; }
        const fd = new FormData(); fd.append('file', input.files[0]);
        const res = await fetch('/api/location/upload', {method:'POST', body: fd});
        let data = {};
        try { data = await res.json(); } catch(e){}
        if(!res.ok || !data.ok){
          alert('Upload failed: ' + (data.error || res.statusText || 'unknown'));
          return;
        }
        // Reload filters so the new custom:* entry appears
        const fres = await fetch('/api/filters');
        const fjson = await fres.json();
        window.FILTERS_META = fjson.filters || [];
        window.FILTER_OPTIONS = fjson.options || {};
        // Re-open Location to refresh the list
        const back = document.getElementById('backBtn');
        back?.click();
        const locBtn = document.querySelector('button.filter-btn[data-key="location"]');
        locBtn?.click();
      });
    }catch(e){ console.warn('AOI inject error', e); }
  }

  // 1) Inject when user clicks the Location filter button
  document.addEventListener('click', (e)=>{
    const btn = e.target.closest('button.filter-btn[data-key="location"]');
    if(!btn) return;
    // Panel renders asynchronously; give it a tick then inject
    setTimeout(injectUploaderIfLocationPanel, 50);
  });

  // 2) Also try whenever the panel shows/changes
  const mo = new MutationObserver(()=> setTimeout(injectUploaderIfLocationPanel, 10));
  mo.observe(document.body, {subtree:true, childList:true, attributes:true, attributeFilter:['class']});

  // 3) On initial load
  document.addEventListener('DOMContentLoaded', ()=> setTimeout(injectUploaderIfLocationPanel, 50));
})();


/* CF_STATIC_UPLOADER_WIRING */
(function(){
  function updateAoiUploaderVisibility(){
    const panel = document.getElementById('panel');
    const titleEl = document.getElementById('panelTitle');
    const box = document.getElementById('aoiUploaderContainer');
    if(!panel || !titleEl || !box) return;
    const isLocation = (titleEl.textContent||'').trim() === 'Location';
    box.classList.toggle('hidden', !isLocation);
  }

  async function uploadCustomAoi(){
    const input = document.getElementById('aoiFile');
    if(!input || !input.files || !input.files[0]){
      alert('Pick a .geojson first');
      return;
    }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    const res = await fetch('/api/location/upload', { method:'POST', body: fd });
    let data = {};
    try { data = await res.json(); } catch(e){}
    if(!res.ok || !data.ok){
      alert('Upload failed: ' + (data.error || res.statusText || 'unknown'));
      return;
    }
    // Refresh available filter options so custom:* appears
    const fres = await fetch('/api/filters');
    const fjson = await fres.json();
    window.FILTERS_META = fjson.filters || [];
    window.FILTER_OPTIONS = fjson.options || [];

    // Re-open the Location panel to refresh the checklist
    const back = document.getElementById('backBtn');
    back?.click();
    const locBtn = document.querySelector('button.filter-btn[data-key="location"]');
    locBtn?.click();
  }

  // Hook up button
  document.addEventListener('DOMContentLoaded', ()=>{
    const btn = document.getElementById('aoiUploadBtn');
    if(btn) btn.addEventListener('click', uploadCustomAoi);
    updateAoiUploaderVisibility();
  });

  // When user opens any panel, adjust visibility
  document.addEventListener('click', (e)=>{
    const btn = e.target.closest('button.filter-btn');
    if(!btn) return;
    // The panel opens asynchronously; wait then toggle visibility
    setTimeout(updateAoiUploaderVisibility, 60);
  });

  // Also watch for panel header/title changes
  const mo = new MutationObserver(()=> setTimeout(updateAoiUploaderVisibility, 10));
  mo.observe(document.body, {subtree:true, childList:true, attributes:true, attributeFilter:['class']});
})();

/* CF_UPLOAD_HANDLER_MINI */
(function(){
  async function refreshFilters(){
    const fres = await fetch('/api/filters');
    const fjson = await fres.json();
    window.FILTERS_META = fjson.filters || [];
    window.FILTER_OPTIONS = fjson.options || {};
  }
  function ensureSelectedLocation(label){
    window.SELECTED = window.SELECTED || {};
    if(!Array.isArray(window.SELECTED['location'])) window.SELECTED['location'] = [];
    const arr = window.SELECTED['location'];
    if(!arr.includes(label)) arr.push(label);
  }
  async function uploadCustomAoiSimple(){
    const input = document.getElementById('aoiFile');
    const btn = document.getElementById('aoiUploadBtn');
    if(!input || !input.files || !input.files[0]){ alert('Pick a .geojson first'); return; }
    const file = input.files[0];
    if(btn){ btn.disabled = true; btn.textContent = 'Uploading…'; }
    try{
      const fd = new FormData(); fd.append('file', file);
      const res = await fetch('/api/location/upload', { method:'POST', body: fd });
      let data={}; try{ data = await res.json(); }catch(e){}
      if(!res.ok || !data.ok){ alert('Upload failed: ' + (data.error || res.statusText || 'unknown')); return; }
      const stem = String(data.stored_as||'').replace(/\.geojson$/i,'');
      const label = 'custom:' + stem;
      await refreshFilters();
      ensureSelectedLocation(label);
      // reopen Location to show item checked
      document.getElementById('backBtn')?.click();
      document.querySelector('button.filter-btn[data-key="location"]')?.click();
      if(input) input.value='';
    }finally{
      if(btn){ btn.disabled = false; btn.textContent = 'Upload'; }
    }
  }
  // bind (rebind) whenever the panel renders
  function wire(){
    const btn = document.getElementById('aoiUploadBtn');
    if(btn && !btn.__wired){ btn.addEventListener('click', uploadCustomAoiSimple); btn.__wired = true; }
  }
  document.addEventListener('DOMContentLoaded', wire);
  const mo = new MutationObserver(()=> setTimeout(wire, 20));
  mo.observe(document.body, {subtree:true, childList:true});
})();


/* CF_UPLOAD_HANDLER_FORCE_VISIBLE */
(function(){
  async function _refreshFilters(){
    try{
      const res = await fetch('/api/filters');
      const data = await res.json();
      if (data && data.options) {
        window.FILTER_OPTIONS = data.options;
      }
      if (data && data.filters) {
        window.FILTERS_META = data.filters;
      }
    } catch(e) { /* ignore */ }
  }
  function _ensureSelectedLocation(label){
    window.SELECTED = window.SELECTED || {};
    if(!Array.isArray(window.SELECTED['location'])) window.SELECTED['location'] = [];
    const arr = window.SELECTED['location'];
    if(!arr.includes(label)) arr.push(label);
  }
  function _ensureOptionListed(label){
    window.FILTER_OPTIONS = window.FILTER_OPTIONS || {};
    if(!Array.isArray(window.FILTER_OPTIONS['location'])) window.FILTER_OPTIONS['location'] = [];
    const opts = window.FILTER_OPTIONS['location'];
    if(!opts.includes(label)) opts.push(label);
  }
  async function _uploadAndExpose(){
    const input = document.getElementById('aoiFile');
    const btn = document.getElementById('aoiUploadBtn');
    if(!input || !input.files || !input.files[0]){ alert('Pick a .geojson first'); return; }
    const file = input.files[0];
    const original = btn ? btn.textContent : '';
    if(btn){ btn.disabled = true; btn.textContent = 'Uploading…'; }
    try{
      const fd = new FormData(); fd.append('file', file);
      const res = await fetch('/api/location/upload', {method:'POST', body: fd});
      let data={}; try{ data = await res.json(); }catch(e){}
      if(!res.ok || !data.ok){ alert('Upload failed: ' + (data.error || res.statusText || 'unknown')); return; }
      // derive label custom:<stem>
      const stem = String(data.stored_as||'').replace(/\.geojson$/i,'');
      const label = 'custom:' + stem;

      // 1) try server refresh (uses your invalidate_cache)
      await _refreshFilters();

      // 2) regardless of server response, make it visible now
      _ensureOptionListed(label);
      _ensureSelectedLocation(label);

      // 3) re-open Location so the new option shows up CHECKED
      document.getElementById('backBtn')?.click();
      document.querySelector('button.filter-btn[data-key="location"]')?.click();

      // clear input, restore button
      if (input) input.value='';
      if (btn) btn.textContent = 'Upload';
    }catch(e){
      console.warn('Upload error', e);
      alert('Upload error: ' + (e && e.message ? e.message : e));
    }finally{
      if(btn){ btn.disabled = false; if(btn.textContent==='Uploading…') btn.textContent = original || 'Upload'; }
    }
  }

  function _wire(){
    const btn = document.getElementById('aoiUploadBtn');
    if(btn && !btn.__wired){ btn.addEventListener('click', _uploadAndExpose); btn.__wired = true; }
  }
  document.addEventListener('DOMContentLoaded', _wire);
  const mo = new MutationObserver(()=> setTimeout(_wire, 20));
  mo.observe(document.body, {subtree:true, childList:true});
})();



/* CF_UPLOAD_HANDLER_ALWAYS_WIRED */
(function(){
  async function refreshFilters(){
    console.log('[AOI] refreshing /api/filters …');
    const res = await fetch('/api/filters');
    const data = await res.json();
    window.FILTERS_META   = data.filters || [];
    window.FILTER_OPTIONS = data.options || {};
    console.log('[AOI] filters refreshed; location options:', (window.FILTER_OPTIONS.location||[]).length);
  }
  function ensureSelectedLocation(label){
    window.SELECTED = window.SELECTED || {};
    if(!Array.isArray(window.SELECTED.location)) window.SELECTED.location = [];
    if(!window.SELECTED.location.includes(label)) window.SELECTED.location.push(label);
  }
  function ensureOptionListed(label){
    window.FILTER_OPTIONS = window.FILTER_OPTIONS || {};
    if(!Array.isArray(window.FILTER_OPTIONS.location)) window.FILTER_OPTIONS.location = [];
    const arr = window.FILTER_OPTIONS.location;
    if(!arr.includes(label)) arr.push(label);
  }
  async function doUpload(){
    const fileInput = document.getElementById('aoiFile');
    const btn = document.getElementById('aoiUploadBtn');
    if(!fileInput || !fileInput.files || !fileInput.files[0]){ alert('Pick a .geojson first'); return; }
    const file = fileInput.files[0];

    if(btn){ btn.disabled = true; btn.textContent = 'Uploading…'; }
    console.log('[AOI] uploading:', file.name);

    try{
      const fd = new FormData();
      fd.append('file', file);
      const res = await fetch('/api/location/upload', { method:'POST', body: fd });
      let data = {};
      try { data = await res.json(); } catch(e){}
      console.log('[AOI] upload response:', res.status, data);

      if(!res.ok || !data.ok){
        alert('Upload failed: ' + (data.error || res.statusText || 'unknown'));
        return;
      }

      const stem  = String(data.stored_as||'').replace(/\.geojson$/i,'');
      const label = 'custom:' + stem;

      // refresh server options (location_filter now invalidates cache server-side)
      await refreshFilters();

      // also force-show client-side in case caching interfered
      ensureOptionListed(label);
      ensureSelectedLocation(label);

      // Reopen Location so you see it CHECKED
      document.getElementById('backBtn')?.click();
      document.querySelector('button.filter-btn[data-key=\"location\"]')?.click();

      // Reset UI
      fileInput.value = '';
      if(btn) btn.textContent = 'Upload';
      console.log('[AOI] done; label =', label);
    }catch(err){
      console.error('[AOI] upload error', err);
      alert('Upload error: ' + (err && err.message ? err.message : err));
    }finally{
      if(btn){ btn.disabled = false; }
    }
  }

  function wireUpload(){
    const btn = document.getElementById('aoiUploadBtn');
    if(btn && !btn.__wired){
      btn.addEventListener('click', doUpload);
      btn.__wired = true;
      console.log('[AOI] upload button wired');
    }
  }

  document.addEventListener('DOMContentLoaded', wireUpload);
  // Rewire on any DOM changes (panel opens, etc.)
  const mo = new MutationObserver(()=> setTimeout(wireUpload, 30));
  mo.observe(document.body, {subtree:true, childList:true});
  // Also rewire when any filter button is clicked
  document.addEventListener('click', (e)=>{
    if(e.target.closest('button.filter-btn')) setTimeout(wireUpload, 50);
  });
})();

