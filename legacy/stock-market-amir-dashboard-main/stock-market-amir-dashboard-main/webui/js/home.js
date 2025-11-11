
// webui/js/home.js
// === keep old import path exactly as before ===
import { renderTagSelect } from "./components/tag-select.js";

const symbols = [
  "TATAMOTORS","ETERNAL","JIOFIN","HAL","LICI",
  "BAJFINANCE","JSWENERGY","RECLTD","BAJAJHFL","SWIGGY"
];
const tbody = document.getElementById("terminalBody");

// shared dock below table (existing element from terminal.html)
const dock = document.getElementById("chartsDock");

// --- NEW: create (or reuse) a Super-Path dock that sits above charts ---
let superDock = document.getElementById("superDock");
if (!superDock) {
  superDock = document.createElement("div");
  superDock.id = "superDock";
  superDock.className = "dock";
  // place it right before the charts dock to appear “above charts”
  if (dock && dock.parentElement) {
    dock.parentElement.insertBefore(superDock, dock);
  } else {
    // fallback: append below table panel if chartsDock missing
    const panel = document.querySelector(".panel");
    if (panel) panel.appendChild(superDock);
  }
  superDock.innerHTML = `<div class="meta">Select Previous Day Context · Open Location · Opening Trend to see Super-Path Composite.</div>`;
}

function makeRow(sym){
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td class="col-stock"><span class="stock">${sym}</span></td>
    <td class="col-tags"><div id="${sym}-tags"></div></td>
    <td class="col-indicator" id="${sym}-indicator"><span class="pill">—</span></td>
    <td class="col-actions">
      <div class="row-actions">
        <button class="btn btn-sm btn-charts">Charts</button>
        <button class="btn" id="${sym}-btn-match">Matching Days</button>
      </div>
    </td>
  `;
  tbody.appendChild(tr);

  // render tag selectors (PDC → OL → OT)
  const mount = tr.querySelector(`#${sym}-tags`);
  renderTagSelect(mount);

  // update indicator on tag changes
  let debounce = null;
  mount.addEventListener("tagsChanged", (ev)=>{
    const { PDC, OL, OT } = ev.detail || {};
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(()=> updateIndicator(sym, OT, OL, PDC), 120);
  });

  // --- NEW: render Super-Path as soon as all three tags are chosen ---
  mount.addEventListener("tagsChanged", async (ev)=>{
    const { PDC, OL, OT } = ev.detail || {};
    if (PDC && OL && OT){
      await renderSuperPath(sym, { OT, OL, PDC });
    } else {
      superDock.innerHTML = `<div class="meta">Select Previous Day Context · Open Location · Opening Trend to see Super-Path Composite.</div>`;
    }
  });

  tr.querySelector(`#${sym}-btn-match`).addEventListener("click", async ()=>{
    const { PDC, OL, OT } = currentTags(mount);
    if (!(PDC && OL && OT)){
      dock.innerHTML = `<div class="meta">Select Previous Day Context · Open Location · Opening Trend first.</div>`;
      return;
    }
    dock.innerHTML = `<div class="meta">Loading matching days…</div>`;
    const q = new URLSearchParams({ symbol:sym, ot:OT, ol:OL, pdc:PDC }).toString();
    const j = await fetch(`/api/matches?${q}`, { cache:"no-store" }).then(r=>r.json());
    const rows = j.rows || [];
    if (!rows.length){
      dock.innerHTML = `<div class="meta">${sym} · No exact 3-tag matches.</div>`;
      return;
    }
    // --- sort newest first ---
    rows.sort((a, b) => {
      const ad = String(a.Date || a.date || a.SessionDate || a.session_date || "");
      const bd = String(b.Date || b.date || b.SessionDate || b.session_date || "");
      const lex = bd.localeCompare(ad);
      if (lex !== 0) return lex;
      const at = Date.parse(ad) || 0, bt = Date.parse(bd) || 0;
      return bt - at;
    });

    const tbl = document.createElement("table");
    tbl.className = "matches-table";
    tbl.innerHTML = `
      <thead><tr>
        <th>Date</th>
        <th>Previous Day Context</th>
        <th>Open Location</th>
        <th>Opening Trend</th>
        <th>First Candle Type</th>
        <th>Range Status</th>
        <th>Result</th>
      </tr></thead>
      <tbody></tbody>
    `;
    const tb = tbl.querySelector("tbody");
    for (const r of rows){
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.Date||"-"}</td>
        <td>${r.PDC || "-"}</td>
        <td>${r.OL  || "-"}</td>
        <td>${r.OT  || "-"}</td>
        <td>${r.FCT || "-"}</td>
        <td>${r.RS  || "-"}</td>
        <td>${r.Result || "-"}</td>
      `;
      tb.appendChild(tr);
    }
    dock.innerHTML = ``;
    dock.appendChild(tbl);
  });
}

function currentTags(mount){
  const selects = mount.querySelectorAll("select.tag-dd");
  const out = {};
  selects.forEach(s => out[s.dataset.name] = s.value || "");
  return out;
}

async function updateIndicator(sym, ot, ol, pdc){
  const el = document.getElementById(`${sym}-indicator`);
  if (!(ot && ol && pdc)){
    el.innerHTML = `<span class="pill">ABSTAIN</span> <span class="meta">select PDC · OL · OT</span>`;
    return;
  }
  el.innerHTML = `<span class="pill loading">…</span>`;
  try{
    const q = new URLSearchParams({ symbol:sym, ot, ol, pdc }).toString();
    const j = await fetch(`/api/freq3?${q}`, { cache:"no-store" }).then(r=>r.json());
    const { pick, conf_pct, total, level } = j;
    el.innerHTML = `
      <span class="pill ${pick.toLowerCase()}">${pick}</span>
      <span class="meta">${conf_pct}% · N=${total} · ${level}</span>
    `;
  }catch(e){
    el.innerHTML = `<span class="pill error">ERR</span>`;
  }
}

// ------- charts helpers (unchanged) -------
async function fetchJSON(url){
  const r = await fetch(url, { cache: "no-store" });
  if(!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

async function fetchDates(symbol, OT, OL, PDC){
  const q = new URLSearchParams({ symbol: symbol, ot: OT, ol: OL, pdc: PDC }).toString();
  const data = await fetchJSON(`/api/matches?${q}`);

  if (Array.isArray(data?.dates) && data.dates.length) {
    return data.dates;
  }
  if (Array.isArray(data?.rows) && data.rows.length) {
    const dateKey = ["Date","date","SessionDate","session_date"].find(k => k in data.rows[0]) || null;
    if (dateKey){
      const uniq = Array.from(new Set(data.rows.map(r => String(r[dateKey]).slice(0,10))));
      return uniq;
    }
  }
  return [];
}

function getTagsFor(sym){
  const root = document.getElementById(`${sym}-tags`);
  if(!root) return { PDC:"", OL:"", OT:"" };
  const pick = n => {
    const el = root.querySelector(`select.tag-dd[data-name="${n}"]`);
    return el ? (el.value || "") : "";
  };
  return { PDC: pick("PDC"), OL: pick("OL"), OT: pick("OT") };
}

// delegated click handler – only for row "Charts" buttons (unchanged)
document.addEventListener("click", async (ev) => {
  const el = ev.target.closest("button");
  if (!el) return;

  const isCharts = el.dataset?.action === "charts" || el.classList.contains("btn-charts");
  if (!isCharts) return;

  const tr = el.closest("tr");
  const symEl = tr?.querySelector(".col-stock .stock");
  const sym = symEl?.textContent?.trim();
  if (!sym) return;

  const { PDC, OL, OT } = getTagsFor(sym);
  if (!(PDC && OL && OT)) {
    dock.innerHTML = `
      <div class="notice">Select <b>Previous Day Context</b>, <b>Open Location</b>, and <b>Opening Trend</b> first.</div>
    `;
    return;
  }

  dock.innerHTML = `<div class="notice">Loading charts for ${sym}…</div>`;
  try {
    let dates = await fetchDates(sym, OT, OL, PDC);
    dates = (dates || []).sort((a, b) => b.localeCompare(a));

    const { renderCharts } = await import("./charts/fast_charts.js");
    dock.innerHTML = `<div id="chartsMount" class="dock-inner"></div>`;
    const mount = document.getElementById("chartsMount");
    await renderCharts(sym, mount, dates);
  } catch (err) {
    console.error(err);
    dock.innerHTML = `<div class="notice error">Charts failed: ${String(err.message || err)}</div>`;
  }
});

// init (unchanged)
symbols.forEach(makeRow);

// ---- NEW: Super-Path fetch + draw (cone + slope + stats) ----
async function renderSuperPath(symbol, tags){
  try{
    const q = new URLSearchParams({ symbol, ot: tags.OT, ol: tags.OL, pdc: tags.PDC }).toString();
    const r = await fetch(`/api/superpath?${q}`, { cache:"no-store" });
    const j = await r.json();

    if (j.error){
      superDock.innerHTML = `<div class="meta">Composite error: ${j.error} · stage=${j.stage||"?"}</div>`;
      return;
    }

    const meta = j.meta || {};
    if (!meta.N){
      superDock.innerHTML = `<div class="meta">${symbol} · No composite data for this tag combo yet.</div>`;
      return;
    }

    // top info band + split layout
    superDock.innerHTML = `
      <div class="charts-toolbar">
        <div class="meta"><b>Super-Path</b> (${symbol})
          · N=${meta.N} · Bias <b>${meta.bias}</b>
          · End ⌀ <b>${meta.mean_end}%</b>
          · Confidence ${meta.confidence}%</div>
      </div>
      <div class="super-wrap">
        <div class="chart-card super-chart-card">
          <canvas id="superCanvas" class="chart-canvas"></canvas>
        </div>
        <div class="cue-card" id="cueCard">
          <div class="cue-head">Decision Cue</div>
          <div class="cue-row"><span class="cue-kv">Bias</span><span id="cueBias">—</span></div>
          <div class="cue-row"><span class="cue-kv">Angle</span><span id="cueAngle">—</span></div>
          <div class="cue-row"><span class="cue-kv">Confidence</span><span id="cueConf">—</span></div>
          <div class="cue-row"><span class="cue-kv">Consistency</span><span id="cueCons">—</span></div>
          <div class="cue-row"><span class="cue-kv">Frequency</span><span id="cueFreq">—</span></div>
          <div style="margin:10px 0;">
            <span id="cueFinal" class="cue-pill neutral">Computing…</span>
          </div>
          <div class="cue-notes" id="cueNotes"></div>
        </div>
      </div>
    `;

    // draw chart
    const canvas = document.getElementById("superCanvas");
    // Your draw function currently expects a cone; keep that call:
    drawSuper(canvas, j.cone || [], meta);

    // ---- compute & render the cue (inside the function!) ----
    try {
      // get freq3 reinforcement
      const q2 = new URLSearchParams({ symbol, ot: tags.OT, ol: tags.OL, pdc: tags.PDC }).toString();
      let freq = null;
      try { freq = await fetch(`/api/freq3?${q2}`, { cache:"no-store" }).then(r=>r.json()); } catch (_){}

      // choose bars for cue: prefer j.bars; else derive from cone median
      let barsForCue = Array.isArray(j.bars) && j.bars.length
        ? j.bars
        : (Array.isArray(j.cone) ? j.cone.map(d => ({ mean: +d.med })) : []);

      const cue = computeCueFromBarsAndMeta(barsForCue, meta, freq);
      renderCue(cue);
    } catch (e) {
      console.error("Decision Cue compute failed:", e);
      renderCue({
        bias: (meta.bias || "NEUTRAL"),
        angleDeg: "0.0", angleTxt: "flat",
        conf: (meta.confidence || 0), confTxt: "weak",
        consistency: 0, consTxt: "low",
        fPick: "-", fConf: 0, fN: 0, fLvl: "",
        final: { side: "NO TRADE", tone: "neutral", reason: ["Computation error"] }
      });
    }
  }catch(e){
    superDock.innerHTML = `<div class="meta">Composite error: ${String(e && e.message || e)}</div>`;
  }
}

// Draw median line + IQR cone + slope arrow
function drawSuper(canvas, cone, meta){
  if (!Array.isArray(cone) || !cone.length) return;

  const parent = canvas.parentElement;
  const W = Math.max(560, parent.clientWidth || 640);
  const H = 240;

  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(W * dpr);
  canvas.height = Math.floor(H * dpr);
  canvas.style.width = W + "px";
  canvas.style.height = H + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  const padL = 48, padR = 12, padT = 16, padB = 26;
  const X0 = padL, X1 = W - padR, Y0 = H - padB, Y1 = padT;

  const n = cone.length;
  const med = cone.map(d => +d.med);
  const p25 = cone.map(d => +d.p25);
  const p75 = cone.map(d => +d.p75);

  const lo = Math.min(0, ...p25);
  const hi = Math.max(0, ...p75);
  const kx = (X1 - X0) / Math.max(1, n - 1);
  const ky = (Y0 - Y1) / Math.max(1e-9, (hi - lo));
  const xAt = (i)=> X0 + i*kx;
  const yAt = (v)=> Y0 - (v - lo)*ky;

  // grid + zero line
  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 1;
  [hi, (hi+lo)/2, lo].forEach(v=>{
    const y = yAt(v);
    ctx.beginPath(); ctx.moveTo(X0, y); ctx.lineTo(X1, y); ctx.stroke();
    ctx.fillStyle = "#475569"; ctx.font = "12px Inter, system-ui";
    ctx.fillText(`${Math.round(v*10)/10}%`, 6, y+4);
  });
  ctx.setLineDash([4,4]);
  ctx.strokeStyle = "#cbd5e1";
  ctx.beginPath(); ctx.moveTo(X0, yAt(0)); ctx.lineTo(X1, yAt(0)); ctx.stroke();
  ctx.setLineDash([]);

  // IQR cone (p25..p75)
  ctx.fillStyle = "rgba(37,99,235,0.10)";
  ctx.beginPath();
  for (let i=0;i<n;i++){ const x=xAt(i), y=yAt(p75[i]); if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }
  for (let i=n-1;i>=0;i--){ const x=xAt(i), y=yAt(p25[i]); ctx.lineTo(x,y); }
  ctx.closePath(); ctx.fill();

  // median line
  ctx.strokeStyle = "#2563eb";
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (let i=0;i<n;i++){
    const x=xAt(i), y=yAt(med[i]);
    if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
  }
  ctx.stroke();

  // slope arrow from 09:40 (left) to 15:05 (right) using meta.angle_deg
  const yStart = yAt(0); // anchor at zero for visual neutrality
  const xStart = X0;
  const xEnd = X1;
  // derive yEnd from slope_per_bar for visual; use median ends to bound
  const yEnd = yAt(med[med.length-1]);

  ctx.strokeStyle = "#0f172a";
  ctx.lineWidth = 2;
  ctx.beginPath(); ctx.moveTo(xStart, yStart); ctx.lineTo(xEnd, yEnd); ctx.stroke();

  // arrow head
  const ang = Math.atan2(yEnd - yStart, xEnd - xStart);
  const ah = 8;
  ctx.beginPath();
  ctx.moveTo(xEnd, yEnd);
  ctx.lineTo(xEnd - ah*Math.cos(ang - Math.PI/6), yEnd - ah*Math.sin(ang - Math.PI/6));
  ctx.lineTo(xEnd - ah*Math.cos(ang + Math.PI/6), yEnd - ah*Math.sin(ang + Math.PI/6));
  ctx.closePath();
  ctx.fillStyle = "#0f172a";
  ctx.fill();

  // angle label
  ctx.fillStyle = "#334155";
  ctx.font = "12px Inter, system-ui";
  const label = `${meta.angle_deg}°`;
  ctx.fillText(label, xStart + 8, yStart - 8);
}

function computeCueFromBarsAndMeta(bars, meta, freq){
  // 1) angle from first vs last mean (approx degrees; canvas uses equal Δx per step)
  let angleDeg = 0, consistency = 50, absEnd = 0;
  if (bars && bars.length >= 2){
    const m0 = +bars[0].mean, m1 = +bars[bars.length-1].mean;
    const endChange = m1 - m0; // % over the whole 09:40→15:05 window
    absEnd = Math.abs(endChange);
    // map end-change to a readable degree (so ~0.3% ≈ a few degrees)
    const scale = 0.20; // % that maps to ~11.3°
    angleDeg = Math.atan(endChange / scale) * (180/Math.PI);
    // consistency: corr with monotone time + smoothness = 1 - std of first diffs
    const means = bars.map(b => +b.mean);
    const diffs = means.slice(1).map((v,i)=> v - means[i]);
    const sdDiff = stddev(diffs);
    const normSd = Math.min(1, Math.max(0, sdDiff / 0.6)); // 0.6% as ref
    const smooth = 1 - normSd; // 0..1
    const corr = pearsonWithIndex(means); // -1..1
    consistency = Math.round(100 * Math.max(0, (0.65*Math.abs(corr) + 0.35*smooth)));
  }

  // classify angle purely from degrees (visual slope)
  // 0–12° = flat, 12–25° = moderate, >25° = steep
  const absθ = Math.abs(angleDeg);
  let angleTxt = "flat";
  if (absθ >= 25) angleTxt = "steep";
  else if (absθ >= 12) angleTxt = "moderate";

  // Composite strength from angle (°) and consistency
  // Map angle: 10°→0, 30°→1  (clamped), then blend with consistency
  const slopeComponent = Math.max(0, Math.min(1, (absθ - 10) / 20));
  const compScore = Math.max(0, Math.min(1, 0.6 * slopeComponent + 0.4 * (consistency / 100)));





  // confidence bucket
  const conf = +meta.confidence || 0;
  let confTxt = "weak";
  if (conf >= 70) confTxt = "strong";
  else if (conf >= 40) confTxt = "moderate";

  // consistency bucket
  let consTxt = "low";
  if (consistency >= 70) consTxt = "high";
  else if (consistency >= 40) consTxt = "medium";

  // frequency reinforcement
  const fPick = (freq && (freq.pick||"")).toUpperCase() || "-";
  const fConf = +(freq && freq.conf_pct || 0);
  const fN = +(freq && freq.total || 0);
  const fLvl = (freq && freq.level) || "";

  // final call
  const bias = (meta.bias||"NEUTRAL").toUpperCase();
  const sameSide = (fPick === bias) && (fPick === "BULL" || fPick === "BEAR");

  let final = { side: "NEUTRAL", tone: "neutral", reason: [] };

  // conflict guard: if both sides are strong but opposite
  if (!sameSide && fConf >= 65 && (meta.confidence||0) >= 60) {
    final = { side: "NO TRADE", tone: "neutral", reason: ["Strong conflict: Frequency vs Composite"] };
  } else {
    // Frequency helpers
    const lvl = (fLvl || "").toUpperCase();
    const freqStrong = sameSide && fConf >= 60 && lvl.includes("L3");
    const freqOkay   = sameSide && fConf >= 58;

    // Composite strength from slope + consistency (meta.confidence can stay low)
    const compStrong = compScore >= 0.70;  // clear slope + steady path
    const compOkay   = compScore >= 0.50;  // decent slope/steadiness

    // Entry rules
    let strong = false;
    if (sameSide && (compStrong || (compOkay && freqOkay) || (freqStrong && consistency >= 65))) {
      // Use absEnd to separate strong vs cautious
      strong = (compStrong && absEnd >= 0.20) || (freqStrong && absEnd >= 0.15);
      final.side = strong ? bias : (bias + " (cautious)");
      final.tone = (bias === "BULL") ? "bull" : (bias === "BEAR" ? "bear" : "neutral");
      final.reason.push(
        strong
          ? "Strong edge (slope/consistency and/or L3 frequency)"
          : "Cautious edge (aligned with frequency)"
      );
    } else {
      final.side = "NO TRADE";
      final.tone = "neutral";
      // Add a more specific reason to help tuning
      if (!sameSide && (fConf >= 58)) {
        final.reason.push("Frequency conflicts with composite");
      } else if (compScore < 0.50) {
        final.reason.push("Composite slope/consistency too weak");
      } else {
        final.reason.push("Alignment insufficient for entry");
      }
    }
  }

    
  return {
    bias,
    angleDeg: angleDeg.toFixed(1),
    angleTxt,
    conf, confTxt,
    consistency, consTxt,
    fPick, fConf, fN, fLvl,
    final
  };
}

function renderCue(c){
  // fill rows
  document.getElementById("cueBias").textContent = c.bias;
  document.getElementById("cueAngle").textContent = `${c.angleTxt} (${c.angleDeg}°)`;
  document.getElementById("cueConf").textContent = `${c.conf} (${c.confTxt})`;
  document.getElementById("cueCons").textContent = `${c.consistency} (${c.consTxt})`;
  document.getElementById("cueFreq").textContent =
    (c.fPick && c.fPick !== "-")
      ? `${c.fPick} ${c.fConf}% · ${c.fLvl} · N=${c.fN}`
      : "n/a";

  // pill
  const pill = document.getElementById("cueFinal");
  pill.className = "cue-pill " + (c.final.tone || "neutral");
  pill.textContent = (c.final.side.startsWith("BULL")) ? "Enter BULL"
                   : (c.final.side.startsWith("BEAR")) ? "Enter BEAR"
                   : "No Trade";

  // notes
  document.getElementById("cueNotes").textContent = c.final.reason.join(" · ");
}

// small math helpers
function stddev(arr){
  if (!arr.length) return 0;
  const m = arr.reduce((a,b)=>a+b,0)/arr.length;
  const v = arr.reduce((s,x)=> s + (x-m)*(x-m), 0)/arr.length;
  return Math.sqrt(v);
}
// corr with index [0..n-1]
function pearsonWithIndex(arr){
  const n = arr.length; if (!n) return 0;
  let sx=0, sy=0, sxx=0, syy=0, sxy=0;
  for (let i=0;i<n;i++){
    const x=i, y=arr[i];
    sx+=x; sy+=y; sxx+=x*x; syy+=y*y; sxy+=x*y;
  }
  const num = n*sxy - sx*sy;
  const den = Math.sqrt((n*sxx - sx*sx)*(n*syy - sy*sy));
  return den>1e-12 ? (num/den) : 0;
}
