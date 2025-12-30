
// webui/js/charts/fast_charts.js
export async function renderCharts(symbol, mount, dates = []) {
  // Build the container (3-per-row grid handled by CSS)
  mount.innerHTML = `
    <div class="charts-toolbar">
      <div class="meta">${symbol} · ${dates.length ? dates.length : "All recent"} day(s)</div>
    </div>
    <div class="charts-wrap" id="chartsGrid"></div>
  `;
  const grid = document.getElementById("chartsGrid");

  // Fetch normalized OHLC CSV from our API (server returns DateTime,Open,High,Low,Close)
  const csv = await fetch(`/api/tm5?symbol=${encodeURIComponent(symbol)}`, { cache:"no-store" })
    .then(r => {
      if(!r.ok) throw new Error(`tm5 fetch failed: ${r.status}`);
      return r.text();
    });

  const rows = parseCSV(csv);
  rows.sort((a,b) => a.DateTime - b.DateTime);

  // Group by YYYY-MM-DD
  const byDay = new Map();
  for (const r of rows) {
    const d = isoDay(r.DateTime);
    if (!byDay.has(d)) byDay.set(d, []);
    byDay.get(d).push(r);
  }

  // Build day list
  const allDaysAsc = Array.from(byDay.keys()).sort();
  
  let pickDays;
  if (Array.isArray(dates) && dates.length) {
    // Use provided list, newest first (defensive sort), and only days we actually have
    pickDays = dates
      .slice()
      .sort((a, b) => b.localeCompare(a))
      .filter(d => byDay.has(d));
  } else {
    // No filter: take last 30 and show newest first
    pickDays = allDaysAsc.slice(-30).reverse();
  }
  
  for (const d of pickDays) {

    const dayRows = byDay.get(d) || [];
    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML = `
      <div class="chart-title">${d}</div>
      <div class="candle-frame">
        <canvas class="chart-canvas"></canvas>
      </div>
    `;
    grid.appendChild(card);

    const canvas = card.querySelector("canvas");
    drawCandles(canvas, dayRows);
  }
}

function parseCSV(text){
  const lines = text.trim().split(/\r?\n/);
  const hdr = lines.shift();
  const cols = hdr.split(",").map(s=>s.trim());
  const idx = Object.fromEntries(cols.map((c,i)=>[c.toLowerCase(), i]));
  const out = [];
  for (const ln of lines){
    if (!ln) continue;
    const a = ln.split(",");
    const dt = new Date(a[idx["datetime"]]);
    if (isNaN(+dt)) continue;
    out.push({
      DateTime: dt,
      Open:  +a[idx["open"]],
      High:  +a[idx["high"]],
      Low:   +a[idx["low"]],
      Close: +a[idx["close"]],
    });
  }
  return out;
}

function isoDay(dt){
  const y = dt.getFullYear();
  const m = String(dt.getMonth()+1).padStart(2,"0");
  const d = String(dt.getDate()).padStart(2,"0");
  return `${y}-${m}-${d}`;
}

function drawCandles(canvas, rows){
  if (!rows || !rows.length) return;

  // Responsive sizing: width from container, fixed analytic height
  const parent = canvas.parentElement;
  const W = Math.max(280, parent.clientWidth || 320);
  const H = 180;

  const dpr = window.devicePixelRatio || 1;
  canvas.width  = Math.floor(W * dpr);
  canvas.height = Math.floor(H * dpr);
  canvas.style.width  = W + "px";
  canvas.style.height = H + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  // Padding
  const padL = 8, padR = 8, padT = 6, padB = 10;

  // Price scale
  const lo = Math.min(...rows.map(r=>r.Low));
  const hi = Math.max(...rows.map(r=>r.High));
  const n  = rows.length;

  const X0 = padL, X1 = W - padR, Y0 = H - padB, Y1 = padT;
  const kx = (X1 - X0) / Math.max(1, n);
  const ky = (Y0 - Y1) / Math.max(1e-9, (hi - lo));

  // Grid lines
  ctx.strokeStyle = "#e9eef5";
  ctx.globalAlpha = 0.25;
  ctx.beginPath();
  for (let i=0;i<4;i++){
    const y = Y1 + (i/3)*(Y0-Y1);
    ctx.moveTo(X0, y); ctx.lineTo(X1, y);
  }
  ctx.stroke();
  ctx.globalAlpha = 1.0;

  // Candles
  for (let i=0;i<n;i++){
    const r = rows[i];
    const x = X0 + i*kx + kx*0.5;
    const yH = Y0 - (r.High  - lo)*ky;
    const yL = Y0 - (r.Low   - lo)*ky;
    const yO = Y0 - (r.Open  - lo)*ky;
    const yC = Y0 - (r.Close - lo)*ky;

    // Wick
    ctx.strokeStyle = "#9aa4b2";
    ctx.beginPath();
    ctx.moveTo(x, yH); ctx.lineTo(x, yL);
    ctx.stroke();

    // Body
    const up = r.Close >= r.Open;
    ctx.fillStyle   = up ? "#2fbf71" : "#ef626a";
    const bw = Math.max(1, Math.floor(kx * 0.6));
    const yTop = Math.min(yO, yC);
    const h    = Math.max(1, Math.abs(yC - yO));
    ctx.fillRect(x - bw/2, yTop, bw, h);
  }
}
