// Probedge Journal UI (clean rebuild)
// - pulls /api/journal/daily
// - normalizes rows
// - computes Core + Pro indicators (Sharpe/Sortino/Calmar/MDD)
// - renders KPI cards, Equity Curve, Daily R histogram, and the table

// ---------- utils ----------
async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

function toISO(d) {
  if (!d) return "";
  const s = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const m1 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (m1) return `${m1[3]}-${m1[2]}-${m1[1]}`;
  const m2 = s.match(/^(\d{2})-(\d{2})-(\d{4})/);
  if (m2) return `${m2[3]}-${m2[2]}-${m2[1]}`;
  const m3 = s.match(/^(\d{4}-\d{2}-\d{2})[T ]/);
  if (m3) return m3[1];
  const dt = new Date(s);
  if (!isNaN(+dt)) {
    const y = dt.getFullYear();
    const m = String(dt.getMonth() + 1).padStart(2, "0");
    const d2 = String(dt.getDate()).padStart(2, "0");
    return `${y}-${m}-${d2}`;
  }
  return s.slice(0, 10);
}

function fmtRs(v) { return (v ?? 0).toFixed(2); }
function fmtPct(v) { return (v ?? 0).toFixed(2) + "%"; }
function fmtINR(v) { return "₹" + Math.round(v ?? 0).toLocaleString("en-IN"); }

// ---------- backend load ----------
async function loadJournalRows() {
  const j = await fetchJSON("/api/journal/daily");
  const rows = Array.isArray(j?.rows) ? j.rows : [];
  rows.forEach(r => {
    r.Date = toISO(r.Date || r.trade_date);
    r.symbol_std = String(r.symbol_std || r.symbol || "").toUpperCase().trim();

    // numeric normals
    r.pnl_final = Number(r.pnl_final ?? r.pnl_gross_est ?? 0) || 0;
    r.day_R_net = Number(r.day_R_net ?? 0) || 0;
    r.trades_n = Number(r.trades_n ?? 0) || 0;
    r.buy_value = Number(r.buy_value ?? 0) || 0;
    r.sell_value = Number(r.sell_value ?? 0) || 0;

    // tags normalized to strings
    for (const k of ["PrevDayContext","GapType","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","Result"]) {
      r[k] = (r[k] ?? "").toString().trim().toUpperCase();
    }
  });
  // newest first
  rows.sort((a,b)=> String(b.Date).localeCompare(String(a.Date)));
  return rows;
}

// ---------- metric engines ----------
const TRADING_DAYS = 252;

function groupDaily(rows) {
  // Sum per Date (R & ₹ & counts)
  const byDate = new Map();
  for (const r of rows) {
    if (!r.Date) continue;
    const key = r.Date;
    if (!byDate.has(key)) {
      byDate.set(key, { Date: key, pnl: 0, R: 0, trades: 0, buy: 0, sell: 0 });
    }
    const d = byDate.get(key);
    d.pnl += r.pnl_final || 0;
    d.R   += r.day_R_net || 0;
    d.trades += r.trades_n || 0;
    d.buy += r.buy_value || 0;
    d.sell += r.sell_value || 0;
  }
  // ordered asc for equity/MDD math
  return Array.from(byDate.values()).sort((a,b)=> a.Date.localeCompare(b.Date));
}

function kpis(rows) {
  // day-level aggregates for KPI math
  const days = groupDaily(rows);
  const pnlDays = days.map(d => d.pnl);
  const rDays   = days.map(d => d.R);

  const wins   = pnlDays.filter(x=> x>0);
  const losses = pnlDays.filter(x=> x<0);
  const totalTrades = rows.reduce((s,r)=> s + (r.trades_n||0), 0);

  const net_p = pnlDays.reduce((a,b)=>a+b, 0);
  const net_r = rDays.reduce((a,b)=>a+b, 0);
  const nonZeroDays = rDays.filter(x => x !== 0).length;
  const win_rate = (wins.length + losses.length) ? (wins.length / (wins.length + losses.length)) * 100 : 0;
  const expectancy = days.length ? (net_r / days.length) : 0;
  const pf = losses.length ? (wins.reduce((a,b)=>a+b,0) / Math.abs(losses.reduce((a,b)=>a+b,0))) : 0;
  const best = pnlDays.length ? Math.max(...pnlDays) : 0;
  const worst= pnlDays.length ? Math.min(...pnlDays) : 0;

  // Avg win/loss in R
  const winR   = rDays.filter(x=> x>0);
  const lossR  = rDays.filter(x=> x<0);
  const avg_win_r  = winR.length  ? (winR.reduce((a,b)=>a+b,0) / winR.length) : 0;
  const avg_loss_r = lossR.length ? Math.abs(lossR.reduce((a,b)=>a+b,0) / lossR.length) : 0;

  // Pro metrics (Sharpe/Sortino on daily R)
  const mean = rDays.length ? (rDays.reduce((a,b)=>a+b,0) / rDays.length) : 0;
  const variance = rDays.length ? (rDays.reduce((s,x)=> s + Math.pow(x - mean, 2), 0) / rDays.length) : 0;
  const std = Math.sqrt(variance);
  const sharpe = std ? (mean * Math.sqrt(TRADING_DAYS)) / std : 0;

  const downside = rDays.filter(x => x < 0);
  const dmean = downside.length ? (downside.reduce((a,b)=>a+b,0) / downside.length) : 0;
  const dvar  = downside.length ? (downside.reduce((s,x)=> s + Math.pow(x - dmean, 2), 0) / downside.length) : 0;
  const dstd  = Math.sqrt(dvar);
  const sortino = dstd ? (mean * Math.sqrt(TRADING_DAYS)) / dstd : 0;

  // Equity in R (cumsum of daily R), MDD (R)
  let eqR = 0, peak = 0, mdd = 0;
  for (const r of rDays) {
    eqR += r;
    if (eqR > peak) peak = eqR;
    const dd = peak - eqR;
    if (dd > mdd) mdd = dd;
  }

  // Calmar = (annualized R) / MDD
  let calmar = 0;
  if (days.length >= 2) {
    const d0 = new Date(days[0].Date);
    const d1 = new Date(days[days.length-1].Date);
    const spanDays = Math.max(1, Math.round((d1 - d0) / 86400000));
    const years = spanDays / 365.25;
    const cagr_r = years > 0 ? (eqR / years) : 0;
    calmar = mdd > 0 ? (cagr_r / mdd) : 0;
  }

  return {
    net_p, net_r, win_rate, expectancy, days: new Set(days.map(d=>d.Date)).size,
    pf, best, worst, avg_win_r, avg_loss_r, sharpe, sortino, calmar, mdd_r: mdd,
    total_trades_n: totalTrades,
    daysAgg: days, // for charts
  };
}

// ---------- canvas helpers ----------
function makeCanvas(id, h = 220) {
  const c = document.getElementById(id);
  if (!c) return null;
  const parent = c.parentElement || document.body;
  const W = Math.max(320, parent.clientWidth || 600);
  const dpr = window.devicePixelRatio || 1;

  c.width = W * dpr;
  c.height = h * dpr;
  c.style.width = W + "px";
  c.style.height = h + "px";

  const ctx = c.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, h);
  return { ctx, W, H: h };
}

function drawEquityCurve(daysAgg) {
  const fx = makeCanvas("equityCurve", 240);
  if (!fx) return;
  const { ctx, W, H } = fx;

  if (!daysAgg.length) {
    ctx.fillText("No trades", 10, 18);
    return;
  }

  const dates = daysAgg.map(d=> d.Date);
  const vals = daysAgg.map(d=> d.pnl);
  let eq = 0;
  const eqSeries = vals.map(v => (eq += v));

  const pad = 10;
  const X0 = pad, X1 = W - pad;
  const Y0 = H - 25, Y1 = 20;
  const n = dates.length;
  const kx = (X1 - X0) / Math.max(1, n - 1);

  const lo = Math.min(...eqSeries, 0);
  const hi = Math.max(...eqSeries, 0);
  const ky = (Y0 - Y1) / Math.max(1e-9, hi - lo || 1);

  // zero
  const yZero = Y0 - (0 - lo) * ky;
  ctx.strokeStyle = "#e5e7eb";
  ctx.beginPath();
  ctx.moveTo(X0, yZero);
  ctx.lineTo(X1, yZero);
  ctx.stroke();

  // curve
  ctx.strokeStyle = "#334155";
  ctx.lineWidth = 1.6;
  ctx.beginPath();
  eqSeries.forEach((v, i) => {
    const x = X0 + i * kx;
    const y = Y0 - (v - lo) * ky;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function drawRHistogram(daysAgg) {
  const fx = makeCanvas("rHistogram", 240);
  if (!fx) return;
  const { ctx, W, H } = fx;

  if (!daysAgg.length) {
    ctx.fillText("No trades", 10, 18);
    return;
  }

  const buckets = new Map();
  daysAgg.forEach(d => {
    const R = Math.round((d.R || 0) * 2) / 2; // 0.5R bins
    buckets.set(R, (buckets.get(R) || 0) + 1);
  });

  const keys = Array.from(buckets.keys()).sort((a,b)=> a-b);
  const vals = keys.map(k => buckets.get(k));
  const maxV = Math.max(...vals, 1);

  const pad = 10;
  const X0 = pad, X1 = W - pad;
  const Y0 = H - 25, Y1 = 20;
  const bw = (X1 - X0) / Math.max(1, keys.length);

  // axis
  ctx.strokeStyle = "#e5e7eb";
  ctx.beginPath();
  ctx.moveTo(X0, Y0);
  ctx.lineTo(X1, Y0);
  ctx.stroke();

  keys.forEach((k, i) => {
    const count = buckets.get(k);
    const h = (count / maxV) * (Y0 - Y1);
    const x = X0 + i * bw + 2;
    const up = k >= 0;
    ctx.fillStyle = up ? "#16a34a" : "#dc2626";
    ctx.fillRect(x, Y0 - h, Math.max(2, bw - 4), h);
  });

  // zero marker
  const zi = keys.indexOf(0);
  if (zi >= 0) {
    const zx = X0 + zi * bw + bw / 2;
    ctx.strokeStyle = "#94a3b8";
    ctx.beginPath();
    ctx.moveTo(zx, Y1);
    ctx.lineTo(zx, Y0);
    ctx.stroke();
  }
}

// ---------- KPI & table render ----------
function renderKpis(m) {
  const el = document.getElementById("kpiRow");
  if (!el) return;
  el.innerHTML = `
    <div class="summary-grid">
      <div class="card"><div class="k">Net P&amp;L</div><div class="v ${m.net_p>=0?"pos":"neg"}">${fmtINR(m.net_p)}</div></div>
      <div class="card"><div class="k">Net R</div><div class="v ${m.net_r>=0?"pos":"neg"}">${fmtRs(m.net_r)} R</div></div>
      <div class="card"><div class="k">Win%</div><div class="v">${fmtPct(m.win_rate)}</div></div>
      <div class="card"><div class="k">Expectancy</div><div class="v">${fmtRs(m.expectancy)} R/day</div></div>
      <div class="card"><div class="k">Profit Factor</div><div class="v">${fmtRs(m.pf)}</div></div>
      <div class="card"><div class="k">Total Trades</div><div class="v">${(m.total_trades_n||0)}</div></div>

      <div class="card"><div class="k">Sharpe</div><div class="v">${fmtRs(m.sharpe)}</div></div>
      <div class="card"><div class="k">Sortino</div><div class="v">${fmtRs(m.sortino)}</div></div>
      <div class="card"><div class="k">Calmar</div><div class="v">${fmtRs(m.calmar)}</div></div>
      <div class="card"><div class="k">Max Drawdown</div><div class="v">${fmtRs(m.mdd_r)} R</div></div>
      <div class="card"><div class="k">Avg Win R</div><div class="v">${fmtRs(m.avg_win_r)} R</div></div>
      <div class="card"><div class="k">Avg Loss R</div><div class="v">${fmtRs(m.avg_loss_r)} R</div></div>
    </div>
  `;
}

function renderTable(rows) {
  const tb = document.querySelector("#jrnlTable tbody");
  const empty = document.getElementById("jrnlEmpty");
  tb.innerHTML = "";
  if (!rows.length) {
    if (empty) empty.style.display = "block";
    return;
  }
  if (empty) empty.style.display = "none";

  // aggregate by day for table (aligns with KPIs)
  const days = groupDaily(rows);
  days.forEach(d => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${d.Date}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.symbol_std || "-"}</td>
      <td>${d.trades}</td>
      <td>${fmtINR(d.buy)}</td>
      <td>${fmtINR(d.sell)}</td>
      <td class="${d.pnl>=0?"pos":"neg"}">${fmtINR(d.pnl)}</td>
      <td>${fmtRs(d.R)}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.PrevDayContext || "-"}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.GapType || "-"}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.OpenLocation || "-"}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.OpeningTrend || "-"}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.RangeStatus || "-"}</td>
      <td>${rows.find(r=> r.Date===d.Date)?.Result || "-"}</td>
    `;
    tb.appendChild(tr);
  });
}

// ---------- main (filters + render cycle) ----------
(async function main() {
  const errEl = document.getElementById("jrnlErr");
  let all = [];
  let apiPayload = null;
  try {
    apiPayload = await fetchJSON("/api/journal/daily");
    if (apiPayload?.error) {
      if (errEl) {
        errEl.style.display = "block";
        errEl.textContent = "Journal backend: " + apiPayload.error;
      }
    }
    all = Array.isArray(apiPayload?.rows) ? apiPayload.rows : [];
  } catch (err) {
    if (errEl) {
      errEl.style.display = "block";
      errEl.textContent = "Journal API failed: " + String(err.message || err);
    }
    renderKpis({}); drawEquityCurve([]); drawRHistogram([]); renderTable([]);
    return;
  }


  const symSel = document.getElementById("jrnlSymbol");
  const fromEl = document.getElementById("jrnlFrom");
  const toEl   = document.getElementById("jrnlTo");
  const searchEl = document.getElementById("jrnlSearch");

  // symbols
  const symbols = Array.from(new Set(all.map(r=> r.symbol_std).filter(Boolean))).sort();
  for (const s of symbols) {
    const opt = document.createElement("option");
    opt.value = s; opt.textContent = s;
    symSel.appendChild(opt);
  }

  // dates
  const allDates = all.map(r=> r.Date).filter(Boolean).sort();
  const dmin = allDates[0] || "";
  const dmax = allDates[allDates.length-1] || "";
  if (fromEl) fromEl.value = dmin;
  if (toEl)   toEl.value = dmax;

  function applyFilters() {
    let sym = symSel.value || "ALL";
    let from = fromEl.value || dmin;
    let to   = toEl.value   || dmax;
    const q  = (searchEl.value || "").toLowerCase();

    if (from && to && from > to) { const tmp = from; from = to; to = tmp; fromEl.value = from; toEl.value = to; }

    let rows = all.filter(r => {
      if (sym !== "ALL" && r.symbol_std !== sym) return false;
      if (r.Date && from && r.Date < from) return false;
      if (r.Date && to   && r.Date > to)   return false;
      if (q) {
        const text = [
          r.symbol_std, r.PrevDayContext, r.GapType, r.OpenLocation,
          r.OpeningTrend, r.RangeStatus, r.Result
        ].join(" ").toLowerCase();
        if (!text.includes(q)) return false;
      }
      return true;
    });

    // newest first (table later aggregates by day)
    rows = rows.slice().sort((a,b)=> String(b.Date).localeCompare(String(a.Date)));

    const m = kpis(rows);
    renderKpis(m);
    drawEquityCurve(m.daysAgg);
    drawRHistogram(m.daysAgg);
    renderTable(rows);
  }

  symSel.addEventListener("change", applyFilters);
  fromEl.addEventListener("change", applyFilters);
  toEl.addEventListener("change", applyFilters);
  searchEl.addEventListener("input", applyFilters);

  applyFilters();
})();
