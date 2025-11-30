// webui/js/live.js
// Live terminal frontend for Probedge.

const STATE_POLL_MS = 1000;
const HEALTH_POLL_MS = 5000;

let lastMerged = null;
let selectedSymbol = null;

// --- HTTP helpers ---

async function getJSON(url) {
  const resp = await fetch(url, { cache: "no-store" });
  if (!resp.ok) {
    throw new Error(`GET ${url} -> ${resp.status}`);
  }
  return resp.json();
}

// --- Merge backend structures ---

function normalizeTagsFromPlan(plan) {
  // Backend sends:
  //   plan.tags = { OpeningTrend, OpenLocation, PrevDayContext }
  // or older variants.
  const t = plan.tags || {};

  const pdc =
    t.PDC ??
    t.pdc ??
    t.PrevDayContext ??
    t.prev_day_context ??
    t.prevDayContext ??
    null;

  const ol =
    t.OL ??
    t.ol ??
    t.OpenLocation ??
    t.open_location ??
    null;

  const ot =
    t.OT ??
    t.ot ??
    t.OpeningTrend ??
    t.opening_trend ??
    null;

  return {
    PDC: pdc,
    OL: ol,
    OT: ot,
  };
}

function mergeState(rawState, planState) {
  // rawState:
  //   { mode, sim, sim_day, sim_clock, symbols: { SBIN: { ltp, ohlc, volume, ... }, ... } }
  // planState:
  //   { date, mode, daily_risk_rs, risk_per_trade_rs, total_planned_risk_rs, active_trades, plans: [...] }

  const symbolsRaw = (rawState && rawState.symbols) || {};
  const plansList = (planState && planState.plans) || [];

  const merged = {};

  // Base from quotes
  for (const [sym, q] of Object.entries(symbolsRaw)) {
    merged[sym] = {
      symbol: sym,
      ltp: q.ltp ?? null,
      ohlc: q.ohlc || null,
      volume: q.volume ?? null,
      tags: {}, // we’ll normalize into PDC/OL/OT
      pick: null,
      confidence: null,
      qty: null,
      entry: null,
      stop: null,
      tp1: null,
      tp2: null,
      status: null,
    };
  }

  // Merge plan info
  for (const p of plansList) {
    const sym =
      p.symbol ||
      p.sym ||
      p.ticker ||
      (typeof p.name === "string" ? p.name.toUpperCase() : null);
    if (!sym) continue;

    if (!merged[sym]) {
      merged[sym] = {
        symbol: sym,
        ltp: null,
        ohlc: null,
        volume: null,
        tags: {},
        pick: null,
        confidence: null,
        qty: null,
        entry: null,
        stop: null,
        tp1: null,
        tp2: null,
        status: null,
      };
    }

    const row = merged[sym];

    // Core plan fields
    row.pick = p.pick ?? row.pick;

    // IMPORTANT: backend uses "confidence%"
    row.confidence =
      p.confidence ??
      p.conf ??
      p["confidence%"] ??
      row.confidence;

    row.qty = p.qty ?? p.quantity ?? row.qty;
    row.entry = p.entry ?? p.entry_price ?? row.entry;
    row.stop = p.stop ?? p.stop_loss ?? row.stop;
    row.tp1 = p.tp1 ?? p.target1 ?? p.tp_primary ?? row.tp1;
    row.tp2 = p.tp2 ?? p.target2 ?? p.tp_secondary ?? row.tp2;
    row.status = p.status ?? row.status;

    // Tags: normalize whatever backend sent into PDC / OL / OT
    const normTags = normalizeTagsFromPlan(p);

    // Merge on top of any existing tags from rawState (if we ever push tags via live_state.json later)
    row.tags = Object.assign({}, row.tags || {}, normTags);
  }

  const meta = {
    mode: planState?.mode || rawState?.mode || "unknown",
    // playback/live day must come from sim_day
    date: rawState?.sim_day || planState?.date || null,
    clock: rawState?.sim_clock || null,
    sim: rawState?.sim ?? null,
    daily_risk_rs: planState?.daily_risk_rs ?? null,
    risk_per_trade_rs: planState?.risk_per_trade_rs ?? null,
    total_planned_risk_rs: planState?.total_planned_risk_rs ?? null,
    active_trades: planState?.active_trades ?? null,
  };

  return { meta, symbols: merged };
}

// --- Formatting helpers ---

function fmtRs(x) {
  if (x == null) return "—";
  const v = Number(x);
  if (!Number.isFinite(v)) return "—";
  if (Math.abs(v) >= 100000) {
    return "₹" + (v / 100000).toFixed(1) + "L";
  }
  return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function fmtNum(x, decimals = 2) {
  if (x == null) return "—";
  const v = Number(x);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(decimals);
}

function fmtPct(x) {
  if (x == null) return "—";
  const v = Number(x);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(1) + "%";
}

function safeText(v) {
  if (v == null || v === "") return "—";
  return String(v);
}

function classifyMode(mode) {
  const m = (mode || "").toLowerCase();
  if (m.includes("live")) return "mode-live";
  if (m.includes("paper")) return "mode-paper";
  if (m.includes("test")) return "mode-test";
  return "mode-unknown";
}

// --- Summary rendering ---

function renderSummary(meta) {
  const modeEl = document.getElementById("summary-mode");
  const dateEl = document.getElementById("summary-date");
  const clockEl = document.getElementById("summary-clock");
  const dailyRiskEl = document.getElementById("summary-daily-risk");
  const plannedRiskEl = document.getElementById("summary-planned-risk");
  const rptEl = document.getElementById("summary-risk-per-trade");
  const activeEl = document.getElementById("summary-active-trades");
  const updatedEl = document.getElementById("summary-last-update");
  const footerMeta = document.getElementById("footer-meta");

  const mode = meta.mode || "unknown";
  modeEl.textContent = mode.toUpperCase();
  modeEl.className = "mode-badge " + classifyMode(mode);

  dateEl.textContent = meta.date || "—";
  clockEl.textContent = meta.clock || "—";

  dailyRiskEl.textContent = fmtRs(meta.daily_risk_rs);
  plannedRiskEl.textContent = fmtRs(meta.total_planned_risk_rs);
  rptEl.textContent = fmtRs(meta.risk_per_trade_rs);
  activeEl.textContent =
    meta.active_trades == null ? "—" : String(meta.active_trades);

  updatedEl.textContent = meta.clock || "—";
  footerMeta.textContent = meta.sim === false ? "LIVE feed" : "SIM feed";
}

// --- Grid rendering ---

function renderGrid(mergedState) {
  const tbody = document.getElementById("symbols-tbody");
  const filterInput = document.getElementById("symbol-filter");
  const filterStr = (filterInput.value || "").trim().toLowerCase();

  const rows = Object.values(mergedState.symbols || {});
  rows.sort((a, b) => a.symbol.localeCompare(b.symbol));

  const frag = document.createDocumentFragment();

  for (const row of rows) {
    const sym = row.symbol;
    if (!sym) continue;

    const tags = row.tags || {};
    const pdc = tags.PDC ?? null;
    const ol = tags.OL ?? null;
    const ot = tags.OT ?? null;

    const pick = row.pick;
    const conf = row.confidence;
    const qty = row.qty;
    const entry = row.entry;
    const stop = row.stop;
    const tp1 = row.tp1;
    const tp2 = row.tp2;
    const status = row.status;

    // Compute change if we have OHLC
    let changePct = null;
    if (row.ohlc && row.ohlc.o != null && row.ltp != null) {
      const o = Number(row.ohlc.o);
      if (o !== 0) {
        changePct = ((Number(row.ltp) - o) / o) * 100;
      }
    }

    const textBlob = [
      sym,
      pdc,
      ol,
      ot,
      pick,
      conf,
      status,
    ]
      .join(" ")
      .toLowerCase();

    if (filterStr && !textBlob.includes(filterStr)) {
      continue;
    }

    const tr = document.createElement("tr");
    if (selectedSymbol && selectedSymbol === sym) {
      tr.classList.add("row-selected");
    }

    // Symbol
    const tdSym = document.createElement("td");
    tdSym.textContent = sym;
    tdSym.className = "cell-symbol";

    // LTP
    const tdLtp = document.createElement("td");
    tdLtp.textContent = fmtNum(row.ltp, 2);
    tdLtp.className = "cell-number";

    // Change
    const tdChg = document.createElement("td");
    tdChg.textContent = changePct == null ? "—" : fmtPct(changePct);
    tdChg.className =
      "cell-number " +
      (changePct > 0 ? "cell-pos" : changePct < 0 ? "cell-neg" : "");

    // Tags
    const tdPdc = document.createElement("td");
    tdPdc.textContent = safeText(pdc);

    const tdOl = document.createElement("td");
    tdOl.textContent = safeText(ol);

    const tdOt = document.createElement("td");
    tdOt.textContent = safeText(ot);

    // Pick
    const tdPick = document.createElement("td");
    tdPick.textContent = safeText(pick);
    tdPick.className =
      "cell-pick " +
      (pick === "BULL"
        ? "pick-long"
        : pick === "BEAR"
        ? "pick-short"
        : "pick-none");

    // Conf
    const tdConf = document.createElement("td");
    tdConf.textContent = conf == null ? "—" : fmtPct(conf);
    tdConf.className = "cell-number";

    // Qty
    const tdQty = document.createElement("td");
    tdQty.textContent = qty == null ? "—" : String(qty);
    tdQty.className = "cell-number";

    // Entry / SL / T1 / T2
    const tdEntry = document.createElement("td");
    tdEntry.textContent = fmtNum(entry, 2);
    tdEntry.className = "cell-number";

    const tdSl = document.createElement("td");
    tdSl.textContent = fmtNum(stop, 2);
    tdSl.className = "cell-number";

    const tdT1 = document.createElement("td");
    tdT1.textContent = fmtNum(tp1, 2);
    tdT1.className = "cell-number";

    const tdT2 = document.createElement("td");
    tdT2.textContent = fmtNum(tp2, 2);
    tdT2.className = "cell-number";

    // Status
    const tdStatus = document.createElement("td");
    tdStatus.textContent = safeText(status);
    tdStatus.className = "cell-status";

    tr.appendChild(tdSym);
    tr.appendChild(tdLtp);
    tr.appendChild(tdChg);
    tr.appendChild(tdPdc);
    tr.appendChild(tdOl);
    tr.appendChild(tdOt);
    tr.appendChild(tdPick);
    tr.appendChild(tdConf);
    tr.appendChild(tdQty);
    tr.appendChild(tdEntry);
    tr.appendChild(tdSl);
    tr.appendChild(tdT1);
    tr.appendChild(tdT2);
    tr.appendChild(tdStatus);

    tr.addEventListener("click", () => {
      selectedSymbol = sym;
      renderDetail(row, mergedState.meta);
    });

    frag.appendChild(tr);
  }

  tbody.innerHTML = "";
  tbody.appendChild(frag);

  // If nothing selected, pick first symbol
  if (!selectedSymbol && rows.length > 0) {
    selectedSymbol = rows[0].symbol;
    const row = mergedState.symbols[selectedSymbol];
    if (row) {
      renderDetail(row, mergedState.meta);
    }
  }
}

// --- Detail panel ---

function renderDetail(row, meta) {
  const symEl = document.getElementById("detail-symbol");
  const modeEl = document.getElementById("detail-mode");
  const tagsEl = document.getElementById("detail-tags");
  const planEl = document.getElementById("detail-plan");
  const riskEl = document.getElementById("detail-risk");
  const clockEl = document.getElementById("detail-clock");
  const titleEl = document.getElementById("detail-title");

  const tags = row.tags || {};
  const parts = [];
  if (tags.PDC) parts.push(`PDC: ${tags.PDC}`);
  if (tags.OL) parts.push(`OL: ${tags.OL}`);
  if (tags.OT) parts.push(`OT: ${tags.OT}`);

  symEl.textContent = row.symbol || "—";
  titleEl.textContent = row.symbol ? `Symbol: ${row.symbol}` : "Symbol details";

  modeEl.textContent = meta.mode || "—";
  tagsEl.textContent = parts.length ? parts.join(" • ") : "—";

  const pick = row.pick ? row.pick : "—";
  const qty = row.qty != null ? row.qty : "—";
  const entry = fmtNum(row.entry, 2);
  const sl = fmtNum(row.stop, 2);
  const t1 = fmtNum(row.tp1, 2);
  const t2 = fmtNum(row.tp2, 2);
  planEl.textContent = `${pick} @ ${entry} / SL ${sl} / T1 ${t1} / T2 ${t2} / Qty ${qty}`;

  riskEl.textContent =
    meta.risk_per_trade_rs != null
      ? `~${fmtRs(meta.risk_per_trade_rs)} per trade`
      : "—";

  clockEl.textContent = meta.clock || "—";
}

// --- Health rendering ---

function renderHealth(health) {
  const dot = document.getElementById("health-dot");
  const txt = document.getElementById("health-text");

  if (!health) {
    dot.className = "health-dot health-unknown";
    txt.textContent = "No health data";
    return;
  }

  const status = (health.system_status || health.status || "unknown").toLowerCase();

  if (status === "ok" || status === "healthy") {
    dot.className = "health-dot health-ok";
  } else if (status === "warn" || status === "warning") {
    dot.className = "health-dot health-warning";
  } else {
    dot.className = "health-dot health-error";
  }

  const reason = health.reason || "";
  txt.textContent = reason ? `${status.toUpperCase()} – ${reason}` : status.toUpperCase();
}

// --- Poll loops ---

async function pollStateLoop() {
  while (true) {
    try {
      // 1) Read raw first (gives sim_day)
      const raw = await getJSON("/api/state_raw");

      // 2) Ask plan for that day
      let planUrl = "/api/state";
      if (raw && raw.sim_day) {
        planUrl = `/api/state?day=${encodeURIComponent(raw.sim_day)}`;
      }

      const plan = await getJSON(planUrl);

      const merged = mergeState(raw, plan);
      lastMerged = merged;

      renderSummary(merged.meta);
      renderGrid(merged);
    } catch (err) {
      console.error("state poll failed:", err);
      const meta = {
        mode: "disconnected",
        date: null,
        clock: null,
        daily_risk_rs: null,
        risk_per_trade_rs: null,
        total_planned_risk_rs: null,
        active_trades: null,
        sim: null,
      };
      renderSummary(meta);
    }

    await new Promise((r) => setTimeout(r, STATE_POLL_MS));
  }
}

async function pollHealthLoop() {
  while (true) {
    try {
      const health = await getJSON("/api/health");
      renderHealth(health);
    } catch (err) {
      console.error("health poll failed:", err);
      renderHealth(null);
    }
    await new Promise((r) => setTimeout(r, HEALTH_POLL_MS));
  }
}

// --- Init controls ---

function initFilter() {
  const filterInput = document.getElementById("symbol-filter");
  filterInput.addEventListener("input", () => {
    if (lastMerged) {
      renderGrid(lastMerged);
    }
  });
}

function initRiskControl() {
  const input = document.getElementById("risk-input");
  const btn = document.getElementById("risk-apply-btn");

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    alert("Risk change via UI will be wired in a later phase.");
  });
}

function init() {
  initFilter();
  initRiskControl();
  pollStateLoop();
  pollHealthLoop();
}

document.addEventListener("DOMContentLoaded", init);
