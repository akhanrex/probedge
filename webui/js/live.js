// webui/js/live.js
// Live terminal frontend for Probedge, with timeline-aware display.
//
// Backend facts:
// - Plan is computed for full day at once (has tags + pick + qty).
// - Playback/live write sim_day + sim_clock into /api/state_raw.
// Our UI responsibility:
// - Always align plan with sim_day.
// - Respect cutovers visually:
//     <09:25:   no tags, no plan
//     09:25+:   show PDC
//     09:30+:   show PDC + OL
//     09:40+:   show PDC + OL + OT + full plan

const STATE_POLL_MS = 1000;
const HEALTH_POLL_MS = 5000;

let lastMerged = null;
let selectedSymbol = null;

// ---------- HTTP helpers ----------

async function getJSON(url) {
  const resp = await fetch(url, { cache: "no-store" });
  if (!resp.ok) {
    throw new Error(`GET ${url} -> ${resp.status}`);
  }
  return resp.json();
}

// ---------- Tag normalization from backend plan ----------

function normalizeTagsFromPlan(plan) {
  // Backend may send tags inside plan.tags *and/or* as top-level keys.
  const t = plan.tags || {};

  let pdc =
    t.PDC ??
    t.pdc ??
    t.PrevDayContext ??
    t.prev_day_context ??
    plan.PDC ??
    plan.pdc ??
    plan.PrevDayContext ??
    plan.prev_day_context ??
    null;

  let ol =
    t.OL ??
    t.ol ??
    t.OpenLocation ??
    t.open_location ??
    plan.OL ??
    plan.ol ??
    plan.OpenLocation ??
    plan.open_location ??
    null;

  let ot =
    t.OT ??
    t.ot ??
    t.OpeningTrend ??
    t.opening_trend ??
    plan.OT ??
    plan.ot ??
    plan.OpeningTrend ??
    plan.opening_trend ??
    null;

  // Fallback: if we STILL didn't get clear PDC/OL/OT but tags object has values,
  // use their insertion order:
  // Expected order from engine: { PrevDayContext, OpenLocation, OpeningTrend }
  const vals = Object.values(t);
  if (vals.length >= 3) {
    if (!pdc) pdc = vals[0];
    if (!ol) ol = vals[1];
    if (!ot) ot = vals[2];
  }

  return { PDC: pdc, OL: ol, OT: ot };
}


// ---------- Merge /api/state_raw + /api/state ----------

function mergeState(rawState, planState) {
  const symbolsRaw = (rawState && rawState.symbols) || {};

  // NEW: support nested portfolio_plan as the canonical place for plan info
  const portfolioPlan =
    (planState && planState.portfolio_plan) || planState || {};

  const plansList = Array.isArray(portfolioPlan.plans)
    ? portfolioPlan.plans
    : [];

  const merged = {};

  // New: live intraday fields (positions / pnl / risk / batch_agent)
  const positionsMap =
    (planState && planState.positions) ||
    (rawState && rawState.positions) ||
    {};

  const pnlObj =
    (planState && planState.pnl) ||
    (rawState && rawState.pnl) ||
    null;

  const riskObj =
    (planState && planState.risk) ||
    (rawState && rawState.risk) ||
    null;

  const batchAgentObj =
    (planState && planState.batch_agent) ||
    (rawState && rawState.batch_agent) ||
    null;


  // Base from quotes
  for (const [sym, q] of Object.entries(symbolsRaw)) {
    merged[sym] = {
      symbol: sym,
      ltp: q.ltp ?? null,
      ohlc: q.ohlc || null,
      volume: q.volume ?? null,
      tags: {}, // we'll normalize to PDC/OL/OT
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

  // Merge in plan data
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

    // Backend uses "confidence%" for confidence
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

    // Tags normalized to PDC / OL / OT
    const normTags = normalizeTagsFromPlan(p);
    row.tags = Object.assign({}, row.tags || {}, normTags);
  }

    // Merge in live positions (PENDING / OPEN / CLOSED)
    for (const [sym, pos] of Object.entries(positionsMap)) {
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
    row.position = pos;
    row.status = pos.status ?? row.status;

    // live P&L components from backend
    const openPnl =
      pos.open_pnl_rs != null ? Number(pos.open_pnl_rs) : 0;
    const realizedPnl =
      pos.realized_pnl_rs != null ? Number(pos.realized_pnl_rs) : 0;

    row.open_pnl_rs = openPnl;
    row.realized_pnl_rs = realizedPnl;
    row.exit_reason = pos.exit_reason ?? row.exit_reason;

    // Aggregated "today P&L" for the grid
    row.pnl_today = openPnl + realizedPnl;
  }


  const isSim = rawState?.sim === true;

  const meta = {
    mode: planState?.mode || rawState?.mode || "unknown",

    // For SIM: use sim_day. For real/live: trust planState.date.
    date: isSim
      ? (rawState?.sim_day || portfolioPlan.date || planState?.date || null)
      : (planState?.date || portfolioPlan.date || rawState?.sim_day || null),

    // For SIM: clock from sim_clock. For real/live: prefer planState.clock if present.
    clock: isSim
      ? (rawState?.sim_clock || null)
      : (planState?.clock || rawState?.sim_clock || null),

    sim: rawState?.sim ?? null,
  

    // Plan aggregates from portfolioPlan, with fallback to flattened/raw if ever needed
    daily_risk_rs:
      portfolioPlan.daily_risk_rs ??
      planState?.daily_risk_rs ??
      rawState?.daily_risk_rs ??
      null,
    risk_per_trade_rs: portfolioPlan.risk_per_trade_rs ?? null,
    total_planned_risk_rs: portfolioPlan.total_planned_risk_rs ?? null,
    active_trades: portfolioPlan.active_trades ?? null,

    // Live intraday metrics
    pnl: pnlObj,
    risk_state: riskObj,
    batch_agent: batchAgentObj,
  };

  return { meta, symbols: merged };
}

// ---------- Formatting helpers ----------

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

// ---------- Timeline gating logic ----------

function extractTime(meta) {
  // meta.clock is ISO-like: "2025-08-01T09:20:00"
  if (!meta || !meta.clock) return null;
  const s = String(meta.clock);
  const idx = s.indexOf("T");
  if (idx === -1) return null;
  return s.slice(idx + 1, idx + 9); // "HH:MM:SS"
}

function gateTagsAndPlan(row, meta) {
  const time = extractTime(meta);

  const baseTags = row.tags || {};
  const rawPdc = baseTags.PDC ?? null;
  const rawOl = baseTags.OL ?? null;
  const rawOt = baseTags.OT ?? null;

  let pdc = null;
  let ol = null;
  let ot = null;

  // Defaults: hide entire plan
  let pick = null;
  let conf = null;
  let qty = null;
  let entry = null;
  let stop = null;
  let tp1 = null;
  let tp2 = null;
  let status = null;

  // If we don't know the time, hide everything
  if (!time) {
    return {
      pdc,
      ol,
      ot,
      pick,
      conf,
      qty,
      entry,
      stop,
      tp1,
      tp2,
      status,
    };
  }

  // PHASE 1: < 09:25:00 => no tags, no plan
  if (time < "09:25:00") {
    return {
      pdc,
      ol,
      ot,
      pick,
      conf,
      qty,
      entry,
      stop,
      tp1,
      tp2,
      status,
    };
  }

  // PHASE 2: 09:25:00–09:29:59 => PDC only, no plan
  if (time < "09:30:00") {
    pdc = rawPdc;
    return {
      pdc,
      ol,
      ot,
      pick,
      conf,
      qty,
      entry,
      stop,
      tp1,
      tp2,
      status,
    };
  }

  // PHASE 3: 09:30:00–09:39:59 => PDC + OL, no OT, no plan
  if (time < "09:40:00") {
    pdc = rawPdc;
    ol = rawOl;
    return {
      pdc,
      ol,
      ot,
      pick,
      conf,
      qty,
      entry,
      stop,
      tp1,
      tp2,
      status,
    };
  }

  // PHASE 4: >= 09:40:00 => full tags + plan + live status visible
  pdc = rawPdc;
  ol = rawOl;
  ot = rawOt;

  pick = row.pick;
  conf = row.confidence;
  qty = row.qty;
  entry = row.entry;
  stop = row.stop;
  tp1 = row.tp1;
  tp2 = row.tp2;

  // Base status from plan (if any)
  let statusText = row.status || null;

  // If we have a live position, override status from that
  const pos = row.position || null;
  if (pos) {
    const st = pos.status;
    if (st === "PENDING") {
      statusText = "PENDING";
    } else if (st === "OPEN") {
      const op = Number(pos.open_pnl_rs ?? 0);
      if (Number.isFinite(op) && op !== 0) {
        const sign = op > 0 ? "+" : op < 0 ? "-" : "";
        statusText = `OPEN (${sign}${Math.round(op)})`;
      } else {
        statusText = "OPEN";
      }
    } else if (st === "CLOSED") {
      const reason = pos.exit_reason || "";
      statusText = `CLOSED${reason ? " (" + reason + ")" : ""}`;
    }
  }

  status = statusText;

  return {
    pdc,
    ol,
    ot,
    pick,
    conf,
    qty,
    entry,
    stop,
    tp1,
    tp2,
    status,
  };
}

// ---------- Summary rendering ----------

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

  // New optional elements (add these IDs in live.html if not present)
  const pnlDayEl = document.getElementById("summary-pnl-day");
  const pnlOpenEl = document.getElementById("summary-pnl-open");
  const pnlRealEl = document.getElementById("summary-pnl-realized");
  const riskStatusEl = document.getElementById("summary-risk-status");
  const batchAgentEl = document.getElementById("summary-batch-agent");


  const mode = meta.mode || "unknown";
  modeEl.textContent = mode.toUpperCase();
  modeEl.className = "mode-badge " + classifyMode(mode);

  dateEl.textContent = meta.date || "—";
  clockEl.textContent = meta.clock || "—";

  // Always show the budget (this is what YOU set)
  dailyRiskEl.textContent = fmtRs(meta.daily_risk_rs);

  // Timeline gating for plan info in summary as well
  const time = extractTime(meta);

  if (!time || time < "09:40:00") {
    // Before 09:40, hide plan-related aggregates
    plannedRiskEl.textContent = "—";
    rptEl.textContent = "—";
    activeEl.textContent = "—";
  } else {
    // From 09:40 onwards, show full plan numbers
    plannedRiskEl.textContent = fmtRs(meta.total_planned_risk_rs);
    rptEl.textContent = fmtRs(meta.risk_per_trade_rs);
    activeEl.textContent =
      meta.active_trades == null ? "—" : String(meta.active_trades);
  }

  updatedEl.textContent = meta.clock || "—";
  footerMeta.textContent = meta.sim === false ? "LIVE feed" : "SIM feed";
    // --- New: live P&L + risk + batch_agent status (if HTML provides spans) ---
  const pnl = meta.pnl || null;
  if (pnl) {
    if (pnlDayEl) pnlDayEl.textContent = fmtRs(pnl.day_total_rs);
    if (pnlOpenEl) pnlOpenEl.textContent = fmtRs(pnl.open_rs);
    if (pnlRealEl) pnlRealEl.textContent = fmtRs(pnl.realized_rs);
  } else {
    if (pnlDayEl) pnlDayEl.textContent = "—";
    if (pnlOpenEl) pnlOpenEl.textContent = "—";
    if (pnlRealEl) pnlRealEl.textContent = "—";
  }

  const risk = meta.risk_state || null;
  if (riskStatusEl) {
    if (!risk) {
      riskStatusEl.textContent = "—";
    } else {
      const rs = (risk.status || "UNKNOWN").toUpperCase();
      riskStatusEl.textContent = rs;
    }
  }

  const ba = meta.batch_agent || null;
  if (batchAgentEl) {
    if (!ba || !ba.last_heartbeat_ts) {
      batchAgentEl.textContent = "WARN – batch_agent has never reported";
    } else {
      const s = (ba.status || "UNKNOWN").toUpperCase();
      const ts = ba.last_heartbeat_ts;
      batchAgentEl.textContent = `${s} – batch_agent ${ts}`;
    }
  }
}


// ---------- Grid rendering ----------

function renderGrid(mergedState) {
  const meta = mergedState.meta || {};
  const tbody = document.getElementById("symbols-tbody");
  const filterInput = document.getElementById("symbol-filter");
  const filterStr = (filterInput.value || "").trim().toLowerCase();

  const rows = Object.values(mergedState.symbols || {});
  rows.sort((a, b) => a.symbol.localeCompare(b.symbol));

  const frag = document.createDocumentFragment();

  for (const row of rows) {
    const sym = row.symbol;
    if (!sym) continue;

    // Apply timeline gating
    const gated = gateTagsAndPlan(row, meta);

    const pdc = gated.pdc;
    const ol = gated.ol;
    const ot = gated.ot;

    const pick = gated.pick;
    const conf = gated.conf;
    const qty = gated.qty;
    const entry = gated.entry;
    const stop = gated.stop;
    const tp1 = gated.tp1;
    const tp2 = gated.tp2;
    const status = gated.status;

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
  
    // P&L (today)
    const tdPnl = document.createElement("td");
    const pnlToday = row.pnl_today != null ? Number(row.pnl_today) : 0;
    tdPnl.textContent = pnlToday ? fmtRs(pnlToday) : "";
    tdPnl.className =
      "cell-number " +
      (pnlToday > 0 ? "cell-pos" : pnlToday < 0 ? "cell-neg" : "");
  
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
    tr.appendChild(tdPnl);    // NEW
    tr.appendChild(tdStatus);


    tr.addEventListener("click", () => {
      selectedSymbol = sym;
      renderDetail(row, meta); // detail also uses gating
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

// ---------- Detail panel ----------

function renderDetail(row, meta) {
  const symEl = document.getElementById("detail-symbol");
  const modeEl = document.getElementById("detail-mode");
  const tagsEl = document.getElementById("detail-tags");
  const planEl = document.getElementById("detail-plan");
  const riskEl = document.getElementById("detail-risk");
  const clockEl = document.getElementById("detail-clock");
  const titleEl = document.getElementById("detail-title");

  // Apply the same gating for consistency
  const gated = gateTagsAndPlan(row, meta);

  const parts = [];
  if (gated.pdc) parts.push(`PDC: ${gated.pdc}`);
  if (gated.ol) parts.push(`OL: ${gated.ol}`);
  if (gated.ot) parts.push(`OT: ${gated.ot}`);

  symEl.textContent = row.symbol || "—";
  titleEl.textContent = row.symbol ? `Symbol: ${row.symbol}` : "Symbol details";

  modeEl.textContent = meta.mode || "—";
  tagsEl.textContent = parts.length ? parts.join(" • ") : "—";

  const pick = gated.pick ? gated.pick : "—";
  const qty = gated.qty != null ? gated.qty : "—";
  const entry = fmtNum(gated.entry, 2);
  const sl = fmtNum(gated.stop, 2);
  const t1 = fmtNum(gated.tp1, 2);
  const t2 = fmtNum(gated.tp2, 2);
  planEl.textContent = `${pick} @ ${entry} / SL ${sl} / T1 ${t1} / T2 ${t2} / Qty ${qty}`;

  riskEl.textContent =
    meta.risk_per_trade_rs != null
      ? `~${fmtRs(meta.risk_per_trade_rs)} per trade`
      : "—";

  clockEl.textContent = meta.clock || "—";
}

// ---------- Health rendering ----------

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

// ---------- Poll loops ----------

async function pollStateLoop() {
  while (true) {
    try {
      // 1) raw -> gives sim_day and clock
      const raw = await getJSON("/api/state_raw");
      const isSim = raw && raw.sim === true;

      // 2) plan for that day
      // LIVE (sim === false): always ask backend "today" via bare /api/state
      // SIM  (sim === true): use sim_day
      let planUrl = "/api/state";
      if (isSim && raw && raw.sim_day) {
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

// ---------- Controls ----------

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

  if (!input || !btn) return;

  // Enable the button now that backend exists
  btn.disabled = false;
  btn.title = "Apply daily risk to this stack";

  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    const val = Number(input.value || 0);
    if (!val || val < 1000) {
      alert("Enter a valid daily risk (>= 1000).");
      return;
    }

    try {
      const resp = await fetch("/api/risk", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ daily_risk_rs: val }),
      });
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(`POST /api/risk → ${resp.status}: ${txt}`);
      }
      const data = await resp.json();
      console.log("Risk updated:", data);

      // Let the polling loop pick up the new daily_risk_rs via /api/state
      alert(`Daily risk set to ₹${val.toLocaleString("en-IN")}`);
    } catch (err) {
      console.error("Risk update failed:", err);
      alert("Failed to update risk. Check logs / console.");
    }
  });
}


// ---------- Init ----------

function init() {
  initFilter();
  initRiskControl();
  pollStateLoop();
  pollHealthLoop();
}

document.addEventListener("DOMContentLoaded", init);
