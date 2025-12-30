// webui/js/live.js
// Live terminal frontend for Probedge.
//
// Responsibilities:
// - Poll /api/state_raw and /api/state.
// - Merge quotes + plan + positions into a single model.
// - Respect timeline gating for PDC/OL/OT + plan visibility.
// - Render summary bar + universe grid.
// - Drive the shared header pills (mode, clock, health, Kite).
//
// Cutovers:
//   <09:25:   no tags, no plan
//   09:25+:   show PDC
//   09:30+:   show PDC + OL
//   09:40+:   show PDC + OL + OT + full plan

// Refresh intervals
const STATE_POLL_MS = 1000;   // 1s – good balance for LIVE
const HEALTH_POLL_MS = 5000;  // health can be slow
const AUTH_POLL_MS = 60000;   // 60s = 1 minute


let lastMerged = null;

// Universe order (single source of truth = /api/config → config/frequency.yaml)
let universeOrder = null;

async function loadUniverseOrder() {
  try {
    const cfg = await getJSON("/api/config");
    const syms = (cfg && Array.isArray(cfg.symbols) ? cfg.symbols : [])
      .map(s => String(s).toUpperCase().trim())
      .filter(Boolean);
    universeOrder = syms.length ? syms : null;
  } catch (err) {
    // Non-fatal: we will fall back to alpha sort
    console.warn("[live] Failed to load /api/config for universe order:", err);
    universeOrder = null;
  }
}

// ---------- HTTP helpers ----------

async function getJSON(url) {
  const resp = await fetch(url, { cache: "no-store" });
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`GET ${url} -> ${resp.status} ${text || ""}`);
  }
  return resp.json();
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent =
    value === null || value === undefined || value === "" ? "—" : String(value);
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

// ---------- Tag normalization from backend plan ----------

function normalizeTagsFromPlan(plan) {
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
  const symbolsRaw = (rawState && rawState.quotes && typeof rawState.quotes === "object") ? rawState.quotes : {};
  const tagsRaw = (rawState && rawState.tags) || {};
  const portfolioPlan =
    (planState && planState.portfolio_plan) || planState || {};

  const plansList = Array.isArray(portfolioPlan.plans)
    ? portfolioPlan.plans
    : Array.isArray(planState && planState.plans)
    ? planState.plans
    : [];

  const merged = {};

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
      tags: normalizeTagsFromPlan({ tags: tagsRaw[sym] || {} }),
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

  // SIMFIX_TAGS_FROM_RAW_V1: ensure rawState.tags attaches to rows (PDC/OL/OT pre-snapshot)
  try {
    const tmap = tagsRaw || {};
    for (const [sym, row] of Object.entries(merged)) {
      if (!row) continue;

      const up = String(sym).toUpperCase();
      const alt =
        up === "TATAMOTORS" ? "TMPV" :
        up === "TMPV" ? "TATAMOTORS" :
        null;

      const t =
        tmap[sym] ||
        tmap[up] ||
        (alt ? (tmap[alt] || tmap[alt.toUpperCase()]) : null) ||
        null;

      if (t) {
        const nt = normalizeTagsFromPlan({ tags: t });
        row.tags = Object.assign({}, row.tags || {}, nt);
      }
    }
  } catch (e) {}


  // Merge plan data
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

    row.pick = p.pick ?? row.pick;
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

    const normTags = normalizeTagsFromPlan(p);
    row.tags = Object.assign({}, row.tags || {}, normTags);
  }

  // Merge positions / live P&L
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

    const openPnl =
      pos.open_pnl_rs != null ? Number(pos.open_pnl_rs) : 0;
    const realizedPnl =
      pos.realized_pnl_rs != null ? Number(pos.realized_pnl_rs) : 0;

    row.open_pnl_rs = openPnl;
    row.realized_pnl_rs = realizedPnl;
    row.exit_reason = pos.exit_reason ?? row.exit_reason;

    row.pnl_today = openPnl + realizedPnl;
  }

  const isSim = rawState && rawState.sim === true;

  let pnl_day = null;
  let pnl_open = null;
  let pnl_realized = null;
  if (pnlObj) {
    pnl_day =
      pnlObj.day ??
      pnlObj.pnl_day ??
      null;
    pnl_open =
      pnlObj.open ??
      pnlObj.pnl_open ??
      null;
    pnl_realized =
      pnlObj.realized ??
      pnlObj.pnl_realized ??
      null;
  }

  const meta = {
    mode: (planState && planState.mode) || (rawState && rawState.mode) || "unknown",

    date: isSim
      ? (rawState && rawState.sim_day) ||
        portfolioPlan.date ||
        (planState && planState.date) ||
        null
      : (planState && planState.date) ||
        portfolioPlan.date ||
        (rawState && rawState.sim_day) ||
        null,

    clock: isSim
      ? (rawState && rawState.sim_clock) || null
      : (planState && planState.clock) ||
        (rawState && rawState.sim_clock) ||
        null,

    plan_status:
      (planState && planState.plan_status) ||
      (planState && planState.status) ||
      null,

    plan_built_at:
      (planState && planState.plan_built_at) ||
      null,

    plan_locked:
      portfolioPlan.plan_locked ??
      (planState && planState.plan_locked) ??
      null,

    sim: rawState && rawState.sim === true,

    daily_risk_rs:
      portfolioPlan.daily_risk_rs ??
      (planState && planState.daily_risk_rs) ??
      (rawState && rawState.daily_risk_rs) ??
      null,

    risk_per_trade_rs:
      portfolioPlan.risk_per_trade_rs ??
      (planState && planState.risk_per_trade_rs) ??
      null,

    total_planned_risk_rs:
      portfolioPlan.total_planned_risk_rs ??
      (planState && planState.total_planned_risk_rs) ??
      null,

    active_trades:
      portfolioPlan.active_trades ??
      (planState && planState.active_trades) ??
      null,

    pnl_day,
    pnl_open,
    pnl_realized,

    pnl: pnlObj,
    risk_state: riskObj,
    batch_agent: batchAgentObj,
  };

  
  // ---- SIM clock/date wiring (critical) ----
  // In SIM, server writes clock_ist like "YYYY-MM-DD HH:MM:SS" (space, not 'T')
  // Ensure meta.clock/meta.date are derived from server clock so header updates continuously.
  if (!meta.clock && rawState) {
    meta.clock = rawState.clock_ist || rawState.sim_now_ist || null;
  }
  if (!meta.date && meta.clock) {
    const parts = String(meta.clock).split(/[T ]/);
    if (parts && parts.length) meta.date = parts[0] || meta.date;
  }
// Gate summary *plan* fields until plan snapshot is READY+locked
  const ps = (meta.plan_status || '').toString().toUpperCase();
  const planReady = (ps === 'READY' || ps === 'READY_PARTIAL') && meta.plan_locked === true;
  if (!planReady) {
    meta.total_planned_risk_rs = null;  // Planned Risk → —
    meta.risk_per_trade_rs = null;      // Risk / Trade → —
    meta.active_trades = null;          // Active Trades → —
  }
    // SIMFIX_QUOTE_TO_ROW_V4: always map raw quotes into rows (LTP/Δ% pre and post snapshot)
  try {
    const q = (rawState && rawState.quotes) ? rawState.quotes : {};
    if (q && typeof q === "object") {
      const qSyms = Object.keys(q);

      // If merged has no rows, seed from quotes.
      if (Array.isArray(merged)) {
        if (merged.length === 0) {
          for (const sym of qSyms) merged.push({ symbol: sym });
        }
      } else if (merged && typeof merged === "object") {
        const keys = Object.keys(merged);
        if (!keys || keys.length === 0) {
          for (const sym of qSyms) merged[sym] = { symbol: sym };
        }
      }

      const rows = Array.isArray(merged) ? merged : Object.values(merged || {});
      for (const row of rows) {
        if (!row || !row.symbol) continue;
        const sym = String(row.symbol).toUpperCase();
        const qq = q[sym] || q[row.symbol] || null;
        if (!qq) continue;

        const ltp = Number(qq.ltp ?? qq.last_price ?? qq.lp ?? qq.price);
        if (Number.isFinite(ltp)) row.ltp = ltp;

        // renderGrid computes Δ% from row.ohlc.o; infer it if we only have pct.
        const pct = Number(qq.change_pct ?? qq.chg_pct ?? qq.pct);
        if (Number.isFinite(pct) && Number.isFinite(ltp) && pct !== -100) {
          const o = ltp / (1 + (pct / 100));
          row.ohlc = row.ohlc || {};
          if (row.ohlc.o == null) row.ohlc.o = o;
        }
      }
    }
  } catch (e) {
    // keep UI alive even if quote mapping fails
  }

return { meta, symbols: merged };
}


// ---------- Timeline gating ----------

function extractTime(meta) {
  if (!meta) return null;

  // LIVE mode → smooth wall clock
  if (meta.sim === false) {
    const now = new Date();
    return now.toTimeString().slice(0, 8);
  }

  const raw = meta.clock || meta.clock_ist || meta.sim_now_ist || null;
  if (!raw) return null;

  const m = String(raw).match(/(\d{2}:\d{2}:\d{2})/);
  return m ? m[1] : null;
}


function gateTagsAndPlan(row, meta) {
  const baseTags = row.tags || {};
  const rawPdc = baseTags.PDC ?? null;
  const rawOl  = baseTags.OL  ?? null;
  const rawOt  = baseTags.OT  ?? null;

  // time gating (SIM and LIVE)
  const t = extractTime(meta); // "HH:MM:SS" or null

  function ge(a, b) {
    if (!a) return false;
    return String(a) >= String(b);
  }

  // Tags reveal on timeline (even before plan snapshot)
  let pdc = ge(t, "09:25:00") ? rawPdc : null;
  let ol  = ge(t, "09:30:00") ? rawOl  : null;
  let ot  = ge(t, "09:40:01") ? rawOt  : null;  // OT moved to 09:40 (not 09:39:50)


  // Plan reveals only once snapshot is READY + locked
  const ps = (meta && meta.plan_status ? String(meta.plan_status) : "").toUpperCase();
  const planReady = (ps === "READY" || ps === "READY_PARTIAL") && meta.plan_locked === true;

  let pick = null, conf = null, qty = null, entry = null, stop = null, tp1 = null, tp2 = null, status = null;

  if (!planReady) {
    return { pdc, ol, ot, pick, conf, qty, entry, stop, tp1, tp2, status };
  }

  pick = row.pick;
  conf = row.confidence;
  qty  = row.qty;
  entry = row.entry;
  stop  = row.stop;
  tp1   = row.tp1;
  tp2   = row.tp2;

  // Position-aware status (same as before)
  let statusText = row.status || null;
  const pos = row.position || null;
  if (pos) {
    const st = pos.status;
    if (st === "PENDING") statusText = "PENDING";
    else if (st === "OPEN") {
      const op = Number(pos.open_pnl_rs ?? 0);
      if (Number.isFinite(op) && op !== 0) {
        const sign = op > 0 ? "+" : op < 0 ? "-" : "";
        statusText = `OPEN (${sign}${Math.round(op)})`;
      } else statusText = "OPEN";
    } else if (st === "CLOSED") {
      const reason = pos.exit_reason || "";
      statusText = `CLOSED${reason ? " (" + reason + ")" : ""}`;
    }
  }
  status = statusText;

  return { pdc, ol, ot, pick, conf, qty, entry, stop, tp1, tp2, status };
}

// ---------- Header helpers ----------

function updateHeaderFromMeta(meta = {}) {
  const pillMode = document.getElementById("pillMode");
  const pillModeText = document.getElementById("pillModeText");
  const clockEl = document.getElementById("headerClock");

  if (pillMode && pillModeText) {
    const m = (meta.mode || "").toString().toLowerCase();
    let modeLabel = "UNKNOWN";
    let cls = "app-pill-mode-paper";

    if (m.includes("live")) {
      modeLabel = "LIVE";
      cls = "app-pill-mode-live";
    } else if (m.includes("paper")) {
      modeLabel = "PAPER";
      cls = "app-pill-mode-paper";
    } else if (m.includes("sim")) {
      modeLabel = "SIM";
      cls = "app-pill-mode-sim";
    }

    pillModeText.textContent = modeLabel;
    pillMode.classList.remove("app-pill-mode-paper", "app-pill-mode-live", "app-pill-mode-sim");
    pillMode.classList.add(cls);
  }

  if (clockEl) {
    const date = meta.date || "--";
    const t = extractTime(meta) || "--:--:--";
    clockEl.textContent = `${date} · ${t}`;
  }

  // Plan snapshot status (single source of truth)
  const phaseEl = document.getElementById("livePhaseBadge");
  if (phaseEl) {
    const ps = (meta.plan_status || "MISSING").toString().toUpperCase();
    const locked = meta.plan_locked === true;
    let built = null;
    if (meta.plan_built_at) {
      const s = String(meta.plan_built_at);
      const idx = s.indexOf("T");
      built = idx >= 0 ? s.slice(idx + 1, idx + 9) : s;
    }

    const ready = (ps === "READY" || ps === "READY_PARTIAL");
    if (!ready) {
      phaseEl.textContent = `Plan: ${ps}`;
    } else if (!locked) {
      phaseEl.textContent = `Plan: ${ps} (UNLOCKED)`;
    } else {
      phaseEl.textContent = built ? `Plan: ${ps} @ ${built}` : `Plan: ${ps} (LOCKED)`;
    }
  }
}

function renderHealth(health) {
  const pill = document.getElementById("pillHealth");
  const textEl = document.getElementById("pillHealthText");
  if (!pill || !textEl) return;

  const status = (health && health.system_status) || "UNKNOWN";

  pill.classList.remove(
    "app-pill-health-ok",
    "app-pill-health-warn",
    "app-pill-health-error"
  );

  let shortStatus = "UNKNOWN";
  if (status === "OK") {
    pill.classList.add("app-pill-health-ok");
    shortStatus = "OK";
  } else if (status === "WARN") {
    pill.classList.add("app-pill-health-warn");
    shortStatus = "WARN";
  } else {
    pill.classList.add("app-pill-health-error");
    shortStatus = "DOWN";
  }

  // very short text → no overflow
  textEl.textContent = `Health: ${shortStatus}`;
}


async function refreshHeaderAuth() {
  const kiteEl = document.getElementById("pillKiteText");
  if (!kiteEl) return;

  try {
    const s = await getJSON("/api/auth/status");
    if (s && s.has_valid_token_today) {
      kiteEl.textContent = "Kite: Connected";
    } else {
      kiteEl.textContent = "Kite: Not connected";
    }
  } catch (err) {
    console.error("auth status failed:", err);
    kiteEl.textContent = "Kite: Error";
  }
}

// ---------- Summary rendering ----------

function renderSummary(meta = {}) {
  const t = extractTime(meta);

  updateHeaderFromMeta(meta);

  // Gate UI plan numbers until plan snapshot is READY+locked
  const ps = (meta.plan_status || "").toString().toUpperCase();
  const planReady = (ps === "READY" || ps === "READY_PARTIAL") && meta.plan_locked === true;

  const plannedRisk = planReady ? meta.total_planned_risk_rs : null;
  const riskPerTrade = planReady ? meta.risk_per_trade_rs : null;
  const activeTradesVal = planReady ? meta.active_trades : null;

  setText("summary-daily-risk", fmtRs(meta.daily_risk_rs));
  setText("summary-planned-risk", fmtRs(plannedRisk));
  setText("summary-risk-per-trade", fmtRs(riskPerTrade));

  const activeTrades =
    activeTradesVal != null ? String(activeTradesVal) : "—";
  setText("summary-active-trades", activeTrades);

  setText("summary-pnl-day", fmtRs(meta.pnl_day));
  setText("summary-pnl-open", fmtRs(meta.pnl_open));
  setText("summary-pnl-realized", fmtRs(meta.pnl_realized));

  const riskStatus =
    (meta.risk_state && meta.risk_state.status) || "NORMAL";
  setText("summary-risk-status", riskStatus);

  const planEl = document.getElementById("summary-plan-status");
  if (planEl) {
    const ps = (meta.plan_status || "—").toString();
    const locked = meta.plan_locked === true;
    planEl.textContent = ps === "—" ? "—" : (locked ? ps : `${ps} (UNLOCKED)`);
  }

  const agent = meta.batch_agent || {};
  const agentText = agent.status || "not reported";
  setText("summary-batch-agent", agentText);

  const lastUpdate = t || "--:--:--";
  setText("summary-last-update", lastUpdate);
}

function startUiClock() {
  setInterval(() => {
    if (!lastMerged || !lastMerged.meta) return;

    const meta = lastMerged.meta || {};
    const isSim = meta.sim === true;

    // In SIM mode, we let renderSummary / extractTime(meta)
    // drive the clock based on sim_clock; don't override.
    if (isSim) {
      return;
    }

    // LIVE mode → use wall-clock for smooth ticking
    const date = meta.date || "--";
    const nowStr = new Date().toTimeString().slice(0, 8);

    // Header clock
    const clockEl = document.getElementById("headerClock");
    if (clockEl) {
      clockEl.textContent = `${date} · ${nowStr}`;
    }

    // Summary "Last Update"
    setText("summary-last-update", nowStr);
  }, 1000);
}



// ---------- Grid rendering ----------

function renderGrid(mergedState) {
  const meta = mergedState.meta || {};
  const tbody = document.getElementById("symbols-tbody") || document.getElementById("terminalBody");
  const filterInput = document.getElementById("symbol-filter");
  const filterStr = (filterInput && filterInput.value || "").trim().toLowerCase();

  const rows = Object.values(mergedState.symbols || {});
  if (Array.isArray(universeOrder) && universeOrder.length) {
    const idx = new Map(universeOrder.map((s, i) => [s, i]));
    rows.sort((a, b) => {
      const ia = idx.has(a.symbol) ? idx.get(a.symbol) : 1e9;
      const ib = idx.has(b.symbol) ? idx.get(b.symbol) : 1e9;
      if (ia !== ib) return ia - ib;
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    });
  } else {
    rows.sort((a, b) => a.symbol.localeCompare(b.symbol));
  }

  const frag = document.createDocumentFragment();

  for (const row of rows) {
    const sym = row.symbol;
    if (!sym) continue;

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

    let changePct = null;
    if (row.ohlc && row.ohlc.o != null && row.ltp != null) {
      const o = Number(row.ohlc.o);
      if (o !== 0) {
        changePct = ((Number(row.ltp) - o) / o) * 100;
      }
    }

    if (changePct == null && row.change_pct != null) {
      const pct = Number(row.change_pct);
      if (Number.isFinite(pct)) changePct = pct;
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

    const tdSym = document.createElement("td");
    tdSym.textContent = sym;
    tdSym.className = "cell-symbol";

    const tdLtp = document.createElement("td");
    tdLtp.textContent = fmtNum(row.ltp, 2);
    tdLtp.className = "cell-number";

    const tdChg = document.createElement("td");
    tdChg.textContent = changePct == null ? "—" : fmtPct(changePct);
    tdChg.className =
      "cell-number " +
      (changePct > 0 ? "cell-pos" : changePct < 0 ? "cell-neg" : "");

    const tdPdc = document.createElement("td");
    tdPdc.textContent = safeText(pdc);

    const tdOl = document.createElement("td");
    tdOl.textContent = safeText(ol);

    const tdOt = document.createElement("td");
    tdOt.textContent = safeText(ot);

    const tdPick = document.createElement("td");
    tdPick.textContent = safeText(pick);
    tdPick.className =
      "cell-pick " +
      (pick === "BULL"
        ? "pick-long"
        : pick === "BEAR"
        ? "pick-short"
        : "pick-none");

    const tdConf = document.createElement("td");
    tdConf.textContent = conf == null ? "—" : fmtPct(conf);
    tdConf.className = "cell-number";

    const tdQty = document.createElement("td");
    tdQty.textContent = qty == null ? "—" : String(qty);
    tdQty.className = "cell-number";

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

    const tdPnl = document.createElement("td");
    const pnlToday = row.pnl_today != null ? Number(row.pnl_today) : 0;
    tdPnl.textContent = pnlToday ? fmtRs(pnlToday) : "—";
    tdPnl.className =
      "cell-number " +
      (pnlToday > 0 ? "cell-pos" : pnlToday < 0 ? "cell-neg" : "");

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
    tr.appendChild(tdPnl);
    tr.appendChild(tdStatus);

    frag.appendChild(tr);
  }

  if (tbody) {
    tbody.innerHTML = "";
    tbody.appendChild(frag);
  }
}

console.log("SIMFIX_BUILD_MARKER_V4 loaded", new Date().toISOString());

async function pollHealthLoop() {
  while (true) {
    try {
      const health = await getJSON("/api/health");
      renderHealth(health);
    } catch (err) {
      console.error("health poll failed:", err);
      renderHealth({ system_status: "DOWN", reason: "health fetch failed" });
    }
    await new Promise((r) => setTimeout(r, HEALTH_POLL_MS));
  }
}


// ---------- Poll loops ----------

async function pollStateLoop() {
  while (true) {
    try {
      const raw = await getJSON("/api/state_raw");
      const isSim = raw && raw.sim === true;

      const urlDay = new URLSearchParams(window.location.search).get("day");
      const day =
        (urlDay && String(urlDay).trim())
          ? String(urlDay).trim()
          : (isSim && raw && raw.sim_day ? String(raw.sim_day) : null);

      // Prefer the immutable snapshot for UI state.
      let snapUrl = "/api/plan_snapshot";
      if (day) snapUrl = `/api/plan_snapshot?day=${encodeURIComponent(day)}`;

      let snap = null;
      try {
        snap = await getJSON(snapUrl);
      } catch (e) {
        snap = null;
      }

      // Build "plan" object ONLY from snapshot (Phase A rule)
      let plan = null;

      const snapStatus = (snap && snap.status) ? String(snap.status).toUpperCase() : "MISSING";
      const snapHasPlan = !!(snap && snap.portfolio_plan);

      if (snapHasPlan && (snapStatus === "READY" || snapStatus === "READY_PARTIAL")) {
        plan = snap.portfolio_plan || {};
        plan.plan_status = snapStatus;
        plan.plan_built_at = snap.built_at || snap.built_at_wall_ist || null;

        const snapLocked =
          (snap && (snap.locked === true || snap.frozen === true)) ||
          (snap && snap.portfolio_plan && snap.portfolio_plan.plan_locked === true);

        plan.plan_locked = (snapLocked === true);
      } else {
        // Snapshot not ready / missing → no plan rows; tags/quotes still come from /api/state_raw
        plan = {
          plans: [],
          plan_status: snapStatus,
          plan_built_at: null,
          plan_locked: false,
          mode: (raw && raw.mode) || "paper",
          date: day || null,
        };
      }

      // Normalize status field if present
      if (plan && plan.status && !plan.plan_status) {
        plan.plan_status = plan.status;
      }

      const merged = mergeState(raw, plan);
      lastMerged = merged;

      renderSummary(merged.meta);
      renderGrid(merged);
    } catch (err) {
      console.error("state poll failed:", err);
      const fallbackMeta = {
        mode: "disconnected",
        date: null,
        clock: null,
        daily_risk_rs: null,
        risk_per_trade_rs: null,
        total_planned_risk_rs: null,
        active_trades: null,
        plan_status: null,
        plan_locked: null,
        sim: null,
      };
      renderSummary(fallbackMeta);
    }

    await new Promise((r) => setTimeout(r, STATE_POLL_MS));
  }
}


// ---------- Controls ----------

function initFilter() {
  const filterInput = document.getElementById("symbol-filter");
  if (!filterInput) return;
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

  // Single knob: config/frequency.yaml drives the universe + row ordering
  loadUniverseOrder();

  pollStateLoop();
  pollHealthLoop();
  refreshHeaderAuth();
  setInterval(refreshHeaderAuth, AUTH_POLL_MS);

  // NEW: smooth UI clock independent of API latency
  startUiClock();
}

document.addEventListener("DOMContentLoaded", init);
