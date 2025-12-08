// webui/js/journal.js
// Journal shell: auth gate + basic state for Trades/Daily views

async function getJSON(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`GET ${url} failed with ${res.status}`);
  }
  return res.json();
}

async function ensureAuthOrRedirect() {
  const overlay = document.getElementById("authOverlay");
  try {
    const status = await getJSON("/api/auth/status");
    const ok = status && status.has_valid_token_today === true;
    const kiteText = ok ? "Kite: Connected" : "Kite: Not connected";

    const pillKiteText = document.getElementById("pillKiteText");
    if (pillKiteText) pillKiteText.textContent = kiteText;

    if (!ok) {
      window.location.href = "/login";
      return false;
    }
    overlay.classList.add("auth-overlay-hidden");
    const appRoot = document.getElementById("appRoot");
    if (appRoot) appRoot.style.display = "flex";
    return true;
  } catch (err) {
    console.error("Auth status check failed:", err);
    window.location.href = "/login";
    return false;
  }
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function formatRs(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "₹ --";
  const num = Number(value);
  const sign = num > 0 ? "+" : "";
  return `₹ ${sign}${num.toFixed(0)}`;
}

function formatClock(dateStr, clockStr) {
  if (!dateStr && !clockStr) return "-- · --:--:--";
  const date = dateStr || "--";
  const clock = clockStr || "--:--:--";
  return `${date} · ${clock}`;
}

function deriveRangeLabel(range) {
  if (range === "7d") return "Last 7 days";
  if (range === "all") return "All trades";
  return "Last 30 days";
}

function updateHeader(meta, health, authOk, range) {
  // Mode pill
  const mode = (meta && meta.mode) || "PAPER";
  const pillMode = document.getElementById("pillMode");
  const pillModeText = document.getElementById("pillModeText");
  if (pillMode && pillModeText) {
    pillMode.classList.remove(
      "app-pill-mode-paper",
      "app-pill-mode-live",
      "app-pill-mode-sim"
    );
    if (mode === "LIVE") {
      pillMode.classList.add("app-pill-mode-live");
    } else if (mode === "SIM") {
      pillMode.classList.add("app-pill-mode-sim");
    } else {
      pillMode.classList.add("app-pill-mode-paper");
    }
    pillModeText.textContent = mode;
  }

  // Clock
  const date = (meta && meta.date) || null;
  const clock =
    (meta && (meta.clock || meta.sim_clock || meta.sim_time)) || null;
  const headerClock = document.getElementById("headerClock");
  if (headerClock) headerClock.textContent = formatClock(date, clock);

  // Health pill
  const systemStatus = (health && health.system_status) || "UNKNOWN";
  const pillHealth = document.getElementById("pillHealth");
  const pillHealthText = document.getElementById("pillHealthText");
  if (pillHealth && pillHealthText) {
    pillHealth.classList.remove(
      "app-pill-health-ok",
      "app-pill-health-warn",
      "app-pill-health-error"
    );
    const statusUpper = String(systemStatus).toUpperCase();
    if (statusUpper === "OK") {
      pillHealth.classList.add("app-pill-health-ok");
    } else if (statusUpper === "WARN") {
      pillHealth.classList.add("app-pill-health-warn");
    } else {
      pillHealth.classList.add("app-pill-health-error");
    }
    pillHealthText.textContent = `Health: ${systemStatus}`;
  }

  // Kite pill
  const pillKiteText = document.getElementById("pillKiteText");
  if (pillKiteText) {
    pillKiteText.textContent = authOk ? "Kite: Connected" : "Kite: Not connected";
  }

  // Range badge
  const badge = document.getElementById("journalRangeBadge");
  if (badge) {
    badge.textContent = deriveRangeLabel(range);
  }
}

// Simple tab toggling between Trades and Daily views
function initTabs() {
  const tabTrades = document.getElementById("tabTrades");
  const tabDaily = document.getElementById("tabDaily");
  const tradesView = document.getElementById("tradesView");
  const dailyView = document.getElementById("dailyView");

  if (!tabTrades || !tabDaily || !tradesView || !dailyView) return;

  tabTrades.addEventListener("click", () => {
    tabTrades.classList.add("active");
    tabDaily.classList.remove("active");
    tradesView.style.display = "";
    dailyView.style.display = "none";
  });

  tabDaily.addEventListener("click", () => {
    tabDaily.classList.add("active");
    tabTrades.classList.remove("active");
    dailyView.style.display = "";
    tradesView.style.display = "none";
  });
}

async function loadJournal() {
  const authOk = await ensureAuthOrRedirect();
  if (!authOk) return;

  // Basic meta + health for header
  let meta = {};
  let health = {};
  try {
    const state = await getJSON("/api/state");
    meta = state && state.meta ? state.meta : {};
  } catch (err) {
    console.error("Failed to load /api/state:", err);
  }

  try {
    health = await getJSON("/api/health");
  } catch (err) {
    console.error("Failed to load /api/health:", err);
  }

  // Default range = 30d
  const filterRange = document.getElementById("filterRange");
  const currentRange = filterRange ? filterRange.value : "30d";
  updateHeader(meta, health, true, currentRange);

  // Journal APIs are optional at this stage: we try, but page stays usable if missing
  await tryLoadTrades(currentRange);
  await tryLoadDaily();
}

async function tryLoadTrades(range) {
  const tbody = document.getElementById("tradesBody");
  if (!tbody) return;

  // Clear placeholder row
  tbody.innerHTML = "";

  let data;
  // We guess an endpoint; if it doesn't exist yet, we fall back
  let query = "";
  if (range === "7d") query = "?days=7";
  else if (range === "30d") query = "?days=30";
  else query = "";

  try {
    data = await getJSON(`/api/journal/trades${query}`);
  } catch (err) {
    console.warn("Journal trades API not available yet:", err);
    tbody.innerHTML =
      '<tr><td colspan="11" style="color: var(--app-text-muted)">Journal trades API not available yet. This page will auto-fill once backend exposes /api/journal/trades.</td></tr>';
    return;
  }

  const rows = (data && (data.rows || data.trades)) || [];
  if (!rows.length) {
    tbody.innerHTML =
      '<tr><td colspan="11" style="color: var(--app-text-muted)">No trades in selected range.</td></tr>';
    return;
  }

  // For now, we render as-is with defensive access
  const dirFilter =
    (document.getElementById("filterDirection") || {}).value || "ALL";
  const symFilter = (document.getElementById("filterSymbol") || {}).value || "ALL";
  const resFilter =
    (document.getElementById("filterResult") || {}).value || "ALL";

  const filtered = rows.filter((r) => {
    const sym = (r.symbol || r.SYMBOL || "").toUpperCase();
    const pick = (r.pick || r.direction || r.PICK || "").toUpperCase();
    const result = (r.result || r.RESULT || "").toUpperCase();

    if (symFilter !== "ALL" && sym !== symFilter) return false;
    if (dirFilter !== "ALL" && pick !== dirFilter) return false;
    if (resFilter !== "ALL") {
      if (resFilter === "WIN" && result !== "WIN") return false;
      if (resFilter === "LOSS" && result !== "LOSS") return false;
    }
    return true;
  });

  if (!filtered.length) {
    tbody.innerHTML =
      '<tr><td colspan="11" style="color: var(--app-text-muted)">No trades match current filters.</td></tr>';
    return;
  }

  const rowsHtml = filtered
    .map((r) => {
      const date = r.date || r.DATE || "--";
      const time = r.time_entry || r.time || r.TIME || "--";
      const sym = (r.symbol || r.SYMBOL || "").toUpperCase();
      const pick = (r.pick || r.direction || r.PICK || "").toUpperCase();
      const qty = r.qty || r.QTY || r.quantity || "";
      const entry = r.entry || r.entry_price || r.ENTRY || "";
      const exit = r.exit || r.exit_price || r.EXIT || "";
      const risk = r.risk_rs || r.RISK_RS || r.risk || "";
      const pnl = r.pnl_rs || r.PNL_RS || r.pnl || "";
      const rMult = r.r_multiple || r.R_MULTIPLE || r.r_mult || "";
      const result = (r.result || r.RESULT || "").toUpperCase();

      const pnlClass =
        pnl > 0 ? "text-positive" : pnl < 0 ? "text-negative" : "";

      return `
        <tr>
          <td>${date}</td>
          <td>${time}</td>
          <td>${sym}</td>
          <td>${pick}</td>
          <td>${qty}</td>
          <td>${entry}</td>
          <td>${exit}</td>
          <td>${risk}</td>
          <td class="${pnlClass}">${pnl}</td>
          <td>${rMult}</td>
          <td>${result}</td>
        </tr>
      `;
    })
    .join("");

  tbody.innerHTML = rowsHtml;
}

async function tryLoadDaily() {
  const tbody = document.getElementById("dailyBody");
  if (!tbody) return;

  tbody.innerHTML = "";

  let data;
  try {
    data = await getJSON("/api/journal/daily?limit=30");
  } catch (err) {
    console.warn("Journal daily API not available yet:", err);
    tbody.innerHTML =
      '<tr><td colspan="4" style="color: var(--app-text-muted)">Journal daily API not available yet. This table will auto-fill once backend exposes /api/journal/daily.</td></tr>';
    return;
  }

  const rows = (data && (data.rows || data.daily)) || [];
  if (!rows.length) {
    tbody.innerHTML =
      '<tr><td colspan="4" style="color: var(--app-text-muted)">No daily P&L data.</td></tr>';
    return;
  }

  const rowsHtml = rows
    .map((r) => {
      const date = r.date || r.DATE || "--";
      const pnl = r.pnl_rs || r.PNL_RS || r.pnl || "";
      const trades = r.num_trades || r.trades || r.NUM_TRADES || "";
      const winRate =
        r.win_rate != null ? Math.round((Number(r.win_rate) || 0) * 100) : null;
      const pnlClass =
        pnl > 0 ? "text-positive" : pnl < 0 ? "text-negative" : "";

      return `
        <tr>
          <td>${date}</td>
          <td class="${pnlClass}">${pnl}</td>
          <td>${trades}</td>
          <td>${winRate != null ? winRate + "%" : "--"}</td>
        </tr>
      `;
    })
    .join("");

  tbody.innerHTML = rowsHtml;
}

function initFilters() {
  const range = document.getElementById("filterRange");
  const symbol = document.getElementById("filterSymbol");
  const direction = document.getElementById("filterDirection");
  const result = document.getElementById("filterResult");

  if (range) {
    range.addEventListener("change", () => {
      const val = range.value;
      const badge = document.getElementById("journalRangeBadge");
      if (badge) badge.textContent = deriveRangeLabel(val);
      tryLoadTrades(val);
    });
  }

  const refetch = () => {
    const rangeVal = range ? range.value : "30d";
    tryLoadTrades(rangeVal);
  };

  if (symbol) symbol.addEventListener("change", refetch);
  if (direction) direction.addEventListener("change", refetch);
  if (result) result.addEventListener("change", refetch);
}

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initFilters();
  loadJournal().catch((err) => console.error("Journal init failed:", err));
});
