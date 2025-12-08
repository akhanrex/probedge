// webui/js/dashboard.js
// Dashboard shell: auth gate + summary from /api/state and /api/health

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

    // We also set header/journal card text here; if shell isn't mounted yet,
    // dashboard init will overwrite it later.
    const pillKiteText = document.getElementById("pillKiteText");
    const cardKiteStatus = document.getElementById("cardKiteStatus");
    if (pillKiteText) pillKiteText.textContent = kiteText;
    if (cardKiteStatus) cardKiteStatus.textContent = kiteText;

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
    // Conservative: force login if auth check fails
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

function derivePhase(clockStr) {
  if (!clockStr) return "Phase: --";
  const hhmmss = clockStr.slice(0, 8);
  if (hhmmss < "09:25:00") return "Phase: Pre-market";
  if (hhmmss < "09:30:00") return "Phase: PDC ready";
  if (hhmmss < "09:40:00") return "Phase: PDC + OL ready";
  return "Phase: Full plan";
}

function updateHeader(meta, health, authStatus) {
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

  // Kite pill (from authStatus if provided)
  const pillKiteText = document.getElementById("pillKiteText");
  if (pillKiteText && authStatus) {
    pillKiteText.textContent = authStatus
      ? "Kite: Connected"
      : "Kite: Not connected";
  }

  // Page subtitle & phase badge
  const pageSubtitle = document.getElementById("pageSubtitle");
  const phaseBadge = document.getElementById("phaseBadge");
  if (pageSubtitle && date) {
    pageSubtitle.textContent = `Today · ${date}`;
  }
  if (phaseBadge) {
    phaseBadge.textContent = derivePhase(clock);
  }
}

function updateSummaryCards(meta, health, authOk) {
  if (!meta) meta = {};

  // Risk card
  setText("cardDailyRisk", formatRs(meta.daily_risk_rs));
  setText("cardPlannedRisk", formatRs(meta.total_planned_risk_rs));
  setText("cardRiskPerTrade", formatRs(meta.risk_per_trade_rs));

  // PnL card
  setText("cardPnlDay", formatRs(meta.pnl_day));
  setText("cardPnlOpen", formatRs(meta.pnl_open));
  setText("cardPnlRealized", formatRs(meta.pnl_realized));

  // Risk state
  const riskState = (meta.risk_state && meta.risk_state.status) || "--";
  const riskReason = (meta.risk_state && meta.risk_state.reason) || "--";
  setText("cardRiskState", riskState);
  setText("cardRiskReason", riskReason);

  // System card
  const agent =
    (meta.batch_agent && meta.batch_agent.status) || "Unknown agent";
  const healthStatus = (health && health.system_status) || "--";
  setText("cardAgentStatus", agent);
  setText("cardHealthStatus", healthStatus);
  setText("cardKiteStatus", authOk ? "Connected" : "Not connected");
}

async function loadDashboard() {
  // Auth gate first
  const authOk = await ensureAuthOrRedirect();
  if (!authOk) return;

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

  // We already know auth is OK at this point
  updateHeader(meta, health, true);
  updateSummaryCards(meta, health, true);

  // NOTE: today’s plan snapshot & last 5 sessions will be wired later
}

document.addEventListener("DOMContentLoaded", () => {
  loadDashboard().catch((err) =>
    console.error("Dashboard init failed:", err)
  );
});
