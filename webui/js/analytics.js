// webui/js/analytics.js
// Analytics shell: auth gate + header from /api/state and /api/health
// Analytics metrics will be wired when dedicated endpoints exist.

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

function formatClock(dateStr, clockStr) {
  if (!dateStr && !clockStr) return "-- · --:--:--";
  const date = dateStr || "--";
  const clock = clockStr || "--:--:--";
  return `${date} · ${clock}`;
}

function updateHeader(meta, health, authOk) {
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

  const date = (meta && meta.date) || null;
  const clock =
    (meta && (meta.clock || meta.sim_clock || meta.sim_time)) || null;
  const headerClock = document.getElementById("headerClock");
  if (headerClock) headerClock.textContent = formatClock(date, clock);

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

  const pillKiteText = document.getElementById("pillKiteText");
  if (pillKiteText) {
    pillKiteText.textContent = authOk ? "Kite: Connected" : "Kite: Not connected";
  }
}

async function loadAnalytics() {
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

  updateHeader(meta, health, true);

  // When analytics endpoints exist, we will pull them here and fill:
  // - analyticsTotalPnl, analyticsMaxDd, analyticsMaxDdRs,
  //   analyticsWinRate, analyticsNumTrades, analyticsExpectancy,
  //   analyticsSymbolBody, analyticsWeekdayBody, etc.
  const badge = document.getElementById("analyticsBadge");
  if (badge) {
    badge.textContent = "Awaiting analytics endpoints";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  loadAnalytics().catch((err) =>
    console.error("Analytics init failed:", err)
  );
});
