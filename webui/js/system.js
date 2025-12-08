// webui/js/system.js
// System shell: auth gate + environment/risk/health from /api/state and /api/health
// Risk update via POST /api/risk.

async function getJSON(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`GET ${url} failed with ${res.status}`);
  }
  return res.json();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`POST ${url} failed with ${res.status}`);
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

function formatRs(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "₹ --";
  const num = Number(value);
  const sign = num > 0 ? "+" : "";
  return `₹ ${sign}${num.toFixed(0)}`;
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

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function updateSystemCards(meta, health, authStatus, settings) {
  const mode = (meta && meta.mode) || "PAPER";
  setText("sysMode", mode);

  const dataDir =
    (settings && settings.data_dir) ||
    (meta && meta.data_dir) ||
    "Configured in .env";
  setText("sysDataDir", dataDir);

  const symbols =
    (settings && Array.isArray(settings.symbols) && settings.symbols.join(", ")) ||
    (meta && Array.isArray(meta.symbols) && meta.symbols.join(", ")) ||
    "--";
  setText("sysSymbols", symbols);

  setText("sysDailyRisk", formatRs(meta.daily_risk_rs));
  setText("sysRiskPerTrade", formatRs(meta.risk_per_trade_rs));
  setText("sysPlannedRisk", formatRs(meta.total_planned_risk_rs));

  const systemStatus = (health && health.system_status) || "--";
  const agentStatus =
    (meta && meta.batch_agent && meta.batch_agent.status) || "--";
  const qcStatus = (health && health.qc_status) || "Unknown";
  setText("sysHealthStatus", systemStatus);
  setText("sysAgentStatus", agentStatus);
  setText("sysQcStatus", qcStatus);

  const kiteStatus = authStatus ? "Connected" : "Not connected";
  setText("sysKiteStatus", kiteStatus);
}

function initRiskForm(meta) {
  const input = document.getElementById("riskInput");
  const btn = document.getElementById("riskApplyBtn");
  const msg = document.getElementById("riskApplyMsg");
  if (!input || !btn || !msg) return;

  if (meta && meta.daily_risk_rs) {
    input.value = meta.daily_risk_rs;
  }

  btn.addEventListener("click", async () => {
    const raw = input.value;
    const val = Number(raw);
    if (!val || val < 1000) {
      msg.textContent = "Please enter a sensible daily risk (>= 1000).";
      msg.style.color = "var(--app-danger)";
      return;
    }
    msg.textContent = "Applying…";
    msg.style.color = "var(--app-text-muted)";
    try {
      await postJSON("/api/risk", { daily_risk_rs: val });
      msg.textContent = "Daily risk updated. New risk applies to future entries only.";
      msg.style.color = "var(--app-success)";
    } catch (err) {
      console.error("Failed to apply risk:", err);
      msg.textContent = "Failed to update risk. Check logs / backend.";
      msg.style.color = "var(--app-danger)";
    }
  });
}

async function loadSystem() {
  const authOk = await ensureAuthOrRedirect();
  if (!authOk) return;

  let meta = {};
  let health = {};
  let settings = null;

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

  // Optional: if backend exposes /api/config or similar, we can use it.
  try {
    settings = await getJSON("/api/config");
  } catch (err) {
    // It's optional; ignore if missing.
    console.warn("Config endpoint not available (optional):", err);
  }

  updateHeader(meta, health, true);
  updateSystemCards(meta, health, true, settings);

  const badge = document.getElementById("systemBadge");
  if (badge) {
    badge.textContent = "Loaded";
  }

  initRiskForm(meta);

  // Health detail text
  const healthDetail = document.getElementById("healthDetail");
  if (healthDetail) {
    const sys = (health && health.system_status) || "UNKNOWN";
    const reason = (health && health.reason) || "";
    const agent =
      (meta && meta.batch_agent && meta.batch_agent.status) || "Unknown";
    healthDetail.textContent = `System: ${sys} | Agent: ${agent}${
      reason ? " | " + reason : ""
    }`;
  }

  // QC detail text
  const qcDetail = document.getElementById("qcDetail");
  if (qcDetail) {
    if (health && health.qc_status) {
      qcDetail.textContent = `QC status: ${health.qc_status}${
        health.qc_detail ? " | " + health.qc_detail : ""
      }`;
    }
  }
}

document.addEventListener("DOMContentLoaded", () => {
  loadSystem().catch((err) => console.error("System init failed:", err));
});
