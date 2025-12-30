// webui/js/login.js
// Probedge Login UX with App PIN + Kite connection

// ======== CONFIG ========
const APP_PIN = "1234";               // Probedge PIN
const LS_KEY = "probedge_app_unlocked";
const SESSION_TTL_MINUTES = 30;       // PIN valid for 30 mins

// ======== Helpers =========
async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(`${url} → ${r.status} ${text || ""}`);
  }
  return r.json();
}

function setDotStatus(status) {
  const dot = document.getElementById("authDot");
  if (!dot) return;
  dot.classList.remove("ok", "warn", "err");
  if (status === "ok") dot.classList.add("ok");
  else if (status === "warn") dot.classList.add("warn");
  else if (status === "err") dot.classList.add("err");
}

function setMessage(msg, isError = false) {
  const el = document.getElementById("message");
  if (!el) return;
  el.textContent = msg || "";
  el.classList.toggle("error", !!isError);
}

// ======== App PIN logic =========
function updateLockUI(unlocked) {
  const kiteSection = document.getElementById("kiteSection");
  const connectBtn = document.getElementById("connectBtn");
  const openTerminalBtn = document.getElementById("openTerminalBtn");
  const authLabel = document.getElementById("authStatusLabel");
  const userEl = document.getElementById("userValue");

  if (!kiteSection) return;

  if (unlocked) {
    kiteSection.classList.remove("locked");
    if (authLabel) authLabel.textContent = "Checking…";
    if (userEl) {
      userEl.textContent = "Checking…";
      userEl.classList.add("muted");
    }
    if (connectBtn) connectBtn.disabled = false;
    if (openTerminalBtn) openTerminalBtn.disabled = false;
  } else {
    kiteSection.classList.add("locked");
    if (authLabel) authLabel.textContent = "Locked";
    if (userEl) {
      userEl.textContent = "Locked";
      userEl.classList.add("muted");
    }
    if (connectBtn) connectBtn.disabled = true;
    if (openTerminalBtn) openTerminalBtn.disabled = true;
    setDotStatus(null);
    setMessage("");
  }
}

function saveSessionUnlocked() {
  const payload = {
    unlocked: true,
    ts: Date.now(),
  };
  try {
    localStorage.setItem(LS_KEY, JSON.stringify(payload));
  } catch (_) {
    // ignore storage errors
  }
}

function loadSessionUnlocked() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return false;
    const obj = JSON.parse(raw);
    if (!obj || !obj.unlocked || !obj.ts) return false;
    const ageMs = Date.now() - obj.ts;
    const ttlMs = SESSION_TTL_MINUTES * 60 * 1000;
    if (ageMs > ttlMs) {
      // expired
      localStorage.removeItem(LS_KEY);
      return false;
    }
    return true;
  } catch (_) {
    // malformed data → clear and treat as locked
    localStorage.removeItem(LS_KEY);
    return false;
  }
}

function handleUnlockClick() {
  const input = document.getElementById("appPinInput");
  const msg = document.getElementById("pinMessage");
  if (!input || !msg) return;

  const val = (input.value || "").trim();
  if (!val) {
    msg.textContent = "Enter your Probedge PIN.";
    msg.classList.add("error");
    return;
  }

  if (val === APP_PIN) {
    saveSessionUnlocked();
    msg.textContent = "Unlocked.";
    msg.classList.remove("error");
    updateLockUI(true);
    // once unlocked, immediately check Kite auth
    refreshAuthStatus();
  } else {
    msg.textContent = "Incorrect PIN.";
    msg.classList.add("error");
  }
}

// ======== Kite auth + terminal navigation =========
async function refreshAuthStatus() {
  const authLabel = document.getElementById("authStatusLabel");
  const userEl = document.getElementById("userValue");
  const connectBtn = document.getElementById("connectBtn");
  const openTerminalBtn = document.getElementById("openTerminalBtn");

  try {
    setDotStatus("warn");
    if (authLabel) authLabel.textContent = "Checking…";

    const s = await fetchJSON("/api/auth/status");

    if (userEl) {
      if (s.session_user_id) {
        userEl.textContent = s.session_user_id;
        userEl.classList.remove("muted");
      } else {
        userEl.textContent = "Not set";
        userEl.classList.add("muted");
      }
    }

    if (s.has_valid_token_today) {
      // Already connected → go straight to terminal (after PIN unlock)
      setDotStatus("ok");
      if (authLabel) authLabel.textContent = "Connected";
      if (connectBtn) {
        connectBtn.textContent = "Re-connect Kite";
        connectBtn.disabled = false;
      }
      if (openTerminalBtn) openTerminalBtn.disabled = false;

      setMessage("Kite already connected. Opening console…");
      setTimeout(() => {
        window.location.href = "/webui/pages/dashboard.html";
      }, 700);
    } else {
      // Not connected yet
      setDotStatus("err");
      if (authLabel) authLabel.textContent = "Not connected";
      if (connectBtn) {
        connectBtn.disabled = false;
        connectBtn.textContent = "Connect Kite";
      }
      if (openTerminalBtn) openTerminalBtn.disabled = true;
      setMessage("Click “Connect Kite” to link Kite for today.");
    }
  } catch (e) {
    setDotStatus("err");
    if (authLabel) authLabel.textContent = "Error";
    setMessage(e.message || "Error while checking Kite auth.", true);
  }
}

async function handleConnectClick() {
  try {
    setMessage("Redirecting to Kite…");
    const data = await fetchJSON("/api/auth/login_url");
    if (!data || !data.login_url) {
      throw new Error("No login_url returned from API.");
    }
    window.location.href = data.login_url;
  } catch (e) {
    setMessage(e.message || "Error while creating Kite login URL.", true);
  }
}

function handleOpenTerminalClick() {
  // Direct navigation to live terminal
  window.location.href = "/webui/pages/dashboard.html";
}

// ======== Boot =========
document.addEventListener("DOMContentLoaded", () => {
  const unlockBtn = document.getElementById("unlockBtn");
  const connectBtn = document.getElementById("connectBtn");
  const openTerminalBtn = document.getElementById("openTerminalBtn");

  if (unlockBtn) unlockBtn.addEventListener("click", handleUnlockClick);
  if (connectBtn) connectBtn.addEventListener("click", handleConnectClick);
  if (openTerminalBtn) openTerminalBtn.addEventListener("click", handleOpenTerminalClick);

  // Enforce 30-minute session TTL
  const stillUnlocked = loadSessionUnlocked();
  updateLockUI(stillUnlocked);

  if (stillUnlocked) {
    // Already unlocked within TTL → just check Kite; if connected, we may jump to /live
    refreshAuthStatus();
  }
});
