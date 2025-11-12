
async function j(url, opts={}) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(url+" -> "+r.status);
  return await r.json();
}
const hbEl = document.getElementById("hb");
const modeP = document.getElementById("mode-pill");
const barP  = document.getElementById("bar-pill");
const symsP = document.getElementById("symbols-pill");
const symSel = document.getElementById("sym");
const stratSel = document.getElementById("strategy");
const planT = document.getElementById("plan");
const ctrlStatus = document.getElementById("ctrl-status");

function renderPlan(plan) {
  const rows = [];
  if (!plan || !plan.symbol) {
    rows.push(`<tr><td colspan="2">No plan yet.</td></tr>`);
  } else {
    const kv = [
      ["Symbol", plan.symbol],
      ["Date", plan.date],
      ["OpeningTrend", plan.OpeningTrend],
      ["Pick", plan.Pick],
      ["Confidence%", plan.Confidence%],
      ["Entry", plan.Entry],
      ["Stop", plan.Stop],
      ["Risk/Share", plan.RiskPerShare],
      ["Qty", plan.Qty],
      ["Target1", plan.Target1],
      ["Target2", plan.Target2],
      ["ORB_H / ORB_L", `${plan.ORB_H} / ${plan.ORB_L}`],
      ["Prev_H / Prev_L", `${plan.Prev_H ?? '-'} / ${plan.Prev_L ?? '-'}`],
      ["Reason", plan.Reason],
    ];
    for (const [k,v] of kv) {
      rows.push(`<tr><th>${k}</th><td class="mono">${v ?? '-'}</td></tr>`);
    }
  }
  planT.innerHTML = rows.join("");
}

async function refresh() {
  try {
    const cfg = await j("/api/settings");
    modeP.textContent = "MODE: "+cfg.mode;
    barP.textContent = "BAR: "+cfg.bar_seconds+"s";
    symsP.textContent = "Symbols: "+(cfg.symbols || []).join(",");
    if (!symSel.options.length) {
      symSel.innerHTML = (cfg.symbols || []).map(s => `<option value="${s}">${s}</option>`).join("");
    }
  } catch (e) {}

  try {
    const st = await j("/api/state");
    hbEl.textContent = st.data && st.data.heartbeat || "—";
    renderPlan(st.data && st.data.plan);
  } catch (e) {}
}

async function arm() {
  const symbol = symSel.value || "TMPV";
  const strategy = stratSel.value || "batch_v1";
  ctrlStatus.textContent = "arming...";
  try {
    await j("/api/arm", {
      method: "POST",
      headers: {"content-type":"application/json"},
      body: JSON.stringify({ symbol, strategy })
    });
    ctrlStatus.textContent = "armed";
    setTimeout(refresh, 500);
  } catch (e) {
    ctrlStatus.textContent = "error: "+e.message;
  }
}

async function stop() {
  ctrlStatus.textContent = "stopping...";
  try {
    await j("/api/stop", { method:"POST" });
    ctrlStatus.textContent = "stopped";
    setTimeout(refresh, 300);
  } catch (e) {
    ctrlStatus.textContent = "error: "+e.message;
  }
}

document.getElementById("arm").addEventListener("click", arm);
document.getElementById("stop").addEventListener("click", stop);

refresh();
setInterval(refresh, 1500);
