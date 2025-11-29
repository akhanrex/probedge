// webui/js/live.js

async function fetchState() {
  const res = await fetch("/api/live_state", { cache: "no-cache" });
  if (!res.ok) {
    console.warn("State fetch failed", res.status);
    return null;
  }
  return await res.json();
}


function renderState(state) {
  if (!state) return;

  const meta = state.meta || {};
  const symbols = state.symbols || {};

  // Header
  document.getElementById("simDay").textContent   = meta.sim_day   || "-";
  document.getElementById("simClock").textContent = meta.sim_clock || "-";
  document.getElementById("simMode").textContent  = meta.mode      || "-";

  // Table body
  const tbody = document.querySelector("#liveGrid tbody");
  tbody.innerHTML = "";

  Object.entries(symbols).forEach(([sym, s]) => {
    const tr = document.createElement("tr");

    const ohlc = s.ohlc || {};
    const tags = s.tags || {};
    const plan = s.plan || {};

    const cells = [
      sym,
      s.ltp != null ? s.ltp.toFixed(2) : "-",
      ohlc.o != null ? ohlc.o.toFixed(2) : "-",
      ohlc.h != null ? ohlc.h.toFixed(2) : "-",
      ohlc.l != null ? ohlc.l.toFixed(2) : "-",
      ohlc.c != null ? ohlc.c.toFixed(2) : "-",
      s.volume != null ? s.volume.toLocaleString("en-IN") : "-",
      tags.OpeningTrend || "",
      tags.OpenLocation || "",
      tags.PrevDayContext || "",
      plan.pick || "",
      plan.confidence != null ? plan.confidence.toString() : ""
    ];

    for (const text of cells) {
      const td = document.createElement("td");
      td.textContent = text;
      tr.appendChild(td);
    }

    tbody.appendChild(tr);
  });
}

async function refreshLoop() {
  try {
    const state = await fetchState();
    renderState(state);
  } catch (err) {
    console.error("Error updating live grid", err);
  } finally {
    // poll every 1s – fine for 10 symbols
    setTimeout(refreshLoop, 1000);
  }
}

// kick off
refreshLoop();
