async function getJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(url + " -> " + r.status);
  return r.json();
}

function setText(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }

async function boot() {
  const cfg = await getJSON("/api/settings").catch(()=>({mode:"?",bar_seconds:"?",symbols:[]}));
  setText("mode", cfg.mode);
  setText("bar", cfg.bar_seconds);
  setText("symbols", (cfg.symbols||[]).join(","));

  const qBody = document.querySelector("#quotes tbody");

  function paint(state) {
    try {
      setText("hb", (state.heartbeat||"—").replace("T"," ").split("+")[0]);
      const quotes = state.quotes || {};
      qBody.innerHTML = "";
      Object.entries(quotes).forEach(([sym, q]) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${sym}</td><td>${q.DateTime||"—"}</td><td>${q.Close!=null?q.Close:"—"}</td>`;
        qBody.appendChild(tr);
      });
      const ctrl = state.control || {};
      document.getElementById("ctrlState").textContent = JSON.stringify(ctrl, null, 2);
    } catch(e) {}
  }

  // SSE with poll fallback
  let ev;
  try {
    ev = new EventSource("/api/state/stream");
    ev.onmessage = (msg) => {
      const st = JSON.parse(msg.data || "{}");
      paint(st);
    };
    ev.onerror = () => { /* keep alive; server restarts will reconnect */ };
  } catch (e) {
    setInterval(async () => {
      try {
        const r = await getJSON("/api/state");
        paint(r.data || {});
      } catch(e) {}
    }, 2000);
  }

  // Wire controls
  document.getElementById("armBtn").onclick = async () => {
    const body = {
      symbol: document.getElementById("sym").value || "TMPV",
      side: document.getElementById("side").value || "BUY",
      qty: parseInt(document.getElementById("qty").value||"1",10),
      note: document.getElementById("note").value || ""
    };
    const r = await fetch("/api/arm", { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body) });
    if (!r.ok) { alert("Arm failed"); return; }
    const j = await r.json();
    document.getElementById("ctrlState").textContent = JSON.stringify(j.state||{}, null, 2);
  };

  document.getElementById("stopBtn").onclick = async () => {
    const r = await fetch("/api/stop", { method: "POST" });
    if (!r.ok) { alert("Stop failed"); return; }
    const j = await r.json();
    document.getElementById("ctrlState").textContent = JSON.stringify(j.state||{}, null, 2);
  };
}

boot();
