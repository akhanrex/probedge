const symbols = ["TATAMOTORS","LT","SBIN"];

// controls (ids must match HTML)
const riskInput     = document.getElementById("risk");
const entryModeSel  = document.getElementById("entryMode");
const killBtn       = document.getElementById("killswitch");

// ---- row builder ----
function row(sym){
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${sym}</td>
    <td id="${sym}-ltp" class="ltp num">-</td>
    <td id="${sym}-pdc"><span class="tag-pill">-</span></td>
    <td id="${sym}-ol"><span class="tag-pill">-</span></td>
    <td id="${sym}-ot"><span class="tag-pill">-</span></td>
    <td id="${sym}-pick"><span class="badge">-</span></td>
    <td id="${sym}-conf" class="num">-</td>
    <td id="${sym}-entry" class="num">-</td>
    <td id="${sym}-sl" class="num">-</td>
    <td id="${sym}-t1" class="num">-</td>
    <td id="${sym}-t2" class="num">-</td>
    <td id="${sym}-trigger" class="num">-</td>
    <td id="${sym}-entryAt" class="num">-</td>
    <td id="${sym}-entryBar" class="num">-</td>
    <td id="${sym}-exitAt" class="num">-</td>
    <td id="${sym}-exitReason">-</td>
    <td id="${sym}-status"><span class="badge status-WAITING">WAITING</span></td>
    <td id="${sym}-pnl"><span class="pnl pnl-flat">0.00</span></td>
  `;
  return tr;
}

// ---- helpers ----
function setText(id, v){
  const el = document.getElementById(id);
  if (!el) return;
  // if the cell contains a pill/span, write into that
  if (el.firstElementChild) {
    el.firstElementChild.textContent = v;
  } else {
    el.textContent = v;
  }
}

function fmtTimeEpoch(ts){
  if(!ts) return "-";
  try { return new Date(ts*1000).toLocaleTimeString(); } catch(e){ return "-"; }
}


async function fetchJSON(url){
  const r = await fetch(url, { cache: "no-store" });
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

// call these on load and on an interval
async function hydrateFromState(){
  try{
    const s = await fetchJSON("/api/state");
    if(s && s.symbols){
      const rows = [];
      for(const sym of Object.keys(s.symbols)){
        const o = s.symbols[sym];
        if(o.tags){
          rows.push({ type:"tags", symbol:sym, ...o.tags });
        }
        if (o.plan && (o.plan.Entry !== undefined && o.plan.Entry !== null)) {
          rows.push({
            type: "plan",
            symbol: sym,
            Pick: o.plan.Pick, Conf: o.plan.Conf, Entry: o.plan.Entry,
            SL: o.plan.SL, T1: o.plan.T1, T2: o.plan.T2, Qty: o.plan.Qty,
            Trigger: o.plan.Trigger
          });
        }
        if (o.trade) {
          rows.push({
            type: "status",
            symbol: sym,
            status: o.trade.status,
            pnl: 0,
            entry_at: o.trade.entry_at,
            entry_bar: o.trade.entry_bar,
            exit_at: o.trade.exit_at,
            exit_reason: o.trade.exit_reason
          });
        }
      }
      if(rows.length){
        handleBatch(rows);
      }
    }
  }catch(e){ /* silent */ }
}

function handleBatch(rows){
  for (const m of rows) {
    if (m.type === "tags") {
      if (m.PDC !== undefined) setText(`${m.symbol}-pdc`, m.PDC);
      if (m.OL  !== undefined) setText(`${m.symbol}-ol`,  m.OL);
      if (m.OT  !== undefined) setText(`${m.symbol}-ot`,  m.OT);
    } else if (m.type === "plan") {
      if (m.Pick) setPick(m.symbol, m.Pick);
      if (m.Conf !== undefined) setText(`${m.symbol}-conf`, m.Conf);
      if (m.Entry   !== undefined && m.Entry   !== null) setText(`${m.symbol}-entry`,   m.Entry.toFixed ? m.Entry.toFixed(2) : m.Entry);
      if (m.SL      !== undefined && m.SL      !== null) setText(`${m.symbol}-sl`,      m.SL.toFixed    ? m.SL.toFixed(2)    : m.SL);
      if (m.T1      !== undefined && m.T1      !== null) setText(`${m.symbol}-t1`,      m.T1.toFixed    ? m.T1.toFixed(2)    : m.T1);
      if (m.T2      !== undefined && m.T2      !== null) setText(`${m.symbol}-t2`,      m.T2.toFixed    ? m.T2.toFixed(2)    : m.T2);
      if (m.Trigger !== undefined && m.Trigger !== null) setText(`${m.symbol}-trigger`, m.Trigger.toFixed ? m.Trigger.toFixed(2) : m.Trigger);
      setStatus(m.symbol, m.Pick === "ABSTAIN" ? "ABSTAINED" : "READY");
    } else if (m.type === "status") {
      if (m.status) setStatus(m.symbol, m.status);
      if (m.entry_at    !== undefined) setText(`${m.symbol}-entryAt`,  fmtTimeEpoch(m.entry_at));
      if (m.entry_bar   !== undefined) setText(`${m.symbol}-entryBar`, m.entry_bar);
      if (m.exit_at     !== undefined) setText(`${m.symbol}-exitAt`,   fmtTimeEpoch(m.exit_at));
      if (m.exit_reason !== undefined) setText(`${m.symbol}-exitReason`, m.exit_reason);
      if (m.pnl         !== undefined) setPnL(m.symbol, m.pnl);
      if (m.trigger     !== undefined && m.trigger !== null) setText(`${m.symbol}-trigger`, m.trigger.toFixed ? m.trigger.toFixed(2) : m.trigger);
    }
  }
}


(async () => {
  try{
    const r = await fetch("/api/config"); 
    const j = await r.json();
    // if you have inputs for risk & entry_mode, reflect here (purely cosmetic)
    // document.querySelector('#riskInput').value = j.risk_rs;
    // document.querySelector('#entryMode').value = j.entry_mode;
  }catch(e){}
})();

async function refreshJournal(){
  try{
    const data = await fetchJSON("/api/journal");
    const body = document.getElementById("journal-body");
    if(!body) return;
    const rows = (data.rows || []).slice().reverse(); // newest first at top
    body.innerHTML = rows.map(r => {
      const pnlCls = (r.pnl||0) > 0 ? "pnl pnl-pos" : (r.pnl||0)<0 ? "pnl pnl-neg" : "pnl pnl-flat";
      return `<tr>
        <td>${r.date||""}</td>
        <td>${r.symbol||""}</td>
        <td><span class="badge ${r.pick==="BULL"?"pick-BULL":"pick-BEAR"}">${r.pick||""}</span></td>
        <td class="num">${r.entry??""}</td>
        <td class="num">${r.exit??""}</td>
        <td class="num">${r.qty??""}</td>
        <td><span class="badge status-${(r.result||"").replaceAll(" ","-")}">${r.result||""}</span></td>
        <td><span class="${pnlCls}">${(r.pnl??0).toFixed ? r.pnl.toFixed(2) : (r.pnl||0)}</span></td>
      </tr>`;
    }).join("");
  }catch(e){ /* ignore transient errors */ }
}

// during init (after ws connect handlers are set), add:
document.addEventListener("DOMContentLoaded", () => {
  const tbody = document.getElementById("terminalBody");
  symbols.forEach(s => tbody.appendChild(row(s)));

  hydrateFromState();
  refreshJournal();
  setInterval(refreshJournal, 10000); // refresh every 10s
});


function setStatus(sym, statusText){
  const cell = document.getElementById(`${sym}-status`);
  if (!cell) return;
  const pill = cell.firstElementChild || cell;
  pill.textContent = statusText;
  pill.className = `badge status-${statusText}`;
}

function setPick(sym, pick){
  const cell = document.getElementById(`${sym}-pick`);
  if (!cell) return;
  const pill = cell.firstElementChild || cell;
  pill.textContent = pick || "-";
  pill.className = "badge " + (pick ? `pick-${pick}` : "");
}

function setPnL(sym, val){
  const cell = document.getElementById(`${sym}-pnl`);
  if (!cell) return;
  const chip = cell.firstElementChild || cell;
  const num = Number(val || 0);
  chip.textContent = (isFinite(num) ? num.toFixed(2) : "0.00");
  chip.className = "pnl " + (num > 0 ? "pnl-pos" : num < 0 ? "pnl-neg" : "pnl-flat");
}

// ---- websocket ----
let ws;
function send(obj){ try{ ws && ws.readyState===1 && ws.send(JSON.stringify(obj)); }catch(e){} }

function connectWS(){
  const proto = (location.protocol === "https:") ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/ticks`);

  ws.onopen = () => {
    // push current controls on connect
    if (riskInput?.value)     send({type:"cfg", risk: Number(riskInput.value)});
    if (entryModeSel?.value)  send({type:"cfg", entry_mode: entryModeSel.value});
  };

  ws.onclose = () => { setTimeout(connectWS, 1000); };
  ws.onerror = () => { try{ws.close();}catch(e){} };

  ws.onmessage = (evt) => {
    try{
      const arr = JSON.parse(evt.data);
      for(const m of arr){
        if(m.type === "tick"){
          setText(`${m.symbol}-ltp`, (m.ltp?.toFixed ? m.ltp.toFixed(2) : m.ltp));
        } else if(m.type === "tags"){
          if(m.PDC !== undefined) setText(`${m.symbol}-pdc`, m.PDC);
          if(m.OL  !== undefined) setText(`${m.symbol}-ol`,  m.OL);
          if(m.OT  !== undefined) setText(`${m.symbol}-ot`,  m.OT);
        } else if(m.type === "plan"){
          if(m.Pick) setPick(m.symbol, m.Pick);
          if(m.Conf !== undefined) setText(`${m.symbol}-conf`, m.Conf);
          if(m.Entry !== null && m.Entry !== undefined) setText(`${m.symbol}-entry`, m.Entry.toFixed ? m.Entry.toFixed(2) : m.Entry);
          if(m.SL    !== null && m.SL    !== undefined) setText(`${m.symbol}-sl`,    m.SL.toFixed    ? m.SL.toFixed(2)    : m.SL);
          if(m.T1    !== null && m.T1    !== undefined) setText(`${m.symbol}-t1`,    m.T1.toFixed    ? m.T1.toFixed(2)    : m.T1);
          if(m.T2    !== null && m.T2    !== undefined) setText(`${m.symbol}-t2`,    m.T2.toFixed    ? m.T2.toFixed(2)    : m.T2);
          if(m.Trigger !== null && m.Trigger !== undefined) setText(`${m.symbol}-trigger`, m.Trigger.toFixed ? m.Trigger.toFixed(2) : m.Trigger);
          setStatus(m.symbol, m.Pick === "ABSTAIN" ? "ABSTAINED" : "READY");
        } else if(m.type === "status"){
          if(m.status) setStatus(m.symbol, m.status);
          if (m.entry_at !== undefined) setText(`${m.symbol}-entryAt`, fmtTimeEpoch(m.entry_at));
          if (m.entry_bar !== undefined) setText(`${m.symbol}-entryBar`, m.entry_bar);
          if (m.exit_at !== undefined) setText(`${m.symbol}-exitAt`, fmtTimeEpoch(m.exit_at));
          if (m.exit_reason !== undefined) setText(`${m.symbol}-exitReason`, m.exit_reason);
          if(m.pnl !== undefined) setPnL(m.symbol, m.pnl);
          if(m.trigger !== undefined && m.trigger !== null) setText(`${m.symbol}-trigger`, (m.trigger.toFixed ? m.trigger.toFixed(2) : m.trigger));
        } else if(m.type === "ack"){
          // no-op
        }
      }
    }catch(e){ console.error(e); }
  };
}
connectWS();

// ---- control events -> server ----
riskInput?.addEventListener("change", () => {
  const v = Number(riskInput.value || "1000");
  send({type:"cfg", risk: v});
});
entryModeSel?.addEventListener("change", () => {
  const v = entryModeSel.value || "6TO10";
  send({type:"cfg", entry_mode: v});
});
killBtn?.addEventListener("click", () => {
  send({type:"cmd", cmd:"KILL"});
});
