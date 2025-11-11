const tbody = document.getElementById("terminalBody");
const riskInput = document.getElementById("risk");
const entryModeSel = document.getElementById("entryMode");
const modeSpan = document.getElementById("mode");

const ws = new WebSocket(`ws://${location.host}/ws/ticks`);

const symbols = ["TATAMOTORS","LT","SBIN"];

function row(sym){
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${sym}</td>
    <td id="${sym}-ltp" class="num">-</td>
    <td id="${sym}-pdc"><span class="tag-pill">-</span></td>
    <td id="${sym}-ol"><span class="tag-pill">-</span></td>
    <td id="${sym}-ot"><span class="tag-pill">-</span></td>
    <td id="${sym}-pick"><span class="badge">-</span></td>
    <td id="${sym}-conf" class="num">-</td>
    <td id="${sym}-entry" class="num">-</td>
    <td id="${sym}-trigger" class="num">-</td>
    <td id="${sym}-sl" class="num">-</td>
    <td id="${sym}-t1" class="num">-</td>
    <td id="${sym}-t2" class="num">-</td>
    <td id="${sym}-qty" class="num">-</td>
    <td id="${sym}-status"><span class="badge">-</span></td>
  `;
  return tr;
}

function initRows(){
  symbols.forEach(s => tbody.appendChild(row(s)));
}

function setTxt(id, v){
  const el = document.getElementById(id);
  if (el) el.textContent = v;
}

function hydrate(sym, st){
  setTxt(`${sym}-ltp`, (st.ltp ?? "-"));
  const t = st.tags || {};
  setTxt(`${sym}-pdc`, t.pdc ?? "-");
  setTxt(`${sym}-ol`, t.ol ?? "-");
  setTxt(`${sym}-ot`, t.ot ?? "-");
  const p = st.plan || {};
  setTxt(`${sym}-pick`, p.direction ?? "-");
  setTxt(`${sym}-conf`, p.confidence ?? "-");
  setTxt(`${sym}-entry`, p.entry_ref ?? "-");
  setTxt(`${sym}-trigger`, p.trigger ?? "-");
  setTxt(`${sym}-sl`, p.stop ?? "-");
  setTxt(`${sym}-t1`, p.t1 ?? "-");
  setTxt(`${sym}-t2`, p.t2 ?? "-");
  setTxt(`${sym}-qty`, p.qty ?? "-");
  setTxt(`${sym}-status`, p.status ?? "-");
}

async function boot(){
  initRows();
  const res = await fetch("/api/state");
  const s = await res.json();
  modeSpan.textContent = s.mode ?? "-";
}

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.type === "state") {
    hydrate(msg.symbol, msg.state);
  }
};

riskInput.addEventListener("change", async () => {
  await fetch("/api/config", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({risk_rs: +riskInput.value})});
});

entryModeSel.addEventListener("change", async () => {
  await fetch("/api/config", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({entry_mode: entryModeSel.value})});
});

boot();
