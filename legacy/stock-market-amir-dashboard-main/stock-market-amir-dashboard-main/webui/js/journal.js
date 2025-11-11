// webui/js/journal.js
// Journal view: loads rows (API if available, else CSV), sorts newest-first, shows summary + table.

async function preferJSON(url){
  const r = await fetch(url, { cache: "no-store" });
  if(!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

function parseCSV(text){
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return [];
  const hdr = lines.shift().split(",").map(s=>s.trim());
  const idx = Object.fromEntries(hdr.map((c,i)=>[c, i]));
  const get = (arr, key, fallback="") => {
    const i = idx[key]; if (i==null) return fallback;
    const v = arr[i];
    return (typeof v === "string" ? v.trim() : v) ?? fallback;
  };
  const rows = [];
  for (const ln of lines){
    if (!ln) continue;
    const a = ln.split(",");
    rows.push({
      Date:         String(get(a,"Date","")),
      SignalSide:   String(get(a,"SignalSide","")),
      EntryPrice: +get(a,"EntryPrice","0"),
      SLPrice:    +get(a,"SLPrice","0"),
      TargetPrice:+get(a,"TargetPrice","0"),
      RiskUsed:   +get(a,"RiskUsed","0"),
      PnL:        +get(a,"PnL","0"),
      DecisionNote: String(get(a,"DecisionNote",""))
    });
  }
  return rows;
}

// Load rows with graceful fallbacks.
// Order of attempts:
//  1) /api/journal  (JSON: {rows:[...]})
//  2) /api/journal.csv  (raw CSV if you expose it)
//  3) /data/journal/Journal.csv  (repo file; may be empty and that's OK)
async function loadRows(){
  try {
    const j = await preferJSON("/api/journal");
    if (Array.isArray(j?.rows)) return j.rows;
  } catch(_) {}

  try {
    const r = await fetch("/api/journal.csv", { cache: "no-store" });
    if (r.ok) return parseCSV(await r.text());
  } catch(_) {}

  const r2 = await fetch("/data/journal/Journal.csv", { cache: "no-store" });
  if (!r2.ok) return [];
  return parseCSV(await r2.text());
}

function sortNewestFirst(rows){
  rows.sort((a,b)=>{
    const ad = String(a?.Date||"");
    const bd = String(b?.Date||"");
    const lex = bd.localeCompare(ad); // ISO yyyy-mm-dd wins
    if (lex !== 0) return lex;
    return (Date.parse(bd)||0) - (Date.parse(ad)||0);
  });
  return rows;
}

function fmtAmt(x){
  if (x==null || Number.isNaN(+x)) return "—";
  const sign = +x < 0 ? "-" : "";
  const v = Math.abs(+x);
  return sign + "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
}
function pct(x){ return isFinite(x) ? (x*100).toFixed(1)+"%" : "—"; }

function summarize(rows){
  const s = { n:0, wins:0, net:0, avgR:0, risk:0 };
  for (const r of rows){
    s.n++;
    const pnl = +r.PnL || 0;
    const risk = +r.RiskUsed || 0;
    if (pnl > 0) s.wins++;
    s.net += pnl;
    s.risk += risk;
    s.avgR += risk ? (pnl/risk) : 0;
  }
  if (s.n) s.avgR = s.avgR / s.n;
  s.winPct = s.n ? s.wins/s.n : 0;
  return s;
}

export async function renderJournal(dock){
  const wrap = document.createElement("div");
  wrap.className = "journal-wrap";
  wrap.innerHTML = `
    <div class="charts-toolbar">
      <div class="meta">Journal</div>
      <div class="spacer"></div>
      <div class="controls">
        <input id="jrnlFilter" class="input" placeholder="Filter by note/side…" />
      </div>
    </div>
    <div class="journal-summary" id="jrnlSummary"></div>
    <div class="journal-table-wrap">
      <table class="matches-table" id="jrnlTable">
        <thead><tr>
          <th>Date</th>
          <th>Side</th>
          <th>Entry</th>
          <th>SL</th>
          <th>Target</th>
          <th>Risk Used</th>
          <th>P&amp;L</th>
          <th>Note</th>
        </tr></thead>
        <tbody></tbody>
      </table>
      <div class="empty-note" id="jrnlEmpty" style="display:none">No journal entries yet.</div>
    </div>
  `;
  dock.innerHTML = "";
  dock.appendChild(wrap);

  let rows = await loadRows();
  rows = sortNewestFirst(rows);

  const summaryEl = wrap.querySelector("#jrnlSummary");
  const tb = wrap.querySelector("#jrnlTable tbody");
  const emptyEl = wrap.querySelector("#jrnlEmpty");

  function drawSummary(list){
    const s = summarize(list);
    summaryEl.innerHTML = `
      <div class="summary-grid">
        <div class="card"><div class="k">Trades</div><div class="v">${s.n}</div></div>
        <div class="card"><div class="k">Win%</div><div class="v">${pct(s.winPct)}</div></div>
        <div class="card"><div class="k">Net P&amp;L</div><div class="v ${s.net>=0?"pos":"neg"}">${fmtAmt(s.net)}</div></div>
        <div class="card"><div class="k">Avg R</div><div class="v">${(s.avgR||0).toFixed(2)}</div></div>
        <div class="card"><div class="k">Risk Used</div><div class="v">${fmtAmt(s.risk)}</div></div>
      </div>
    `;
  }

  function drawTable(list){
    tb.innerHTML = "";
    for (const r of list){
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.Date||"-"}</td>
        <td>${r.SignalSide||"-"}</td>
        <td>${r.EntryPrice??"-"}</td>
        <td>${r.SLPrice??"-"}</td>
        <td>${r.TargetPrice??"-"}</td>
        <td>${fmtAmt(r.RiskUsed)}</td>
        <td class="${(+r.PnL)>=0?"pos":"neg"}">${fmtAmt(r.PnL)}</td>
        <td>${(r.DecisionNote||"").slice(0,200)}</td>
      `;
      tb.appendChild(tr);
    }
    const has = list.length > 0;
    emptyEl.style.display = has ? "none" : "block";
  }

  drawSummary(rows);
  drawTable(rows);

  const inp = wrap.querySelector("#jrnlFilter");
  inp.addEventListener("input", ()=>{
    const q = inp.value.trim().toLowerCase();
    const filtered = q
      ? rows.filter(r =>
          (r.DecisionNote||"").toLowerCase().includes(q) ||
          (r.SignalSide||"").toLowerCase().includes(q)
        )
      : rows;
    drawSummary(filtered);
    drawTable(filtered);
  });
}
