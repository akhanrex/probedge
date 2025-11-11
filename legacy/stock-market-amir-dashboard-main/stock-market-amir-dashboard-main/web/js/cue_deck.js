// web/js/cue_deck.js
(() => {
  const API = "/api/cues?syms=TATAMOTORS,LT,SBIN"; // adjust if you want dynamic list
  const mount = document.getElementById("cuesDeck");
  if (!mount) return;

  fetch(API)
    .then(r => r.json())
    .then(data => {
      if (!data || !Array.isArray(data.symbols)) return;
      // Sort by strongest bias angle/consistency combo (optional)
      const scored = data.symbols.map(row => {
        const m = row?.superpath?.meta || {};
        const angle = Math.abs(Number(m.angle_deg) || 0);
        const cons = Number(m.consistency) || 0;
        const score = 0.6 * Math.min(1, Math.max(0, (angle - 10) / 20)) + 0.4 * (cons / 100);
        return { ...row, __score: score };
      }).sort((a, b) => (b.__score || 0) - (a.__score || 0));

      scored.forEach(item => mount.appendChild(card(item)));
    })
    .catch(err => {
      console.error("cues fetch error:", err);
    });

  function card(item) {
    const sym = item.symbol || "-";
    const cue = item.cue || {};
    const sp = item.superpath || {};
    const meta = sp.meta || {};
    const tone = (cue.tone || "neutral").toLowerCase();
    const sideTxt = cue.final_side || "NO TRADE";

    const el = document.createElement("article");
    el.className = "cue-card";
    el.innerHTML = `
      <div class="card-header">
        <h3 style="margin:0;font-size:16px;letter-spacing:0.5px">${sym}</h3>
        <span class="badge">Neff: ${fmt(meta.Neff)} · N: ${meta.N ?? "-"}</span>
      </div>
      <div class="mini" style="margin-bottom:6px">
        <b class="tone-${tone}">${sideTxt}</b>
        <span> • Bias: ${meta.bias ?? "-"}</span>
        <span> • θ: ${fmt(meta.angle_deg)}°</span>
        <span> • Cons: ${meta.consistency ?? "-"}</span>
        <span> • Freq: ${cue.fPick ?? "-"} @ ${cue.fConf ?? 0}% (${cue.level ?? "-"})</span>
      </div>
      <canvas class="spark"></canvas>
      <div class="mini" style="margin-top:6px">${cue.reason ?? ""}</div>
    `;

    // draw cone
    const bars = Array.isArray(sp.bars) ? sp.bars : [];
    drawCone(el.querySelector("canvas"), bars);

    return el;
  }

  function fmt(v) {
    const n = Number(v);
    return Number.isFinite(n) ? (Math.round(n * 10) / 10) : "-";
  }

  function drawCone(canvas, bars) {
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const W = canvas.width = canvas.clientWidth;
    const H = canvas.height = canvas.clientHeight;

    if (!bars || bars.length < 2) {
      ctx.fillStyle = "#333";
      ctx.fillRect(0,0,W,H);
      return;
    }

    // y-range from percent moves (include cone bounds)
    const all = bars.flatMap(b => [b.p25 ?? 0, b.mean ?? 0, b.p75 ?? 0]);
    const ymin = Math.min(...all);
    const ymax = Math.max(...all);
    const pad = 0.2 * (ymax - ymin || 1);
    const lo = ymin - pad, hi = ymax + pad;

    const x = (i) => (i / (bars.length - 1)) * (W - 8) + 4;
    const y = (p) => H - ((p - lo) / (hi - lo)) * (H - 8) - 4;

    // background
    ctx.fillStyle = "#0a0a0a";
    ctx.fillRect(0, 0, W, H);

    // cone fill (p25..p75)
    ctx.beginPath();
    bars.forEach((b, i) => {
      const xi = x(i);
      const yi = y(b.p75 ?? 0);
      if (i === 0) ctx.moveTo(xi, yi);
      else ctx.lineTo(xi, yi);
    });
    for (let i = bars.length - 1; i >= 0; i--) {
      const b = bars[i];
      ctx.lineTo(x(i), y(b.p25 ?? 0));
    }
    ctx.closePath();
    ctx.fillStyle = "rgba(100, 149, 237, 0.18)"; // soft cone
    ctx.fill();

    // mean line
    ctx.beginPath();
    bars.forEach((b, i) => {
      const xi = x(i);
      const yi = y(b.mean ?? 0);
      if (i === 0) ctx.moveTo(xi, yi);
      else ctx.lineTo(xi, yi);
    });
    ctx.lineWidth = 2;
    ctx.strokeStyle = "#cfd8ff";
    ctx.stroke();

    // zero line
    const yz = y(0);
    ctx.beginPath();
    ctx.moveTo(0, yz);
    ctx.lineTo(W, yz);
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(160,160,160,0.3)";
    ctx.stroke();
  }
})();
