# ws/server.py
import os, asyncio, json, time, contextlib
from aiohttp import web
import pandas as pd
from storage.master_store import read_master_headers, read_tm5
from decision.tags_engine import compute_tags_5
from infra.config import SYMBOLS, BAR_SECONDS, RISK_RS_DEFAULT, ENTRY_MODE, ENTRY_EPS_BPS
from storage import tm5min_store
from storage.journal_store import append_trade
from storage.live_state import save_state
from realtime.bars.five_min_agg import BarAggregator
from decision.levels import orb_from_bars, sl_targets_from_rules
from decision.risk_plan import ensure_targets
from risk.sizing import qty_from_risk
from realtime.feed_router import get_tick_stream
# Load .env (so MODE, KITE_* etc. are available even when not exported by shell)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

IS_PAPER = os.getenv("MODE","paper").lower() == "paper"

# --- new: drawdown + paper nudge knobs from env ---
def _dd_limit_rs():
    # DAILY_MAX_DD_RS overrides; else default 3x base risk per trade
    try:
        return float(os.getenv("DAILY_MAX_DD_RS", ""))
    except Exception:
        return 3.0 * float(RISK_RS_DEFAULT)

def _paper_nudge_points(sym: str):
    # Per-symbol optional “paper nudge” (price points), default 0 = off
    # Example env: PAPER_NUDGE_TATAMOTORS=1.2  PAPER_NUDGE_LT=-1.2
    key = f"PAPER_NUDGE_{sym.upper()}"
    try:
        return float(os.getenv(key, "0"))
    except Exception:
        return 0.0

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
WEBUI = os.path.join(ROOT, "webui")

# ---------- Static file routes ----------
async def terminal_page(request): return web.FileResponse(os.path.join(WEBUI, "terminal.html"))
async def static_terminal_css(request): return web.FileResponse(os.path.join(WEBUI, "terminal.css"))
async def static_terminal_js(request): return web.FileResponse(os.path.join(WEBUI, "terminal.js"))
async def login_page(request): return web.FileResponse(os.path.join(WEBUI, "login.html"))
async def login_js(request): return web.FileResponse(os.path.join(WEBUI, "login.js"))

# ---------- Config & session flags ----------
GLOBAL = {
    "risk_rs": RISK_RS_DEFAULT,
    "entry_mode": ENTRY_MODE,
    "kill": False,
    # new runtime stats/limits
    "pnl_day": 0.0,
    "dd_limit": _dd_limit_rs(),
}

# ---------- WebSocket ----------
async def ws_ticks(request):
    ws = web.WebSocketResponse(heartbeat=15.0)
    await ws.prepare(request)
    request.app["clients"].add(ws)
    try:
        async for msg in ws:
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            if isinstance(data, dict) and data.get("type") == "cfg":
                if "risk" in data:
                    try: GLOBAL["risk_rs"] = float(data["risk"])
                    except Exception: pass
                if "entry_mode" in data and str(data["entry_mode"]).upper() in ("5THBAR","6TO10"):
                    GLOBAL["entry_mode"] = str(data["entry_mode"]).upper()
                await ws.send_str(json.dumps([{"type":"ack","risk":GLOBAL["risk_rs"],"entry_mode":GLOBAL["entry_mode"]}]))
            elif isinstance(data, dict) and data.get("type") == "cmd" and data.get("cmd") == "KILL":
                GLOBAL["kill"] = True
                await ws.send_str(json.dumps([{"type":"ack","kill":"armed"}]))
    finally:
        request.app["clients"].discard(ws)
    return ws

# ---- helpers ----
from datetime import datetime, time as dtime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
TRD_START = dtime(9,15)
TRD_DECISION = dtime(9,40)
TRD_END = dtime(15,30)

def _ist_now():
    return datetime.now(IST)

def _in_trading_window():
    t = _ist_now().time()
    return (t >= TRD_START) and (t <= TRD_END)
    
def _safe_prev_day_ohlc(df_all: pd.DataFrame, end_ts: float):
    if df_all is None or df_all.empty:
        return None
    df = df_all.copy()

    # ✅ Ensure DateTime exists (derive from end_ts/start_ts if missing)
    if "DateTime" not in df.columns:
        if "end_ts" in df.columns:
            df["DateTime"] = pd.to_datetime(df["end_ts"], unit="s", errors="coerce")
        elif "start_ts" in df.columns:
            df["DateTime"] = pd.to_datetime(df["start_ts"], unit="s", errors="coerce")
        else:
            return None  # no time columns to derive from

    # ✅ Ensure dtype is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["DateTime"]):
        df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")

    # Use only rows strictly before current bar end
    df = df[df["DateTime"] < pd.to_datetime(end_ts, unit="s")]
    if df.empty:
        return None
    
    # Day split
    if "Date" not in df.columns:
        df["Date"] = df["DateTime"].dt.normalize()
    
    # ✅ exclude today's date; we want strictly the previous trading day
    today_norm = pd.to_datetime(end_ts, unit="s").normalize()
    df = df[df["Date"] < today_norm]
    if df.empty:
        return None


    dates = sorted(df["Date"].dropna().unique())
    if not dates:
        return None

    dprev = dates[-1]
    ddf = df[df["Date"] == dprev].sort_values("DateTime")
    if ddf.empty:
        return None

    o = float(ddf.iloc[0]["Open"])
    h = float(ddf["High"].max())
    l = float(ddf["Low"].min())
    c = float(ddf.iloc[-1]["Close"])
    return (o, h, l, c)


def _compute_pdc(prev_ohlc):
    if not prev_ohlc: return "TR"
    o,h,l,c = prev_ohlc
    if o <= 0: return "TR"
    ret = 100.0*(c-o)/o
    if ret >= 0.6: return "BULL"
    if ret <= -0.6: return "BEAR"
    return "TR"

def _compute_ol(today_open: float, prev_ohlc):
    if today_open is None or not prev_ohlc: return "OIM"
    _, ph, pl, _ = prev_ohlc
    rng = max(1e-6, ph - pl); near = 0.2 * rng
    if today_open > ph: return "OAR"
    if today_open < pl: return "OBR"
    if abs(today_open - ph) <= near: return "OOH"
    if abs(today_open - pl) <= near: return "OOL"
    return "OIM"

def _compute_ot(bars_1_to_5):
    if len(bars_1_to_5) < 5: return "-"
    o = float(bars_1_to_5[0]["Open"]); c = float(bars_1_to_5[-1]["Close"])
    if o <= 0: return "TR"
    thr = 0.1 if BAR_SECONDS < 300 else 0.6
    ret = 100.0*(c-o)/o
    if ret >= thr: return "BULL"
    if ret <= -thr: return "BEAR"
    return "TR"

def _eps_mul():
    if IS_PAPER: return 0.0
    return max(0.0, ENTRY_EPS_BPS) / 10000.0

def get_today_df(symbol: str):
    df = read_tm5(symbol)
    if df is None or df.empty: return None
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    d0 = pd.Timestamp.now().normalize()
    return df[df["DateTime"].dt.normalize() == d0].copy()

async def tick_publisher(app: web.Application):
    aggs = { s: BarAggregator(s, bar_seconds=BAR_SECONDS) for s in SYMBOLS }
    state = {
        s: {
            "bars": [],
            "tags": {"PDC":"-", "OL":"-", "OT":"-"},
            "locked": {"PDC":False,"OL":False,"OT":False, "PLAN":False},
            "plan": {},
            "trade": {"status":"IDLE"},
            "entry_mode": GLOBAL["entry_mode"],
            "done_for_day": False,  # new: cap at one trade/day per symbol
        } for s in SYMBOLS
    }
    tick_stream = get_tick_stream(SYMBOLS)  # paper or live

    try:
        async for batch in tick_stream:
            # batch is list of (symbol, ts, price); may be [] in unconfigured live
            t = time.time()
            tick_payloads, bar_payloads, tag_payloads, plan_payloads, status_payloads = [], [], [], [], []

            for (sym, ts, price) in batch:
                # ensure usable timestamp for bar aggregation
                try:
                    ts = float(ts) if ts is not None else 0.0
                except Exception:
                    ts = 0.0
                if ts <= 0:
                    ts = time.time()
                st = state[sym]

                # DEV nudge in paper so we actually see triggers
                if IS_PAPER:
                    tr = st["trade"]; pl = st["plan"]
                    if pl and pl.get("Pick") in ("BULL","BEAR") and tr["status"] in ("READY","ARMED"):
                        if st["entry_mode"] == "5THBAR" and "bar5_high" in pl and "bar5_low" in pl:
                            target = pl["bar5_high"] if pl["Pick"] == "BULL" else pl["bar5_low"]
                            if pl["Pick"] == "BULL" and price < target: price = target + 0.05
                            elif pl["Pick"] == "BEAR" and price > target: price = target - 0.05
                        elif st["entry_mode"] == "6TO10" and st.get("next_break_adj") is not None:
                            target = float(st["next_break_adj"])
                            if pl["Pick"] == "BULL" and price < target: price = target + 0.05
                            elif pl["Pick"] == "BEAR" and price > target: price = target - 0.05

                # Optional paper nudge (config via env: PAPER_NUDGE_SYMBOL=+/-points). Default 0 = off
                if IS_PAPER and BAR_SECONDS < 300 and len(state[sym]["bars"]) < 5:
                    nud = _paper_nudge_points(sym)
                    if nud != 0:
                        price = price + nud
                
                price = round(max(0.01, price), 2)
                tick_payloads.append({"type": "tick", "symbol": sym, "ltp": price, "ts": ts})
                # --- trading window guard (LIVE only) ---
                out_of_window = (not IS_PAPER) and (not _in_trading_window())
                if out_of_window:
                    # we still aggregate bars & compute tags so UI stays alive; we only skip planning/entries
                    if st["trade"]["status"] in ("READY","ARMED"):
                        st["trade"]["status"] = "CLOSED"
                    st["skip_trading"] = True
                else:
                    st["skip_trading"] = False
                
                # once-per-day cap per symbol OR day killed (applies to both modes)
                if state[sym]["done_for_day"] or GLOBAL["kill"]:
                    # still publish ticks but skip planning/entries
                    continue

                # KILL handling
                if GLOBAL["kill"]:
                    tr = st["trade"]
                    if tr["status"] in ("READY","ARMED"):
                        tr["status"] = "CLOSED"
                        state[sym]["done_for_day"] = True
                        state[sym]["locked"]["PLAN"] = True
                        status_payloads.append({"type":"status","symbol":sym,"status":"KILLED","pnl":0})
                    elif tr["status"] == "LIVE":
                        qty = int(tr["qty"]); entry=float(tr["entry"]); long = (st["plan"].get("Pick")=="BULL")
                        pnl = qty * ((price - entry) if long else (entry - price))
                        pnl = float(round(pnl, 2))
                        tr["status"]="CLOSED"
                        state[sym]["done_for_day"] = True
                        state[sym]["locked"]["PLAN"] = True
                        GLOBAL["pnl_day"] = float(GLOBAL.get("pnl_day", 0.0)) + pnl
                        status_payloads.append({"type":"status","symbol":sym,"status":"KILLED","pnl":pnl})
                        append_trade({
                            "symbol": sym, "result": "KILLED", "entry": entry, "exit": price, "qty": qty,
                            "pick": st["plan"].get("Pick"), "sl": st["plan"].get("SL"),
                            "t1": st["plan"].get("T1"), "t2": st["plan"].get("T2"),
                            "pnl": pnl, "ts": ts
                        })
                    st["locked"]["PLAN"] = True

                # BAR aggregation
                closed = aggs[sym].on_tick(ts=ts, price=price)
                if closed:
                    tm5min_store.append_bar(sym, closed)
                    print(f"[bar] {sym} n={len(st['bars'])+1} end_ts={closed.get('end_ts')} ohlc=({closed['Open']},{closed['High']},{closed['Low']},{closed['Close']})")
                    bar_payloads.append({"type": "bar", "symbol": sym, **closed})
                    st["bars"].append(closed)
                    n = len(st["bars"])

                    # Tag locks (unified via classifier adapter)
                    try:
                        df_all = tm5min_store.read_all(sym)
                        prev   = _safe_prev_day_ohlc(df_all, closed["end_ts"])
                        bars5  = st["bars"][:min(5, n)]
                        today_open = float(st["bars"][0]["Open"]) if st["bars"] else None

                        # Build df for engine (needs prev-day present to compute PDC/OL/etc)
                        bars_df = pd.DataFrame(bars5)
                        if "DateTime" not in bars_df.columns:
                            if "end_ts" in bars_df.columns:
                                bars_df["DateTime"] = pd.to_datetime(bars_df["end_ts"], unit="s")
                            elif "start_ts" in bars_df.columns:
                                bars_df["DateTime"] = pd.to_datetime(bars_df["start_ts"], unit="s")
                            else:
                                # last-resort monotonic timestamps so engine doesn’t crash
                                base = pd.Timestamp.now().normalize()
                                bars_df["DateTime"] = base + pd.to_timedelta(range(len(bars_df)), unit="m")


                        # prepend previous trading day bars from store (if available)
                        df_all = tm5min_store.read_all(sym)
                        if df_all is not None and not df_all.empty and "end_ts" in df_all.columns:
                            df_all = df_all.copy()
                            df_all["DateTime"] = pd.to_datetime(df_all["end_ts"], unit="s")
                            today_norm = pd.to_datetime(bars_df["DateTime"].iloc[-1]).normalize()
                            alld = sorted(df_all["DateTime"].dt.normalize().unique())
                            # find prev trading day present in df_all
                            prev_norm = None
                            for i in range(1, 8):
                                cand = today_norm - pd.Timedelta(days=i)
                                if cand in alld:
                                    prev_norm = cand
                                    break
                            if prev_norm is not None:
                                prev_df = df_all[df_all["DateTime"].dt.normalize().eq(prev_norm)][["DateTime","Open","High","Low","Close"]]
                                # concat prev day + today-partial
                                combo = pd.concat([prev_df, bars_df[["DateTime","Open","High","Low","Close"]]], ignore_index=True)
                                tags_all = compute_tags_5(combo)
                            else:
                                tags_all = compute_tags_5(bars_df)
                        else:
                            tags_all = compute_tags_5(bars_df)
                        
                        # normalize keys so downstream code is stable
                        def _norm_tags(d):
                            if not isinstance(d, dict): return {}
                            m = {
                                "PDC_R":"PDC", "PDC":"PDC",
                                "OT_R":"OT",   "OT":"OT",
                                "OL":"OL",
                                "FIRST_CANDLE":"FirstCandleType",
                                "FirstCandleType":"FirstCandleType",
                                "RANGE_STATUS":"RangeStatus",
                                "RangeStatus":"RangeStatus",
                            }
                            out = {}
                            for k, v in d.items():
                                kk = m.get(k)
                                if kk: out[kk] = v
                            return out
                        
                        tags_all = _norm_tags(tags_all)

                        # fallback: if engine returned nothing, derive tags from simple rules
                        if not tags_all or (not tags_all.get("PDC") and not tags_all.get("OL") and not tags_all.get("OT")):
                            tags_all = {
                                "PDC": _compute_pdc(prev),
                                "OL":  _compute_ol(today_open, prev),
                                "OT":  _compute_ot(bars5),
                            }

                        # lock PDC at ≥3, OL at ≥4, OT at ≥5 (only publish the three used for trading/UI)
                        pushed = False
                        if n >= 3 and not st["locked"]["PDC"]:
                            st["tags"]["PDC"] = tags_all.get("PDC", "TR")
                            st["locked"]["PDC"] = True
                            pushed = True
                        if n >= 4 and not st["locked"]["OL"]:
                            st["tags"]["OL"]  = tags_all.get("OL", "OIM")
                            st["locked"]["OL"] = True
                            pushed = True
                        if n >= 5 and not st["locked"]["OT"]:
                            st["tags"]["OT"]  = tags_all.get("OT", "TR")
                            st["locked"]["OT"] = True
                            pushed = True

                        if pushed:
                            tag_payloads.append({"type":"tags","symbol":sym, **st["tags"]})
                    except Exception as e:
                        print(f"[tags][{sym}] error: {e}")
                        # --- simple fallback so tags still lock & UI stays alive ---
                        try:
                            prev = _safe_prev_day_ohlc(tm5min_store.read_all(sym), closed["end_ts"])
                            bars5  = st["bars"][:min(5, len(st["bars"]))]
                            today_open = float(st["bars"][0]["Open"]) if st["bars"] else None
                            tags_all = {
                                "PDC": _compute_pdc(prev),
                                "OL":  _compute_ol(today_open, prev),
                                "OT":  _compute_ot(bars5),
                            }
                            pushed = False
                            if len(st["bars"]) >= 3 and not st["locked"]["PDC"]:
                                st["tags"]["PDC"] = tags_all.get("PDC","TR")
                                st["locked"]["PDC"] = True
                                pushed = True
                            if len(st["bars"]) >= 4 and not st["locked"]["OL"]:
                                st["tags"]["OL"] = tags_all.get("OL","OIM")
                                st["locked"]["OL"] = True
                                pushed = True
                            if len(st["bars"]) >= 5 and not st["locked"]["OT"]:
                                st["tags"]["OT"] = tags_all.get("OT","TR")
                                st["locked"]["OT"] = True
                                pushed = True
                            if pushed:
                                tag_payloads.append({"type":"tags","symbol":sym, **st["tags"]})
                        except Exception as e2:
                            print(f"[tags-fallback][{sym}] error: {e2}")

                    # Plan after OT lock (not before 09:39:40 IST)
                    if (not GLOBAL["kill"]
                    and not state[sym]["done_for_day"]
                    and st["locked"]["OT"]
                    and not st["locked"]["PLAN"]
                    and not st.get("skip_trading", False)
                    and (IS_PAPER or _ist_now().time() >= dtime(9,39,40))):
                        ot = st["tags"]["OT"]
                        if ot in ("BULL","BEAR"):
                            pick = ot; conf = 60
                            orb_h, orb_l, _ = orb_from_bars(st["bars"][:5])
                            entry_ref = float(st["bars"][4]["Close"])
                            df_all = tm5min_store.read_all(sym)
                            prev = _safe_prev_day_ohlc(df_all, closed["end_ts"])
                            prev_h = prev[1] if prev else None; prev_l = prev[2] if prev else None
                            stop, rps, t1_raw, t2_raw = sl_targets_from_rules(ot, pick, entry_ref, orb_h, orb_l, prev_h, prev_l)
                            # Always ensure 1:2 RR targets regardless of levels module defaults
                            t1, t2 = ensure_targets(entry_ref, stop, t1_raw, t2_raw, rr=2.0)
                            qty = qty_from_risk(GLOBAL["risk_rs"], entry_ref, stop)
                            
                            bar5_high = float(st["bars"][4]["High"]); bar5_low = float(st["bars"][4]["Low"])
                            eps = _eps_mul()
                            bar5_high_adj = bar5_high * (1.0 + eps)
                            bar5_low_adj  = bar5_low  * (1.0 - eps)
                            trigger_adj = bar5_high_adj if pick=="BULL" else bar5_low_adj
                            
                            st["plan"] = {
                                "Pick": pick, "Conf": conf, "Entry": round(entry_ref,2),
                                "SL": round(float(stop),2) if stop==stop else None,
                                "T1": t1,
                                "T2": t2,
                                "Qty": int(qty),
                                "bar5_high": bar5_high, "bar5_low": bar5_low,
                                "bar5_high_adj": bar5_high_adj, "bar5_low_adj": bar5_low_adj,
                                "Trigger": round(trigger_adj,2)
                            }
                            st["locked"]["PLAN"] = True
                            st["entry_mode"] = GLOBAL["entry_mode"]
                            st["trade"] = {"status":"READY", "side": ("LONG" if pick=="BULL" else "SHORT")}
                            plan_payloads.append({
                                "type":"plan","symbol":sym,
                                "Pick": st["plan"]["Pick"], "Conf": st["plan"]["Conf"],
                                "Entry": st["plan"]["Entry"], "SL": st["plan"]["SL"],
                                "T1": st["plan"]["T1"], "T2": st["plan"]["T2"],
                                "Qty": st["plan"]["Qty"], "Trigger": st["plan"]["Trigger"]
                            })
                            status_payloads.append({"type":"status","symbol":sym,"status":"READY","pnl":0})
                        else:
                            st["plan"] = {"Pick":"ABSTAIN","Conf":0,"Entry":None,"SL":None,"T1":None,"T2":None,"Qty":0}
                            st["locked"]["PLAN"] = True
                            st["trade"] = {"status":"CLOSED"}
                            plan_payloads.append({"type":"plan","symbol":sym, **st["plan"]})
                            status_payloads.append({"type":"status","symbol":sym,"status":"ABSTAINED","pnl":0})

                    # 6→10: arm next bar & set adjusted break
                    tr = st["trade"]
                    if (st["entry_mode"] == "6TO10"
                    and not st.get("skip_trading", False)
                    and tr["status"] in ("READY","ARMED")
                    and 6 <= len(st["bars"]) <= 10):
                        k = len(st["bars"])
                        prev_bar = st["bars"][k-1]
                        eps = _eps_mul()
                        raw_brk = float(prev_bar["High"] if st["plan"]["Pick"]=="BULL" else prev_bar["Low"])
                        adj_brk = raw_brk * (1.0 + eps) if st["plan"]["Pick"]=="BULL" else raw_brk * (1.0 - eps)
                        st["next_break"] = raw_brk
                        st["next_break_adj"] = adj_brk
                        st["armed_for_bar"] = k + 1
                        tr["status"] = "ARMED"
                        status_payloads.append({"type":"status","symbol":sym,"status":"ARMED","pnl":0,"trigger":round(adj_brk,2)})

                    if st["entry_mode"] == "6TO10" and len(st["bars"]) >= 10 and st["trade"]["status"] in ("READY","ARMED"):
                        st["trade"]["status"] = "CLOSED"
                        st["done_for_day"] = True
                        st["locked"]["PLAN"] = True
                        status_payloads.append({"type":"status","symbol":sym,"status":"NO-TRIGGER (6–10)","pnl":0})

                # ---- tick-time trade engine ----
                st = state[sym]; tr = st["trade"]; pl = st["plan"]
                if (tr["status"] == "READY"
                and not st.get("skip_trading", False)
                and pl and pl.get("Pick") in ("BULL","BEAR")
                and st["entry_mode"] == "5THBAR"):
                    eps = _eps_mul()
                    crossed_5 = (
                        (pl["Pick"] == "BULL" and price >= st["plan"]["bar5_high"] * (1.0 + eps)) or
                        (pl["Pick"] == "BEAR" and price <= st["plan"]["bar5_low"]  * (1.0 - eps))
                    )
                    if crossed_5:
                        tr["status"] = "LIVE"
                        tr["entry_at"] = ts; tr["entry_bar"] = len(state[sym]["bars"])  # may be last-closed bar index
                        status_payloads.append({"type":"status","symbol":sym,"status":"LIVE","pnl":0,
                                                "entry_at": tr["entry_at"], "entry_bar": tr["entry_bar"]})
                        tr["entry"] = price
                        tr["qty"]   = pl["Qty"]
                        tr["stop"]  = pl["SL"]
                        tr["t1"]    = pl["T1"]
                        tr["t2"]    = pl["T2"]

                elif (tr["status"] == "ARMED"
                      and not st.get("skip_trading", False)
                      and pl and pl.get("Pick") in ("BULL","BEAR")
                      and st["entry_mode"] == "6TO10"):
                    brk = st.get("next_break_adj")
                    crossed = (
                        (pl["Pick"] == "BULL" and brk is not None and price >= brk) or
                        (pl["Pick"] == "BEAR" and brk is not None and price <= brk)
                    )
                    if crossed:
                        tr["status"] = "LIVE"
                        tr["entry_at"] = ts; tr["entry_bar"] = len(state[sym]["bars"])  # may be last-closed bar index
                        status_payloads.append({"type":"status","symbol":sym,"status":"LIVE","pnl":0,
                                                "entry_at": tr["entry_at"], "entry_bar": tr["entry_bar"]})
                        tr["entry"] = price
                        tr["qty"]   = pl["Qty"]
                        tr["stop"]  = pl["SL"]
                        tr["t1"]    = pl["T1"]
                        tr["t2"]    = pl["T2"]

                elif tr["status"] == "LIVE":
                    qty = int(tr["qty"]); entry = float(tr["entry"])
                    stop = float(tr["stop"]); t1 = float(tr["t1"]); t2 = float(tr["t2"])
                    long = (pl["Pick"] == "BULL")
                    mtm = qty * ((price - entry) if long else (entry - price))
                    status_payloads.append({"type":"status","symbol":sym,"status":"LIVE","pnl":round(mtm,2)})

                    hit_stop = (price <= stop) if long else (price >= stop)
                    hit_t2   = (price >= t2)   if long else (price <= t2)
                    hit_t1   = (price >= t1)   if long else (price <= t1)

                    result = None; exit_px = None
                    if hit_t2:      result, exit_px = "T2",   t2
                    elif hit_stop:  result, exit_px = "STOP", stop
                    elif hit_t1:    result, exit_px = "T1",   t1

                    if result:
                        tr["status"] = "CLOSED"
                        pnl = qty * ((exit_px - entry) if long else (entry - exit_px))
                        pnl = float(round(pnl, 2))
                        # update day PnL and enforce kill if beyond drawdown
                        GLOBAL["pnl_day"] = float(GLOBAL.get("pnl_day", 0.0)) + pnl
                        dd_hit = (GLOBAL["pnl_day"] <= -abs(GLOBAL.get("dd_limit", _dd_limit_rs())))
                        if dd_hit:
                            GLOBAL["kill"] = True
                        # mark this symbol done for the day on any resolution
                        state[sym]["done_for_day"] = True
                        state[sym]["locked"]["PLAN"] = True
                        tr["exit_at"] = ts
                        tr["exit_reason"] = result
                        status_payloads.append({
                            "type":"status","symbol":sym,"status":result,"pnl":pnl,
                            "exit_at": tr["exit_at"], "exit_reason": tr["exit_reason"]
                        })

                        append_trade({
                            "symbol": sym, "result": result, "entry": entry, "exit": exit_px, "qty": qty,
                            "pick": pl.get("Pick"), "sl": pl.get("SL"), "t1": pl.get("T1"), "t2": pl.get("T2"),
                            "pnl": pnl, "ts": ts
                        })

            # fan out to clients
            if app["clients"]:
                async def fan(msg):
                    await asyncio.gather(*[c.send_str(msg) for c in list(app["clients"]) if not c.closed], return_exceptions=True)

                if tick_payloads:   await fan(json.dumps(tick_payloads))
                if bar_payloads:    await fan(json.dumps(bar_payloads))
                if tag_payloads:    await fan(json.dumps(tag_payloads))
                if plan_payloads:   await fan(json.dumps(plan_payloads))
                if status_payloads: await fan(json.dumps(status_payloads))

            # persist live state snapshot (lightweight)
            try:
                snapshot = {
                    "risk_rs": GLOBAL["risk_rs"],
                    "entry_mode": GLOBAL["entry_mode"],
                    "kill": GLOBAL["kill"],
                    "symbols": {
                        s: {
                            "tags": state[s]["tags"],
                            "locked": state[s]["locked"],
                            "plan": {k:v for k,v in state[s]["plan"].items() if k not in ("bar5_high","bar5_low","bar5_high_adj","bar5_low_adj")},
                            "trade": state[s]["trade"],
                            "bars": state[s]["bars"][-5:],  # last 5 bars
                        } for s in SYMBOLS
                    },
                    "ts": time.time()
                }
                save_state(snapshot)
            except Exception:
                pass

    except asyncio.CancelledError:
        return

async def on_startup(app: web.Application):
    app["clients"] = set()
    app["master_headers"] = { s: read_master_headers(s) for s in SYMBOLS }
    app["pub_task"] = asyncio.create_task(tick_publisher(app))

async def on_cleanup(app: web.Application):
    task = app.get("pub_task")
    if task:
        task.cancel()
        with contextlib.suppress(Exception):
            await task

async def api_recompute_master(request):
    from ops.recompute_master_tags import run as recompute_run
    syms = request.query.get("symbols")
    symbols = [s.strip().upper() for s in syms.split(",")] if syms else SYMBOLS
    try:
        recompute_run(symbols)
        return web.json_response({"ok": True, "symbols": symbols})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def api_state(request):
    from storage.live_state import load_state
    try:
        return web.json_response(load_state() or {})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

# --- health & config ---
async def api_health(request):
    mode_now = "paper" if os.getenv("MODE","paper").lower()=="paper" else "live"
    return web.json_response({
        "ok": True,
        "mode": mode_now,
        "bar_seconds": BAR_SECONDS,
        "pnl_day": round(float(GLOBAL.get("pnl_day", 0.0)), 2),
        "dd_limit": float(GLOBAL.get("dd_limit", _dd_limit_rs())),
        "kill": bool(GLOBAL.get("kill", False)),
    })

async def api_get_config(request):
    return web.json_response({"risk_rs": GLOBAL["risk_rs"], "entry_mode": GLOBAL["entry_mode"]})

async def api_set_config(request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if "risk_rs" in body:
        try: GLOBAL["risk_rs"] = float(body["risk_rs"])
        except Exception: pass
    if "entry_mode" in body and str(body["entry_mode"]).upper() in ("5THBAR","6TO10"):
        GLOBAL["entry_mode"] = str(body["entry_mode"]).upper()
    return web.json_response({"ok": True, "risk_rs": GLOBAL["risk_rs"], "entry_mode": GLOBAL["entry_mode"]})

def make_app():
    app = web.Application()
    app["clients"] = set()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/ui/terminal.html", terminal_page)
    app.router.add_get("/ui/login.html", login_page)
    app.router.add_get("/webui/terminal.css", static_terminal_css)
    app.router.add_get("/webui/terminal.js", static_terminal_js)
    app.router.add_get("/webui/login.js", login_js)
    app.router.add_get("/ws/ticks", ws_ticks)
    app.router.add_get("/api/master/recompute", api_recompute_master)
    app.router.add_get("/api/journal", api_journal)
    app.router.add_get("/api/state", api_state)
    app.router.add_get("/api/health", api_health)
    app.router.add_get("/api/config", api_get_config)
    app.router.add_post("/api/config", api_set_config)
    return app

async def api_journal(request):
    from storage.journal_store import load_journal
    try:
        df = load_journal()
        rows = df.tail(200).to_dict(orient="records")
        return web.json_response({"rows": rows})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "9002"))
    web.run_app(make_app(), host="127.0.0.1", port=port)
