from __future__ import annotations
import asyncio, json, os
from aiohttp import web, WSMsgType
from infra.logger import setup_logger
from infra import config
from infra.state import AppState
from orders.oms import OMS
from decision.manager import DecisionManager

log = setup_logger("ws.server")

routes = web.RouteTableDef()
clients = set()

app_state = AppState(
    risk_rs=config.RISK_RS_DEFAULT,
    entry_mode=config.ENTRY_MODE,
    mode=config.MODE,
    symbols={},
)
oms = OMS()
dm = DecisionManager(app_state, oms)

@routes.get("/api/health")
async def health(_):
    return web.json_response({"ok": True, "mode": config.MODE})

@routes.get("/api/state")
async def api_state(_):
    payload = {
        "risk_rs": app_state.risk_rs,
        "entry_mode": app_state.entry_mode,
        "mode": app_state.mode,
        "symbols": {k: v.__dict__ for k, v in app_state.symbols.items()},
    }
    return web.json_response(payload)

@routes.post("/api/config")
async def api_config(req):
    body = await req.json()
    app_state.risk_rs = int(body.get("risk_rs", app_state.risk_rs))
    app_state.entry_mode = str(body.get("entry_mode", app_state.entry_mode))
    return web.json_response({"ok": True})

@routes.get("/ws/ticks")
async def ws_ticks(req):
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(req)
    clients.add(ws)
    log.info("client connected (%d total)", len(clients))
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    continue
                sym = payload.get("symbol")
                ltp = payload.get("ltp")
                bar_ctx = payload.get("ctx", {})
                if sym and ltp is not None:
                    state = dm.on_tick(sym, float(ltp), bar_ctx)
                    if state:
                        await broadcast({"type": "state", "symbol": sym, "state": state})
            elif msg.type == WSMsgType.ERROR:
                log.error("ws error: %s", ws.exception())
    finally:
        clients.discard(ws)
        log.info("client disconnected (%d total)", len(clients))
    return ws

async def broadcast(obj):
    if not clients:
        return
    data = json.dumps(obj, ensure_ascii=False)
    await asyncio.gather(*(c.send_str(data) for c in list(clients) if not c.closed), return_exceptions=True)

def make_app() -> web.Application:
    app = web.Application()
    app.add_routes(routes)
    here = os.path.dirname(__file__)
    webui = os.path.join(os.path.dirname(here), "webui")
    app.router.add_static("/ui/", webui, show_index=True)
    return app

def main():
    app = make_app()
    web.run_app(app, host=config.HOST, port=config.PORT)

if __name__ == "__main__":
    main()
