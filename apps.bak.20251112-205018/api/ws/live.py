from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from probedge.infra.settings import SETTINGS
from probedge.realtime.kite_live import live_tick_stream
import json

router = APIRouter()

@router.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    try:
        # Stream batches and forward minimal JSON: { t: <epoch>, ticks: [[SYM, LTP], ...] }
        async for batch in live_tick_stream(SETTINGS.symbols):
            if not batch:
                continue
            ts = batch[0][1]
            ticks = [[s, p] for (s, _, p) in batch]
            await ws.send_text(json.dumps({"t": ts, "ticks": ticks}))
    except WebSocketDisconnect:
        # client closed — normal
        pass
    except Exception as e:
        # any error — close gracefully
        try:
            await ws.close()
        except Exception:
            pass
