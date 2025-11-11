import os
from .playback import playback_tick_stream

SYMBOLS_DEFAULT = ["TATAMOTORS", "LT", "SBIN"]

def get_tick_stream(symbols=None):
    symbols = symbols or SYMBOLS_DEFAULT
    mode = os.getenv("MODE", "paper").lower()
    bar_seconds = int(os.getenv("BAR_SECONDS", "300"))
    if mode == "paper":
        date_str = os.getenv("PAPER_DATE", "2025-10-10")
        speed = float(os.getenv("PAPER_SPEED", "3.0"))
        return playback_tick_stream(symbols, date_str=date_str, bar_seconds=bar_seconds, speed=speed)
    else:
        try:
            from .kite_live import live_tick_stream
            return live_tick_stream(symbols)
        except Exception:
            async def _noop():
                while True:
                    yield []
            return _noop()
