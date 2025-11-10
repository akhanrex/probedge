from ..infra.settings import SETTINGS
def ticks_stream(symbols: list[str]):
    if SETTINGS.mode.lower() == "paper":
        from .playback import playback_stream
        return playback_stream(symbols)
    else:
        from .kite_live import live_tick_stream
        return live_tick_stream(symbols)
