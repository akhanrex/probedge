class DayController:
    def __init__(self, day: str, mode: str = "sim-paper", daily_risk_rs: int = 10000):
        # day = "2025-08-01"
        # mode = sim-paper (fake-live) / live-paper / live-kite
        # risk = from config or UI
        ...
        

    def preload_data(self):
        # Load TM5 for all 10 symbols for this day (same reader as backtest)
        # Build tags + plan once using existing plan_core logic
        # For sim mode we can prebuild everything before loop
        ...

    def build_plan(self):
        # For this day:
        # - call your existing plan engine
        # - apply risk split (daily_risk_rs / active symbols)
        # - store plan per symbol in memory
        ...

    def simulate_trades_offline(self):
        # Use the SAME logic used in paper backtest (exec_adapter + paper_exec_from_journal),
        # but capture trade-level timings in memory instead of only CSV.
        # Output: schedule per symbol: entry_time, exit_time, side, qty, pnl, etc.
        ...

    def run_playback_loop(self, speed: float = 10.0):
        # Iterates over TM5 bars (09:15..15:10) in chronological order.
        # For each bar:
        #  - update live_state.json with LTP/ohlc/volume per symbol + sim_clock
        #  - check in-memory schedule:
        #       * if we cross an entry_time, mark position open
        #       * if we cross exit_time (SL/TP/EOD), close position, update P&L
        #  - write positions + P&L to live_state.json
        #  - sleep wall-clock based on 'speed' factor (5min -> 30sec for speed=10)
        ...
