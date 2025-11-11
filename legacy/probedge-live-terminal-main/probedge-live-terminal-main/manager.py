from __future__ import annotations
from typing import Dict, Any, Optional
from dataclasses import asdict
from infra import config
from infra.state import AppState, SymbolState, PlanState
from infra.clock_ist import T_0925, T_0930, T_093950, T_0940, T_1505, is_after_ist
from decision.tags_engine import compute_tags_5
from decision.picker import pick_with_gates
from decision.levels import orb_from_first5, sl_targets_from_rules
from decision.risk_plan import qty_from_risk, ensure_targets
from orders.oms import OMS

class DecisionManager:
    def __init__(self, app_state: AppState, oms: OMS) -> None:
        self.app = app_state
        self.oms = oms

    def ensure_symbol(self, sym: str) -> SymbolState:
        if sym not in self.app.symbols:
            self.app.symbols[sym] = SymbolState(symbol=sym)
        return self.app.symbols[sym]

    def on_tick(self, sym: str, ltp: float, bar_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        s = self.ensure_symbol(sym)
        s.ltp = float(ltp)

        # Hard time locks for tags
        if is_after_ist(T_0925) and not s.tags.locked_pdc:
            tags = compute_tags_5(bar_context)
            s.tags.pdc = tags.PDC
            s.tags.locked_pdc = True

        if is_after_ist(T_0930) and not s.tags.locked_ol:
            tags = compute_tags_5(bar_context)
            s.tags.ol = tags.OL
            s.tags.locked_ol = True

        if is_after_ist(T_093950) and not s.tags.locked_ot:
            tags = compute_tags_5(bar_context)
            s.tags.ot = tags.OT
            s.tags.first_candle_type = tags.FirstCandleType
            s.tags.range_status = tags.RANGE_STATUS
            s.tags.locked_ot = True

            # Picker once OT is locked
            freq = bar_context.get("freq", {})  # frequency tables prepared from master
            direction, conf, lvl = pick_with_gates(freq, s.tags.ot)
            if direction == "NONE":
                s.plan.status = "ABSTAINED"
            else:
                s.plan.direction = direction
                s.plan.confidence = conf
                s.plan.level = lvl

        # Arm plan at 09:40
        if is_after_ist(T_0940) and s.tags.locked_ot and s.plan.status in ("IDLE", "ABSTAINED"):
            bar5_high = bar_context.get("bar5_high")
            bar5_low = bar_context.get("bar5_low")
            if s.plan.direction in ("BULL", "BEAR") and bar5_high and bar5_low:
                entry = bar5_high if s.plan.direction == "BULL" else bar5_low
                orb_h, orb_l = orb_from_first5(bar5_high, bar5_low)
                stop, t1, t2, rps = sl_targets_from_rules(s.plan.direction, entry, orb_h, orb_l)
                qty = qty_from_risk(self.app.risk_rs, rps)
                entry, stop, t1, t2 = ensure_targets(entry, stop, t1, t2)
                s.plan.entry_ref = entry
                s.plan.trigger = entry
                s.plan.stop, s.plan.t1, s.plan.t2 = stop, t1, t2
                s.plan.qty = qty
                s.plan.mode = self.app.entry_mode
                if qty > 0:
                    s.plan.status = "ARMED"
                else:
                    s.plan.status = "ABSTAINED"

        # Entry trigger detection & OMS
        if s.plan.status == "ARMED" and s.plan.trigger:
            trig = s.plan.trigger
            crossed = (ltp >= trig) if s.plan.direction == "BULL" else (ltp <= trig)
            if crossed:
                self.oms.place_entry(sym, s.plan.direction, trig, s.plan.qty)
                s.plan.status = "ORDER_SENT"

        # OMS updates
        status = self.oms.sync(sym, ltp, s.plan)
        if status:
            s.plan.status = status

        # EOD flatten
        if is_after_ist(T_1505) and s.plan.status in ("LIVE", "ORDER_SENT", "ARMED"):
            self.oms.force_exit(sym, ltp, s.plan)
            s.plan.status = "FLAT"

        return asdict(s)
