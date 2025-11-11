# fees.py
from __future__ import annotations
import math
import yaml
import pandas as pd


def load_fee_rules(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _round2(x):
    return float(round(x, 2))


def estimate_costs_for_row(row: pd.Series, rules: dict) -> dict:
    """
    Input (row): must have at least:
      side: "BUY"/"SELL"
      qty: float
      price: float
      segment: e.g. "EQ" or "NSE"
      series: e.g. "MIS" / "CNC" / "NRML"
    Returns dict with cost components and total_cost (positive number).
    """
    qty = float(row.get("qty", 0) or 0)
    price = float(row.get("price", 0) or 0)
    side = str(row.get("side", "")).upper()
    series = str(row.get("series", "")).upper()
    seg = str(row.get("segment", "")).upper()

    turnover = qty * price

    is_delivery = series == "CNC"
    is_intraday = (series in ("MIS", "BO", "CO")) or (not is_delivery)
    is_fo = seg in ("NFO", "FO")

    # --- Brokerage
    brokerage = 0.0
    if is_fo:
        brokerage = rules.get("brokerage_fo_flat", 20.0)  # per executed order
    else:
        if is_delivery:
            pct = rules.get("brokerage_delivery_pct", 0.0)
            cap = rules.get("brokerage_delivery_cap", 0.0)
        else:
            pct = rules.get("brokerage_intraday_pct", 0.0003)
            cap = rules.get("brokerage_intraday_cap", 20.0)
        brokerage = min(turnover * pct, cap)

    # --- Exchange transaction charges (on turnover; both sides)
    if is_fo:
        # crude split: index vs stock if symbol hints; if unknown, use stock rate
        sym = str(row.get("symbol_std", ""))
        pct = rules.get("txn_fo_stock_pct", 0.0002)
        if any(k in sym for k in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
            pct = rules.get("txn_fo_index_pct", 0.000053)
    else:
        pct = rules.get("txn_eq_intraday_pct", 0.00032)
        if is_delivery:
            pct = rules.get("txn_eq_delivery_pct", pct)
    ex_txn = turnover * pct

    # --- STT (side dependent)
    stt = 0.0
    if is_fo:
        if side == "SELL":
            stt += turnover * rules.get("stt_fo_sell_pct", 0.0000625)
    else:
        if is_delivery:
            if side == "BUY":
                stt += turnover * rules.get("stt_eq_delivery_pct_buy", 0.001)
            elif side == "SELL":
                stt += turnover * rules.get("stt_eq_delivery_pct_sell", 0.001)
        else:
            if side == "SELL":
                stt += turnover * rules.get("stt_eq_intraday_pct", 0.00025)

    # --- SEBI charges (on turnover both sides)
    sebi = (turnover / 1e7) * rules.get("sebi_charges_per_crore", 10.0)  # 1 crore = 1e7

    # --- Stamp duty (buy side only)
    stamp = 0.0
    if side == "BUY":
        if is_fo:
            stamp = turnover * rules.get("stamp_fo_pct", 0.000003)
        else:
            if is_delivery:
                stamp = turnover * rules.get("stamp_eq_delivery_pct", 0.00015)
            else:
                stamp = turnover * rules.get("stamp_eq_intraday_pct", 0.00003)

    # --- GST on (brokerage + exchange txn + sebi) only
    gst_base = brokerage + ex_txn + sebi
    gst = gst_base * rules.get("gst_pct", 0.18)

    total = brokerage + ex_txn + stt + sebi + stamp + gst
    return dict(
        brokerage=_round2(brokerage),
        ex_txn=_round2(ex_txn),
        stt=_round2(stt),
        sebi=_round2(sebi),
        stamp=_round2(stamp),
        gst=_round2(gst),
        total_cost=_round2(total),
    )


def estimate_day_costs(trades_df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """Return day-level costs & net P&L estimates."""
    if trades_df is None or trades_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "symbol_std",
                "gross_pnl",
                "cost_total",
                "dp_charges",
                "pnl_net",
            ]
        )
    t = trades_df.copy()
    # Assume parse_tradebook already normalized these names:
    # columns: trade_date, symbol_std, side, qty, price, segment, series
    cost_rows = []
    for idx, r in t.iterrows():
        comp = estimate_costs_for_row(r, rules)
        cost_rows.append(comp)
    cost = pd.DataFrame(cost_rows).fillna(0.0)
    t = pd.concat([t.reset_index(drop=True), cost], axis=1)

    # Gross P&L at day+symbol level = SELL - BUY
    t["signed_val"] = (t["qty"] * t["price"]) * t["side"].map({"SELL": 1, "BUY": -1})
    gross = (
        t.groupby(["trade_date", "symbol_std"], dropna=False)["signed_val"]
        .sum()
        .reset_index(name="gross_pnl")
    )

    # Sum costs per day+symbol
    fee_cols = ["brokerage", "ex_txn", "stt", "sebi", "stamp", "gst", "total_cost"]
    fees = (
        t.groupby(["trade_date", "symbol_std"], dropna=False)[fee_cols]
        .sum()
        .reset_index()
    )
    out = gross.merge(fees, on=["trade_date", "symbol_std"], how="left")

    # DP charges (apply once per day when there is at least one SELL of delivery)
    dp_per_day = float(rules.get("dp_charge_per_sale_day", 0.0))
    if dp_per_day > 0:
        sold_delivery = (
            t[(t["side"] == "SELL") & (t["series"].astype(str).str.upper() == "CNC")]
            .groupby(["trade_date", "symbol_std"])
            .size()
            .reset_index(name="n")
        )
        sold_delivery["dp_charges"] = dp_per_day
        out = out.merge(
            sold_delivery[["trade_date", "symbol_std", "dp_charges"]],
            on=["trade_date", "symbol_std"],
            how="left",
        )
        out["dp_charges"] = out["dp_charges"].fillna(0.0)
    else:
        out["dp_charges"] = 0.0

    out["cost_total"] = out["total_cost"].fillna(0.0) + out["dp_charges"].fillna(0.0)
    out["pnl_net"] = out["gross_pnl"].fillna(0.0) - out["cost_total"].fillna(0.0)
    return out
