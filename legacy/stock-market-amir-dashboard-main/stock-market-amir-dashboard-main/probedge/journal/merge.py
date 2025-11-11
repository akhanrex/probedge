# journal_utils.py — tiny helpers for ProbEdge Journal (Excel-only P&L)
# Pulls config from journal_config.yaml (paths, sheet names, UI bits).
# Source of truth for config keys comes from the uploaded YAML.  [oai_citation:0‡journal_config.yaml](file-service://file-GA29PY8Z3qkTX7wGosKsFz)

from __future__ import annotations
import io
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Config loader (YAML, optional)
# -----------------------------
_CFG: Dict[str, object] = {}


def _load_yaml(path: str | Path) -> Dict[str, object]:
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    try:
        p = Path(path)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def cfg(key: str, default=None):
    global _CFG
    if not _CFG:
        # fallbacks baked from journal_config.yaml if file isn’t available
        _CFG = {
            "RISK_UNIT": 10000,
            "MASTER_DEFAULT_PATH": "./TataMotors_Master.csv",
            "ZERODHA_PNL_SHEETS": {
                "equity": "Equity",
                "other_debits_credits": "Other Debits and Credits",
            },
            "TIMEZONE": "Asia/Kolkata",
        }
        y = _load_yaml("./journal_config.yaml")
        if y:
            _CFG.update(y)
    cur = _CFG
    for part in key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


# --------------------------------------
# Master loader (structure + tag columns)
# --------------------------------------
def load_master(path: str | Path) -> pd.DataFrame:
    path = str(path)
    usecols = [
        "Date",
        "PrevDayContext",
        "GapType",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
        "Result",
    ]
    try:
        master = pd.read_csv(path, usecols=usecols, low_memory=False)
    except Exception:
        return pd.DataFrame(columns=usecols + ["symbol_std"])
    master["Date"] = pd.to_datetime(master["Date"], errors="coerce").dt.date
    for c in [
        "PrevDayContext",
        "GapType",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
        "Result",
    ]:
        if c in master:
            master[c] = master[c].astype(str).str.strip().str.upper()
    master["symbol_std"] = "TATAMOTORS"
    return master


# -------------------------------------------------------
# Zerodha P&L workbook → daily NET P&L by symbol (Excel)
# -------------------------------------------------------
def parse_pnl_workbook(
    file_like_or_path,
    equity_sheet: Optional[str] = None,
    odc_sheet: Optional[str] = None,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Returns:
      (pnl_daily_df, charges_df_or_None)

    pnl_daily_df columns: ['trade_date','symbol_std','pnl_net']
    We use ONLY this net number everywhere. If the workbook/sheet missing, returns (empty_df, None).
    """
    name = (
        getattr(file_like_or_path, "name", "").lower()
        if hasattr(file_like_or_path, "name")
        else str(file_like_or_path)
    )
    equity_sheet = equity_sheet or str(cfg("ZERODHA_PNL_SHEETS.equity", "Equity"))
    odc_sheet = odc_sheet or str(
        cfg("ZERODHA_PNL_SHEETS.other_debits_credits", "Other Debits and Credits")
    )

    try:
        import openpyxl  # noqa: F401
    except Exception:
        # Without openpyxl we can’t parse xlsx reliably; return empty sentinel
        return (pd.DataFrame(columns=["trade_date", "symbol_std", "pnl_net"]), None)

    try:
        xls = pd.ExcelFile(file_like_or_path, engine="openpyxl")
    except Exception:
        return (pd.DataFrame(columns=["trade_date", "symbol_std", "pnl_net"]), None)

    def _scan_df(name: str) -> Optional[pd.DataFrame]:
        if name not in xls.sheet_names:
            return None
        raw = xls.parse(name, header=None)
        # Find header row by presence of 'Date' and a Net-like column
        hdr_idx = None
        for i in range(min(120, len(raw))):
            row = raw.iloc[i].astype(str).str.strip().tolist()
            if any(v.lower() == "date" for v in row) and any(
                ("p&l" in v.lower()) or ("pnl" in v.lower()) or (v.lower() == "net")
                for v in row
            ):
                hdr_idx = i
                break
        if hdr_idx is None:
            return None
        body = raw.iloc[hdr_idx + 1 :].copy()
        body.columns = raw.iloc[hdr_idx].astype(str).str.strip().tolist()
        return body.dropna(how="all")

    eq_df = _scan_df(equity_sheet)
    pnl_daily = pd.DataFrame(columns=["trade_date", "symbol_std", "pnl_net"])
    if eq_df is not None and "Date" in eq_df.columns:
        sym_col = next(
            (
                c
                for c in [
                    "Trading symbol",
                    "Instrument",
                    "Symbol",
                    "Scrip",
                    "Scrip name",
                ]
                if c in eq_df.columns
            ),
            None,
        )
        net_col = next(
            (
                p
                for p in [
                    "Net realised P&L",
                    "Net P&L",
                    "Net P&L (₹)",
                    "Net P&L (Rs.)",
                    "Net",
                ]
                if p in eq_df.columns
            ),
            None,
        )
        # Some exports label negatives as (12,345.67). Convert safely.
        if net_col is not None:
            tmp = pd.DataFrame()
            tmp["trade_date"] = pd.to_datetime(
                eq_df["Date"], dayfirst=True, errors="coerce"
            ).dt.date
            tmp = tmp[~tmp["trade_date"].isna()]
            tmp["symbol_std"] = (
                eq_df[sym_col].astype(str).str.strip().str.upper()
                if sym_col
                else "TATAMOTORS"
            )
            # Canonicalize TATA MOTORS spellings
            rep = {
                "TATA MOTORS LTD": "TATAMOTORS",
                "TATA MOTORS LIMITED": "TATAMOTORS",
                "TATA MOTORS": "TATAMOTORS",
                "TATAMOTORS-EQ": "TATAMOTORS",
                "TATAMOTORS , EQ": "TATAMOTORS",
            }
            tmp["symbol_std"] = tmp["symbol_std"].replace(rep)
            tmp["pnl_net"] = pd.to_numeric(
                eq_df[net_col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("₹", "", regex=False)
                .str.replace(r"\(([^)]+)\)", r"-\1", regex=True)
                .str.replace("—", "0", regex=False)
                .str.strip(),
                errors="coerce",
            ).fillna(0.0)
            pnl_daily = (
                tmp.groupby(["trade_date", "symbol_std"], dropna=False)["pnl_net"]
                .sum()
                .reset_index()
            )

    charges_df = xls.parse(odc_sheet) if odc_sheet in xls.sheet_names else None
    return (pnl_daily, charges_df)


# -------------------------------------------
# Merge: MASTER (structure) × Excel NET P&L
# -------------------------------------------
def build_journal(
    master: pd.DataFrame, pnl_daily: pd.DataFrame, risk_unit: float
) -> pd.DataFrame:
    """
    Returns journal rows for days where we have Excel net P&L (no fee-model, no gross fallback).
    """
    if master is None or master.empty:
        return pd.DataFrame(
            columns=[
                "Date",
                "symbol_std",
                "pnl_net",
                "pnl_final",
                "day_R_net",
                "breach_daily_stop",
                "PrevDayContext",
                "GapType",
                "OpenLocation",
                "FirstCandleType",
                "OpeningTrend",
                "RangeStatus",
                "Result",
            ]
        )

    df = (
        pnl_daily.copy()
        if (pnl_daily is not None)
        else pd.DataFrame(columns=["trade_date", "symbol_std", "pnl_net"])
    )
    if df.empty:
        # No P&L → empty join (don’t fabricate with tradebook)
        return pd.DataFrame(
            columns=[
                "Date",
                "symbol_std",
                "pnl_net",
                "pnl_final",
                "day_R_net",
                "breach_daily_stop",
                "PrevDayContext",
                "GapType",
                "OpenLocation",
                "FirstCandleType",
                "OpeningTrend",
                "RangeStatus",
                "Result",
            ]
        )

    df = df.rename(columns={"trade_date": "Date"})
    # Ensure date dtype
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df = df.dropna(subset=["Date"])

    out = master.merge(df, on=["Date", "symbol_std"], how="inner")
    out["pnl_final"] = out["pnl_net"]  # Excel is the only source of truth
    ru = float(risk_unit or 1.0)
    out["day_R_net"] = out["pnl_final"] / (ru if ru != 0.0 else 1.0)
    out["breach_daily_stop"] = out["pnl_final"] < -ru
    return out
