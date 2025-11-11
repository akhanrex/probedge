from __future__ import annotations
import os

# Mode
MODE = os.getenv("MODE", "paper").lower()  # 'paper' | 'live'

# Symbols (comma-separated)
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "TATAMOTORS,LT,SBIN").split(",") if s.strip()]

# Risk config
RISK_RS_DEFAULT = int(os.getenv("RISK_RS_DEFAULT", "1000"))
SLIPPAGE_CAP_PCT = float(os.getenv("SLIPPAGE_CAP_PCT", "0.0005"))  # 5 bps
FILL_TIMEOUT_MS = int(os.getenv("FILL_TIMEOUT_MS", "500"))

# Entry mode
ENTRY_MODE = os.getenv("ENTRY_MODE", "5TH_BAR").upper()  # '5TH_BAR' | '6TO10_PREV'

# Bars aggregation
BAR_SECONDS = 300 if MODE == "live" else 10

# Web
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "9002"))

# Data layout
DATA_DIR = os.getenv("DATA_DIR", "./data")
MASTER_PATH = os.getenv("MASTER_PATH", os.path.join(DATA_DIR, "master.csv"))
JOURNAL_DIR = os.getenv("JOURNAL_DIR", os.path.join(DATA_DIR, "journal"))

# Broker (placeholders for live wiring)
KITE_API_KEY = os.getenv("KITE_API_KEY", "")
KITE_API_SECRET = os.getenv("KITE_API_SECRET", "")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")

# Picker gates (locked)
PICKER_MIN_L3 = int(os.getenv("PICKER_MIN_L3", "8"))
PICKER_MIN_L2 = int(os.getenv("PICKER_MIN_L2", "6"))
PICKER_MIN_L1 = int(os.getenv("PICKER_MIN_L1", "4"))
PICKER_MIN_L0 = int(os.getenv("PICKER_MIN_L0", "3"))
PICKER_CONF_MIN = int(os.getenv("PICKER_CONF_MIN", "55"))
PICKER_REQUIRE_OT_ALIGN = os.getenv("PICKER_REQUIRE_OT_ALIGN", "true").lower() == "true"
