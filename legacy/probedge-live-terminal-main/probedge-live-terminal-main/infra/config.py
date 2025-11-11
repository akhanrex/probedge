from dotenv import load_dotenv
import os

load_dotenv()

MODE = os.getenv("MODE", "paper").lower()
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "TATAMOTORS,LT,SBIN").split(",") if s.strip()]
RISK_RS_DEFAULT = float(os.getenv("RISK_RS", "1000"))
ENTRY_MODE = os.getenv("ENTRY_MODE", "6TO10").upper()
# 5 bps = 0.05% breakout buffer; set to 0.0 for live
ENTRY_EPS_BPS = float(os.getenv("ENTRY_EPS_BPS", "5"))  # basis points
SLIPPAGE_CAP_PCT = float(os.getenv("SLIPPAGE_CAP_PCT", "0.0005"))
FILL_TIMEOUT_MS = int(os.getenv("FILL_TIMEOUT_MS", "500"))
DATA_DIR = os.getenv("DATA_DIR", "./data")

# Paper-friendly bar duration: 10s default in paper, 300s (5 min) in live
BAR_SECONDS = int(os.getenv("BAR_SECONDS", "10" if MODE == "paper" else "300"))

KITE = {
    "API_KEY": os.getenv("KITE_API_KEY", ""),
    "API_SECRET": os.getenv("KITE_API_SECRET", ""),
    "ACCESS_TOKEN": os.getenv("KITE_ACCESS_TOKEN", ""),
}
