# probedge/core/rules.py

# Shared tag constants
DEFAULT_TAG_COLS = [
    "PrevDayContext",
    "OpenLocation",
    "FirstCandleType",
    "OpeningTrend",
    "RangeStatus",
]
NA_SENTINEL = "∅"


def _canon_tag_value(label: str, value):
    if value is None:
        return "UNKNOWN"
    v = str(value).strip().upper()
    L = (label or "").strip().upper()
    if L == "PREVDAYCONTEXT":
        if "BULL" in v:
            return "BULL"
        if "BEAR" in v:
            return "BEAR"
        if v in {"TR", "RANGE", "TRADINGRANGE"}:
            return "TR"
        return "UNKNOWN"
    if L == "OPENLOCATION":
        return v if v in {"OOL", "OOH", "OAR", "OBR", "OIM"} else "UNKNOWN"
    if L == "FIRSTCANDLETYPE":
        if v in {"HUGE OPEN", "NORMAL", "DOJI"}:
            return v
        if v in {"UP", "DOWN"}:
            return v
        return "UNKNOWN"
    if L == "OPENINGTREND":
        if v in {"BULL", "BEAR", "TR"}:
            return v
        if v == "TRENDUP":
            return "BULL"
        if v == "TRENDDOWN":
            return "BEAR"
        if v == "FLAT":
            return "TR"
        return "UNKNOWN"
    if L == "RANGESTATUS":
        if v in {"SAR", "WAR", "SBR", "WBR", "SWR"}:
            return v
        if v in {"BROKEUP", "BROKEDOWN", "INSIDE"}:
            return v
        return "UNKNOWN"
    return "UNKNOWN"


def _tag_color_map(label: str, value: str) -> str:
    v = (value or "UNKNOWN").strip().upper()
    maps = {
        "OPENLOCATION": {
            "OOL": "#2563eb",
            "OOH": "#f59e0b",
            "OAR": "#8b5cf6",
            "OBR": "#ef4444",
            "OIM": "#64748b",
            "UNKNOWN": "#94a3b8",
        },
        "FIRSTCANDLETYPE": {
            "HUGE OPEN": "#8b5cf6",
            "NORMAL": "#16a34a",
            "DOJI": "#6b7280",
            "UP": "#16a34a",
            "DOWN": "#ef4444",
            "UNKNOWN": "#94a3b8",
        },
        "OPENINGTREND": {
            "BULL": "#16a34a",
            "BEAR": "#ef4444",
            "TR": "#64748b",
            "UNKNOWN": "#94a3b8",
        },
        "RANGESTATUS": {
            "SAR": "#16a34a",
            "WAR": "#16a34a",
            "SBR": "#ef4444",
            "WBR": "#ef4444",
            "SWR": "#6b7280",
            "BROKEUP": "#16a34a",
            "BROKEDOWN": "#ef4444",
            "INSIDE": "#6b7280",
            "UNKNOWN": "#94a3b8",
        },
        "PREVDAYCONTEXT": {
            "BULL": "#16a34a",
            "BEAR": "#ef4444",
            "TR": "#64748b",
            "UNKNOWN": "#94a3b8",
        },
    }
    return maps.get(label.upper(), {}).get(v, "#94a3b8")


def _tag_icon(label: str, value: str) -> str:
    v = (value or "UNKNOWN").strip().upper()
    L = label.upper()
    if L == "PREVDAYCONTEXT":
        return (
            "🐂"
            if v == "BULL"
            else ("🐻" if v == "BEAR" else "〰️" if v == "TR" else "❔")
        )
    if L == "OPENLOCATION":
        return "📍"
    if L == "FIRSTCANDLETYPE":
        return "🕯️"
    if L == "OPENINGTREND":
        return "📈" if v == "BULL" else ("📉" if v == "BEAR" else "➖")
    if L == "RANGESTATUS":
        if v in {"SAR", "WAR", "BROKEUP"}:
            return "🔓"
        if v in {"SBR", "WBR", "BROKEDOWN"}:
            return "🔻"
        if v in {"SWR", "INSIDE"}:
            return "🧱"
        return "❔"
    return "🏷️"


def _pretty_value(label: str, raw_val: str) -> str:
    if not raw_val or raw_val.upper() == "UNKNOWN":
        return "—"
    L = label.upper()
    V = raw_val.upper()
    if L == "OPENINGTREND":
        return {"BULL": "BULL", "BEAR": "BEAR", "TR": "TR"}.get(V, V)
    if L == "RANGESTATUS":
        return V
    if L == "FIRSTCANDLETYPE":
        return {
            "HUGE OPEN": "HUGE OPEN",
            "NORMAL": "NORMAL",
            "DOJI": "DOJI",
            "UP": "UP",
            "DOWN": "DOWN",
        }.get(V, V)
    if L == "PREVDAYCONTEXT":
        return {"BULL": "BULL", "BEAR": "BEAR", "TR": "TR"}.get(V, V)
    if L == "OPENLOCATION":
        return V
    return V
