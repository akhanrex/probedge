# infra/aliases.py
ALIAS = {
    # map any feed/ticker name to your “canonical” terminal name
    "TMPV": "TATAMOTORS",
    "TATA MOTORS": "TATAMOTORS",
    "TATAMOTORS": "TATAMOTORS",

    "LT": "LT",
    "SBIN": "SBIN",
}

def canonical(sym: str) -> str:
    return ALIAS.get(sym.upper().strip(), sym.upper().strip())
