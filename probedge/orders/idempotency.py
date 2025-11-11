import time, threading
_lock = threading.Lock()
_seq = 0

def next_client_order_id(prefix: str, symbol: str) -> str:
    global _seq
    with _lock:
        _seq += 1
        s = _seq
    date = time.strftime("%Y%m%d")
    sym = symbol.replace("-", "").upper()
    return f"{prefix}-{date}-{sym}-{s:06d}"
