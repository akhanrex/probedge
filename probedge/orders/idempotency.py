_seen=set()
def ensure_once(client_order_id: str)->bool:
    if client_order_id in _seen: return False
    _seen.add(client_order_id); return True
