from .idempotency import ensure_once
def place_limit(sym, side, qty, price, client_order_id):
    if not ensure_once(client_order_id): return {"status":"duplicate","client_order_id":client_order_id}
    return {"status":"accepted","order_id":client_order_id}
