from typing import Optional
from kiteconnect import KiteConnect
from probedge.infra.settings import SETTINGS
from .idempotency import next_client_order_id

class BrokerKite:
    def __init__(self):
        self.kc = KiteConnect(api_key=SETTINGS.kite_api_key)
        self.kc.set_access_token(SETTINGS.kite_access_token)

    def place_limit(self, symbol: str, side: str, qty: int, price: float, client_order_id: Optional[str]=None):
        side = side.upper()
        t = self.kc.TRANSACTION_TYPE_BUY if side == "BUY" else self.kc.TRANSACTION_TYPE_SELL
        if not client_order_id:
            client_order_id = next_client_order_id(SETTINGS.client_id_prefix, symbol)
        order_id = self.kc.place_order(
            variety=self.kc.VARIETY_REGULAR,
            exchange=self.kc.EXCHANGE_NSE,
            tradingsymbol=symbol.upper(),
            transaction_type=t,
            quantity=int(qty),
            order_type=self.kc.ORDER_TYPE_LIMIT,
            price=float(price),
            product=self.kc.PRODUCT_CNC,
            validity=self.kc.VALIDITY_DAY,
            tag=client_order_id
        )
        return order_id

    def modify_once_nudge(self, order_id: str, new_price: float):
        return self.kc.modify_order(
            variety=self.kc.VARIETY_REGULAR,
            order_id=order_id,
            price=float(new_price),
        )

    def cancel(self, order_id: str):
        return self.kc.cancel_order(variety=self.kc.VARIETY_REGULAR, order_id=order_id)
