from pydantic import BaseModel
class Plan(BaseModel):
    symbol: str
    side: str
    entry: float
    stop: float
    tp: float
    qty: int
    rr: float
    rationale: str
def compute_dummy_plan(symbol: str) -> Plan:
    return Plan(symbol=symbol, side="BUY", entry=100, stop=99, tp=102, qty=1, rr=2.0, rationale="scaffold")
