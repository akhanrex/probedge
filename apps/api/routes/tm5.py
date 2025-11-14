from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from probedge.storage.resolver import locate_for_read

router = APIRouter()

@router.get("/api/tm5")
def get_tm5(symbol: str = Query(..., alias="symbol")):
    path = locate_for_read("intraday", symbol)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"TM5 not found for {symbol}")
    df = pd.read_csv(path)

    # Make JSON-safe: replace NaN/NaT with None
    df = df.where(pd.notnull(df), None)

    return {
        "symbol": symbol.upper(),
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns],
        "data": df.to_dict(orient="records"),
    }
