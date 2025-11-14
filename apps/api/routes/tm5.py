from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from probedge.storage.resolver import locate_for_read
from ._jsonsafe import json_safe_df

router = APIRouter()

@router.get("/api/tm5")
def get_tm5(symbol: str = Query(..., alias="symbol")):
    path = locate_for_read("intraday", symbol)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"TM5 not found for {symbol}")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read CSV: {e}")

    df = json_safe_df(df)

    return {
        "symbol": symbol.upper(),
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns],
        "data": df.to_dict(orient="records"),
    }
