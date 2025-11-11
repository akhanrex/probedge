
from fastapi import APIRouter, HTTPException
from probedge.infra.settings import SETTINGS
import csv, os

router = APIRouter()

@router.get("/api/journal/daily")
def journal_daily():
    path = SETTINGS.paths.journal
    if not path:
        # Settings loaded but journal key absent → mis-config
        raise HTTPException(status_code=500, detail="journal path not set in settings")

    if not os.path.exists(path):
        # Helpful 404 with the path we looked at and CWD
        raise HTTPException(status_code=404, detail=f"Journal not found: {path} (cwd={os.getcwd()})")

    try:
        rows = []
        with open(path, newline="") as f:
            sniffer = csv.Sniffer()
            sample = f.read(2048)
            f.seek(0)
            dialect = sniffer.sniff(sample) if sample else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            for r in reader:
                rows.append(r)
        return {"source": path, "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"journal error: {e}")
