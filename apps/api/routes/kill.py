from fastapi import APIRouter
router = APIRouter(prefix="/api", tags=["ops"])
@router.post("/kill")
def kill(): return {"killed": True}
