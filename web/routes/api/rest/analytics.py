# Backend REST API endpoint for analytics views

from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter(prefix="/analytics")

@router.get("/top500dr", name="get_top500dr")
async def get_top500dr(db: Session = Depends(get_db)):
    return readers.read_top500dr(db)

@router.get("/top10vlmb", name="get_top10vlmb")
async def get_top10vlmb(db: Session = Depends(get_db)):
    return readers.read_top10vlmb(db)
