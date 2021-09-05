# Backend REST API endpoint for analytics views

from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter(prefix="/analytics")

@router.get("/geodr", name="get_geodr")
async def get_geodr(limit: int = 500, db: Session = Depends(get_db)):
    '''
    If limit == -1, returns all symbols

    Default limit of 500 rows
    '''
    
    return readers.read_geodr(db, limit)

@router.get("/top10vlmb", name="get_top10vlmb")
async def get_top10vlmb(db: Session = Depends(get_db)):
    return readers.read_top10vlmb(db)

@router.get("/wr", name="get_wr")
async def get_wr(limit: int = 500, db: Session = Depends(get_db)):
    '''
    If limit == -1, returns all symbols
    
    Default limit of 500 rows
    '''

    return readers.read_wr(db, limit)
