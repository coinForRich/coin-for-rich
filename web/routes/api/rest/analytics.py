# Backend REST API endpoint for analytics views

from typing import Union, Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter(prefix="/analytics")

@router.get("/geodr", name="get_geodr")
async def get_geodr(
        db: Session = Depends(get_db),
        cutoff_upper_pct: Optional[Union[int, None]] = 10000,
        cutoff_lower_pct: Optional[Union[int, None]] = 0,
        limit: int = 500
    ):
    '''
    Gets geometric daily return

    :params:
        `limit`: limit number of rows
        `cutoff_upper_pct`: upper cutoff for extreme values
        `cutoff_lower_pct`: lower cutoff for extreme values
    
    If limit == -1, returns all symbols

    Default limit of 500 rows
    '''
    
    return readers.read_geodr(db, cutoff_upper_pct, cutoff_lower_pct, limit)

@router.get("/top20qvlm", name="get_top20qvlm")
async def get_top20qvlm(db: Session = Depends(get_db)):
    return readers.read_top20qvlm(db)

@router.get("/wr", name="get_wr")
async def get_wr(
        db: Session = Depends(get_db),
        cutoff_upper_pct: Optional[Union[int, None]] = 10000,
        cutoff_lower_pct: Optional[Union[int, None]] = 0,
        limit: int = 500
    ):
    '''
    Gets weekly return

    :params:
        `limit`: limit number of rows
        `cutoff_upper_pct`: upper cutoff for extreme values
        `cutoff_lower_pct`: lower cutoff for extreme values

    If `limit` == -1, returns all symbols
    
    Default limit of 500 rows
    '''

    return readers.read_wr(db, cutoff_upper_pct, cutoff_lower_pct, limit)
