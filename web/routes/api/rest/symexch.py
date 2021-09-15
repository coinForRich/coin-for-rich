# Backend API endpoint for OHLCV

from typing import Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter()

@router.get("/symbol-exchange", name="get_symbol_exchange")
async def get_symbol_exchange(
        db: Session = Depends(get_db)
    ):
    '''
    API to read symbol-exchanges from database
    '''

    return readers.read_symbol_exchange(db)
