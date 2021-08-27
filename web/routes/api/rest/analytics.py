# Backend REST API endpoint for analytics views

from typing import Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter(prefix="/analytics")

@router.get("/top500dr", name="get_top500dr")
async def get_top500dr(db: Session = Depends(get_db)):
    return readers.read_top500dr(db)
