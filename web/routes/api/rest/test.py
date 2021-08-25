# Backend API endpoint for Test table

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter()

@router.get("/test", name="read_test")
async def read_test(db: Session = Depends(get_db)):
    return readers.read_test(db)
