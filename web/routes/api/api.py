from fastapi import APIRouter
from web.routes.api.rest import ohlcvs, symexch, test

api_router = APIRouter()
api_router.include_router(ohlcvs.router, tags=["api_ohlcvs"])
api_router.include_router(symexch.router, tags=["api_symexch"])
api_router.include_router(test.router, tags=["api_test"])
