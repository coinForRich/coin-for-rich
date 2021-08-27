from fastapi import APIRouter
from web.routes.api.rest import (
    ohlcvs as ohlcvs_rest, symexch, test as test_rest, analytics
)
from web.routes.api.ws import ohlcvs as ohlcvs_ws, test as test_ws

api_router = APIRouter()
api_router.include_router(ohlcvs_rest.router, tags=["api_ohlcvs_rest"])
api_router.include_router(symexch.router, tags=["api_symexch"])
api_router.include_router(test_rest.router, tags=["api_test_rest"])
api_router.include_router(ohlcvs_ws.router, tags=["api_ohlcvs_ws"])
api_router.include_router(analytics.router, tags=["api_analytics"])
api_router.include_router(test_ws.router, tags=["api_test_ws"])
