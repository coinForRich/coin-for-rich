import redis
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from starlette.websockets import WebSocketDisconnect
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, DEFAULT_DATETIME_STR_QUERY
)
from web.config.constants import WS_SERVE_EVENT_TYPES
from web.routes.api.ws.ohlcvs import WSServerSender
from web.routes.api.ws.utils.connections import WSServerConnectionManager
from web.routes.api.api import api_router
from web.routes.views import views_router
# from web.db.base import metadata
# from web.db.session import engine

# metadata.create_all(bind=engine)
app = FastAPI()
app.mount("/src", StaticFiles(directory="web/src"), name="src")
app.include_router(api_router, prefix="/api")
app.include_router(views_router, prefix="/view")
ws_manager = WSServerConnectionManager()
redis_client = redis.Redis(
    host=REDIS_HOST,
    username="default",
    password=REDIS_PASSWORD,
    decode_responses=True
)


# @app.websocket("/candles")
# async def websocket_endpoint(
#         websocket: WebSocket,
#         db: Session = Depends(get_db)
#     ):
#     await ws_manager.connect(websocket)
#     ws_sender = WSServerSender(ws_manager, websocket, redis_client, db)
#     try:
#         while True:
#             input = await websocket.receive_json()
#             if input:
#                 print(input)
#                 try:
#                     event_type = input['event_type']
#                     data_type = input['data_type']
#                     exchange = input['exchange']
#                     base_id = input['base_id']
#                     quote_id = input['quote_id']
#                     interval = input['interval']
#                     if event_type not in WS_SERVE_EVENT_TYPES:
#                         await websocket.send_json({
#                             'detail': "event_type must be subscribe or unsubscribe"
#                         })
#                     elif event_type == "subscribe":
#                         if data_type == "ohlc":
#                             ws_sender.serve_ohlc(
#                                 exchange, base_id, quote_id, interval
#                             )
#                     elif event_type == "unsubscribe":
#                         if data_type == "ohlc":
#                             ws_sender.unserve_ohlc(
#                                 exchange, base_id, quote_id, interval
#                             )
#                             await websocket.send_json({
#                             'detail': \
#                                 f"unsubscribed successfully from {exchange}_{base_id}_{quote_id}_{interval}"
#                         })
#                 except Exception as exc:
#                     print(f'EXCEPTION: {exc}')
#     except WebSocketDisconnect:
#         ws_manager.disconnect(websocket)

# @app.get("/testws", response_class=HTMLResponse)
# async def testws(
#         request: Request,
#         exchange: str,
#         base_id: str,
#         quote_id: str
#     ):
#     return templates.TemplateResponse(
#         "viewsymbol.html", {
#             "request": request,
#             "exchange": exchange,
#             "base_id": base_id,
#             "quote_id": quote_id
#         }
#     )

# @app.get("/analytics", response_class=HTMLResponse)
# async def analytics(request: Request):
#     return templates.TemplateResponse(
#         "analytics.html", {
#             "request": request
#         })
