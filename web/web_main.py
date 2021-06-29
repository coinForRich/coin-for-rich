import asyncio
import redis
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from common.config.constants import REDIS_HOST, REDIS_PASSWORD
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web.utils.asyncioutils import WSBroadcaster


app = FastAPI()
app.mount("/scripts", StaticFiles(directory="web/scripts"), name="scripts")
templates = Jinja2Templates(directory="web/templates")
redis_client = redis.Redis(
    host=REDIS_HOST,
    username="default",
    password=REDIS_PASSWORD,
    decode_responses=True
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/testws", response_class=HTMLResponse)
async def testws(request: Request):
    return templates.TemplateResponse("testws.html", {"request": request})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    broadcaster = WSBroadcaster()
    while True:
        input = await websocket.receive_text()
        if input:
            try:
                exchange, symbol = input.split(";")
                broadcaster.broadcast_from_redis_list(
                    websocket,
                    redis_client,
                    WS_SERVE_REDIS_KEY.format(
                        exchange = exchange,
                        symbol = symbol)
                )
            except Exception as exc:
                print(f'EXCEPTION: {exc}')