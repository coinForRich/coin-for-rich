import redis
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from web.routes.api.ws.utils.connections import WSConnectionManager
from web.routes.api.api import api_router
from web.routes.views import views_router
# from web.db.base import metadata
# from web.db.session import engine

# metadata.create_all(bind=engine)
app = FastAPI(openapi_url="/api/openapi.json")
app.mount("/src", StaticFiles(directory="web/src"), name="src")
app.include_router(api_router, prefix="/api")
app.include_router(views_router, prefix="/view")
ws_manager = WSConnectionManager()





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
