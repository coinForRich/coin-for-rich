# Simple path definitions for web views

from fastapi import Request, APIRouter
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from web.routes.api.rest import ohlcvs

views_router = APIRouter()
templates = Jinja2Templates(directory="web/templates")

@views_router.get(
    "/wschart", name="views_wschart", response_class=HTMLResponse)
async def wschart(
        request: Request,
        exchange: str,
        base_id: str,
        quote_id: str
    ):
    return templates.TemplateResponse(
        "viewsymbol.html", {
            "request": request,
            "exchange": exchange,
            "base_id": base_id,
            "quote_id": quote_id
        }
    )

@views_router.get(
    "/analytics/general",
    name="views_analytics",
    response_class=HTMLResponse
)
async def analytics_general(request: Request):
    return templates.TemplateResponse(
        "analytics.html", {
            "request": request
        }
    )
