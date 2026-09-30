from fastapi import FastAPI

from api.repository import MemoryRepository
from api.routers import alerts, health, vitals


def create_app(repository=None) -> FastAPI:
    app = FastAPI(title="Hospital Vital Signs API", version="1.0.0")
    app.state.repository = repository or MemoryRepository()
    app.include_router(vitals.router)
    app.include_router(alerts.router)
    app.include_router(health.router)
    try:
        from api.routers.reports import router as reports_router
    except ImportError:
        reports_router = None
    if reports_router is not None:
        app.include_router(reports_router)
    return app


app = create_app()
