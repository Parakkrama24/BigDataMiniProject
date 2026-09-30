import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.repository import MemoryRepository
from api.routers import alerts, health, vitals


def _default_repository():
    """Pick a repository backend from the environment.

    Defaults to the in-memory one so `create_app()` works with no database
    running (tests rely on that). Set API_REPOSITORY=postgres -- as
    docker-compose does -- to serve the rows the Spark streaming job and the
    Airflow batch job actually write to Postgres.
    """
    backend = os.getenv("API_REPOSITORY", "memory").strip().lower()
    if backend == "postgres":
        from api.postgres_repository import PostgresRepository

        return PostgresRepository()
    if backend != "memory":
        raise ValueError(f"Unknown API_REPOSITORY={backend!r}; expected 'memory' or 'postgres'")
    return MemoryRepository()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    yield
    # Only the Postgres repository holds a connection pool to release; without
    # this its worker threads outlive the process and psycopg_pool complains.
    close = getattr(app.state.repository, "close", None)
    if callable(close):
        close()


def create_app(repository=None) -> FastAPI:
    app = FastAPI(title="Hospital Vital Signs API", version="1.0.0", lifespan=_lifespan)
    app.state.repository = repository if repository is not None else _default_repository()
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
