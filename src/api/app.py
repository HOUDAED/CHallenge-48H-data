"""
FastAPI application factory.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .router import router
from .store import store


@asynccontextmanager
async def lifespan(app: FastAPI):
    store.load()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Air Quality & Meteo API",
        description=(
            "Combined LCSQA air quality and SYNOP meteorological data "
            "for France's major cities. Hourly resolution."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router, prefix="/api/v1")
    return app


app = create_app()
