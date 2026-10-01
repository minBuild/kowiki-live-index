import time
from typing import Annotated

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from kowiki import metrics
from kowiki.embedder import Embedder, make_embedder
from kowiki.errors import TransientError
from kowiki.index import QdrantIndex
from kowiki.settings import Settings, get_settings


def create_app(
    settings: Settings | None = None, embedder: Embedder | None = None, index: QdrantIndex | None = None
) -> FastAPI:
    settings = settings or get_settings()
    embedder = embedder or make_embedder(settings)
    index = index or QdrantIndex.from_settings(settings)

    app = FastAPI(title="kowiki-live-index")
    app.mount("/metrics", make_asgi_app())

    @app.exception_handler(TransientError)
    async def unavailable(_: Request, exc: TransientError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=503)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/search")
    def search(
        q: Annotated[str, Query(min_length=1, max_length=500)],
        k: Annotated[int, Query(ge=1, le=50)] = 5,
    ) -> dict:
        started = time.perf_counter()
        vector = embedder.embed([settings.embed_query_prefix + q])[0]
        hits = index.search(vector, k)
        metrics.SEARCH_SECONDS.observe(time.perf_counter() - started)
        return {"query": q, "hits": [{"score": round(h.score, 4), **(h.payload or {})} for h in hits]}

    return app
