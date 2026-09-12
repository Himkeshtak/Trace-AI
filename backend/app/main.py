from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware

from backend.app.analytics import (
    load_agent_metrics,
    load_memory_metrics,
    load_recent_executions,
    load_summary,
    load_token_metrics,
    load_tool_metrics,
)
from backend.app.config import get_settings
from backend.app.infrastructure import AnalyticsCache, AnalyticsStore, EventPublisher
from shared.traceai_contracts import TraceEvent


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    store = AnalyticsStore(settings.database_url)
    cache = AnalyticsCache(settings.redis_url, settings.analytics_cache_ttl_seconds)
    publisher = EventPublisher(settings)
    await store.open()
    await publisher.start()
    app.state.store = store
    app.state.cache = cache
    app.state.publisher = publisher
    yield
    await publisher.stop()
    await cache.close()
    await store.close()


settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description="Ingestion and analytics API for TraceAI execution telemetry.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.get("/health", tags=["operations"])
async def health(request: Request) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    for name, check in (
        ("postgres", request.app.state.store.ping),
        ("redis", request.app.state.cache.ping),
    ):
        try:
            checks[name] = await check()
        except Exception:
            checks[name] = False
    healthy = all(checks.values())
    return {"status": "ok" if healthy else "degraded", "checks": checks}


@app.post("/api/v1/events", status_code=status.HTTP_202_ACCEPTED, tags=["ingestion"])
async def ingest_event(event: TraceEvent, request: Request) -> dict[str, str]:
    try:
        await request.app.state.publisher.publish(event)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The event stream is temporarily unavailable.",
        ) from exc
    return {"event_id": str(event.event_id), "status": "accepted"}


async def cached_response(
    request: Request,
    cache_key: str,
    loader: Callable[[], Awaitable[Any]],
) -> dict[str, Any]:
    data, cached = await request.app.state.cache.get_or_load(cache_key, loader)
    return {"data": data, "meta": {"cached": cached}}


@app.get("/api/v1/analytics/summary", tags=["analytics"])
async def summary(request: Request) -> dict[str, Any]:
    return await cached_response(
        request, "analytics:summary", lambda: load_summary(request.app.state.store)
    )


@app.get("/api/v1/analytics/tools", tags=["analytics"])
async def tools(request: Request) -> dict[str, Any]:
    return await cached_response(
        request, "analytics:tools", lambda: load_tool_metrics(request.app.state.store)
    )


@app.get("/api/v1/analytics/agents", tags=["analytics"])
async def agents(request: Request) -> dict[str, Any]:
    return await cached_response(
        request, "analytics:agents", lambda: load_agent_metrics(request.app.state.store)
    )


@app.get("/api/v1/analytics/tokens", tags=["analytics"])
async def tokens(request: Request) -> dict[str, Any]:
    return await cached_response(
        request, "analytics:tokens", lambda: load_token_metrics(request.app.state.store)
    )


@app.get("/api/v1/analytics/memory", tags=["analytics"])
async def memory(request: Request) -> dict[str, Any]:
    return await cached_response(
        request, "analytics:memory", lambda: load_memory_metrics(request.app.state.store)
    )


@app.get("/api/v1/executions/recent", tags=["executions"])
async def recent_executions(
    request: Request,
    limit: int = Query(default=12, ge=1, le=100),
) -> dict[str, Any]:
    return await cached_response(
        request,
        f"analytics:executions:recent:{limit}",
        lambda: load_recent_executions(request.app.state.store, limit),
    )
