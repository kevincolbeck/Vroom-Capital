"""
Legion Bot — FastAPI Application Entry Point
"""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from loguru import logger
import os

from backend.database import init_db
from backend.api.routes import router
from backend.backtest.routes import router as backtest_router
from backend.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    logger.info("Starting Legion Bot API...")
    await init_db()
    logger.info("Database initialized")

    # Auto-start bot if configured
    if settings.bot_enabled and settings.bitunix_api_key:
        from backend.bot_engine import get_bot_engine
        engine = get_bot_engine()
        asyncio.create_task(engine.start())
        logger.info("Bot engine auto-started")

    yield

    logger.info("Shutting down Legion Bot API...")
    from backend.bot_engine import get_bot_engine
    engine = get_bot_engine()
    if engine.is_running:
        await engine.stop()


app = FastAPI(
    title="Legion Bot API",
    description="Autonomous Bitcoin Futures Trading Bot with Copy Trading",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend dev server and production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(router, prefix="/api/v1")
app.include_router(backtest_router, prefix="/api/v1")

# Serve frontend static files (production build)
frontend_dist = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dist, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info",
    )
