#!/usr/bin/env python3
"""
main.py

Clean, single-app FastAPI bootstrap with:
 - Pydantic v2 settings with native .env support
 - Modern lifespan startup (always enabled)
 - Centralized logging with JSON support for production
 - Security middleware and observability
 - Package-qualified router imports (container friendly)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, cast
from contextvars import ContextVar
from pydantic import field_validator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.middleware.base import BaseHTTPMiddleware

from backend.services import env_utils

# Optional observability integration (Diamond standard)
try:
    from backend.services import observability_utils as obs  # type: ignore
    _OBS_AVAILABLE = True
except Exception:
    obs = None  # type: ignore
    _OBS_AVAILABLE = False

# ---------- Settings ----------
class Settings(BaseSettings):
    ENVIRONMENT: str = "dev"
    ALLOWED_ORIGINS: Optional[str] = None
    ALLOWED_HOSTS: Optional[str] = None
    ENABLE_RAW_ANNOUNCEMENTS: bool = False
    WARM_INDEX_ON_STARTUP: bool = False
    BACKEND_PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )

    @field_validator('BACKEND_PORT')
    @classmethod
    def validate_port(cls, v):
        if not 1024 <= v <= 65535:
            raise ValueError('BACKEND_PORT must be between 1024 and 65535')
        return v

    @field_validator('LOG_LEVEL')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'LOG_LEVEL must be one of {valid_levels}')
        return v.upper()
settings = Settings(ENVIRONMENT=env_utils.get_environment())

# ---------- Request Context ----------
# Context var used to propagate request id into log records (async-safe)
_request_id_ctx: ContextVar[str] = ContextVar("_request_id", default="unknown")

# ---------- Logging Setup ----------
class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for production logs."""
    def format(self, record):
        log_obj = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "unknown"),
        }
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


# ---------- Logging Filter ----------
class RequestIDFilter(logging.Filter):
    """Attach request id from contextvar to all log records."""
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.request_id = _request_id_ctx.get()
        except Exception:
            record.request_id = "unknown"
        return True

class CorrelationFilter(logging.Filter):
    """Ensure every record has a correlation_id (fallback to request_id or 'none')."""
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # prefer existing context request id if available
            val = _request_id_ctx.get("unknown")
        except Exception:
            val = "unknown"
        # set both request_id and correlation_id to satisfy formatters that expect either
        setattr(record, "request_id", getattr(record, "request_id", val))
        setattr(record, "correlation_id", getattr(record, "correlation_id", val))
        return True

def setup_logging(log_level: str, environment: str) -> logging.Logger:
    """Configure logging with JSON for production."""
    if _OBS_AVAILABLE and hasattr(obs, "get_logger"):
        try:
            logger = obs.get_logger("backend.main")  # type: ignore[attr-defined]
            logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            return logger
        except Exception:
            pass

    log_level_int = getattr(logging, log_level.upper(), logging.INFO)
    logger = logging.getLogger("backend.main")

    # ✅ Prevent duplicate handlers on reload or repeated imports
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        if environment.lower() == "prod":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter('[%(request_id)s] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )

        # ✅ Add filter so all records carry request_id
        handler.addFilter(RequestIDFilter())
        handler.addFilter(CorrelationFilter())

        logger.setLevel(log_level_int)
        logger.addHandler(handler)
        logger.propagate = False  # prevent duplicates in uvicorn logs

    return logger

logger = setup_logging(settings.LOG_LEVEL, settings.ENVIRONMENT)
logger.info("Settings loaded: ENVIRONMENT=%s", settings.ENVIRONMENT)


def _metric_inc(name: str, value: int = 1) -> None:
    try:
        if _OBS_AVAILABLE and hasattr(obs, "metrics_inc"):
            obs.metrics_inc(name, value)
        else:
            logger.debug("metric %s += %s (fallback)", name, value)
    except Exception:
        logger.debug("metric emit failed for %s", name, exc_info=True)


def _audit(event: str, detail: dict, status: str = "success") -> None:
    try:
        if _OBS_AVAILABLE and hasattr(obs, "audit_log"):
            obs.audit_log(event, "backend.main", status, detail)
        else:
            logger.info("AUDIT %s status=%s detail=%s", event, status, detail)
    except Exception:
        logger.debug("audit emit failed for %s", event, exc_info=True)

# ---------- Warm index helper ----------
async def _maybe_warm_index() -> None:
    """Run index warm-up in a threadpool if available and requested."""
    if not settings.WARM_INDEX_ON_STARTUP:
        logger.debug("WARM_INDEX_ON_STARTUP is false; skipping warm index")
        return
    _metric_inc("app.index_warm.requests")
    try:
        from backend.services import index_builder
        loop = asyncio.get_running_loop()
        if hasattr(index_builder, "refresh_index"):
            logger.info("Running index_builder.refresh_index in executor")
            await loop.run_in_executor(None, getattr(index_builder, "refresh_index"))
            _metric_inc("app.index_warm.success")
            _audit("app.index_warm", {"status": "completed"})
        else:
            logger.info("index_builder has no refresh_index; skipping")
            _metric_inc("app.index_warm.missing")
    except Exception as exc:
        logger.exception("Index warm-up failed (non-fatal)")
        _metric_inc("app.index_warm.failure")
        _audit("app.index_warm", {"error": str(exc)}, status="error")

# ---------- Metrics collector setup ----------
import threading
_metrics_installed = False
_metrics_lock = threading.Lock()

try:
    from prometheus_client import Counter, make_asgi_app
except Exception:
    Counter = None
    make_asgi_app = None
    logger.warning("prometheus_client not available; metrics disabled")

if Counter is not None:
    _csv_fallback_counter = Counter(
        "csv_processor_fallback_to_local_total",
        "Count of S3→local CSV fallback events"
    )
else:
    _csv_fallback_counter = None

def prometheus_collector(name: str, value: int):
    """Real Prometheus metrics integration."""
    try:
        if name == "csv_processor.fallback_to_local" and _csv_fallback_counter is not None:
            _csv_fallback_counter.inc(int(value))
    except Exception as exc:
        logger.warning("Metrics collection failed: %s", exc)

def setup_metrics():
    """Thread-safe, idempotent setup for CSV metrics collector."""
    global _metrics_installed
    with _metrics_lock:
        if _metrics_installed:
            logger.debug("Metrics already setup; skipping")
            return
        try:
            from backend.services import csv_processor  # type: ignore
            hook = getattr(csv_processor, "set_metrics_collector", None)
            if callable(hook):
                hook(prometheus_collector)
                _metrics_installed = True
                logger.info("Metrics collector set up")
                _metric_inc("app.metrics.setup.success")
                _audit("app.metrics.setup", {"status": "success"})
            else:
                logger.debug("csv_processor.set_metrics_collector not available; skipping metrics hook")
                _metric_inc("app.metrics.setup.missing")
        except Exception:
            logger.exception("Metrics setup failed (non-fatal)")
            _metric_inc("app.metrics.setup.failure")
            _audit("app.metrics.setup", {"status": "error"}, status="error")

# ---------- Request ID Middleware ----------
class RequestIDMiddleware(BaseHTTPMiddleware):
    """Add unique request ID to each request for tracing."""
    async def dispatch(self, request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        # ✅ Set request_id in context for logging and capture token
        token = _request_id_ctx.set(request_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            # ✅ Reset context to prevent leakage into background tasks
            _request_id_ctx.reset(token)

# ---------- Security Headers Middleware ----------
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-XSS-Protection", "1; mode=block")
        # HSTS only in prod (requires HTTPS)
        if settings.ENVIRONMENT.lower() == "prod":
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return response

# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modern FastAPI startup/shutdown handler."""
    # Startup
    await _maybe_warm_index()
    setup_metrics()
    
    # Validate CSV processor settings (fail fast in production)
    try:
        from backend.services.csv_processor import preload_settings, validate_settings
        preload_settings()
        validate_settings(allow_local_default=settings.ENVIRONMENT.lower() != "prod")
        logger.info("CSV processor settings validated")
        _metric_inc("app.csv_settings.validated")
        _audit("app.csv_settings", {"status": "validated"})
    except Exception as exc:
        logger.exception("CSV processor validation failed: %s", exc)
        _metric_inc("app.csv_settings.failure")
        _audit("app.csv_settings", {"error": str(exc)}, status="error")
        raise  # Fail fast - don't start if CSV processing is broken
    
    logger.info("Application startup complete")
    _metric_inc("app.startup.complete")
    _audit("app.startup", {"status": "complete"})
    yield
    # Shutdown
    try:
        from backend.services.csv_processor import shutdown_csv_executors
        shutdown_csv_executors()
        logger.info("CSV processor executors shut down")
        _metric_inc("app.csv_shutdown.success")
        _audit("app.csv_shutdown", {"status": "success"})
    except Exception as exc:
        logger.exception("CSV processor shutdown failed: %s", exc)
        _metric_inc("app.csv_shutdown.failure")
        _audit("app.csv_shutdown", {"error": str(exc)}, status="error")
    logger.info("Application shutting down")
    _metric_inc("app.shutdown.complete")

# ---------- App creation ----------
show_docs = settings.ENVIRONMENT.lower() != "prod"
app = FastAPI(
    title="NEXO Backend",
    version="0.0.1",
    docs_url="/docs" if show_docs else None,
    redoc_url="/redoc" if show_docs else None,
    openapi_url="/openapi.json" if show_docs else None,
    lifespan=lifespan,
)

# ---------- Security Middleware ----------
if settings.ENVIRONMENT.lower() == "prod" and settings.ALLOWED_HOSTS:
    allowed_hosts = [h.strip() for h in settings.ALLOWED_HOSTS.split(",")]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)
    logger.info("TrustedHostMiddleware enabled with hosts: %s", allowed_hosts)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(SecurityHeadersMiddleware)

# ---------- CORS ----------
if settings.ALLOWED_ORIGINS:
    origins = [o.strip() for o in settings.ALLOWED_ORIGINS.split(",") if o.strip()]
elif settings.ENVIRONMENT.lower() == "dev":
    origins = ["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000"]
else:
    origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
logger.debug("CORS origins: %s", origins)

# ---------- Routers ----------
health_router = None
announcements_router = None
raw_announcements_router = None

try:
    from backend.routers import health as health_router
    logger.debug("Imported backend.routers.health")
except Exception as exc:
    logger.exception("Could not import health router: %s", exc)
    health_router = None

try:
    from backend.routers import announcements as announcements_router
    logger.debug("Imported backend.routers.announcements")
except Exception as exc:
    logger.exception("Could not import announcements router: %s", exc)
    announcements_router = None

try:
    from backend.routers import raw_announcements as raw_announcements_router
    logger.debug("Imported backend.routers.raw_announcements")
except Exception:
    raw_announcements_router = None

if health_router is not None:
    app.include_router(health_router.router)
if announcements_router is not None:
    app.include_router(announcements_router.router)
if settings.ENABLE_RAW_ANNOUNCEMENTS and raw_announcements_router is not None:
    app.include_router(raw_announcements_router.router)

# ---------- Static files ----------
static_dir = Path(env_utils.build_local_path("backend/input_data"))
if static_dir.exists() and static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    logger.info("Mounted /static -> %s", static_dir)
else:
    logger.debug("Static dir %s not found; skipping mount", static_dir)

# ✅ Serve processed images dynamically (logos, banners) — keep /backend in path
processed_images_dir = Path(env_utils.build_local_path("backend/output_data/processed_images"))
if processed_images_dir.exists() and processed_images_dir.is_dir():
    app.mount(
        "/backend/output_data/processed_images",
        StaticFiles(directory=str(processed_images_dir)),
        name="processed_images_backend",
    )
    logger.info("Mounted /backend/output_data/processed_images -> %s", processed_images_dir)
else:
    logger.warning("Processed images dir not found: %s", processed_images_dir)

# ---------- Expose settings on app.state ----------
app.state.settings = settings

# ---------- Metrics endpoint ----------
if make_asgi_app is not None:
    from starlette.types import ASGIApp

    metrics_app = cast(ASGIApp, make_asgi_app())
    app.mount("/metrics", metrics_app)
else:
    logger.debug("/metrics not mounted (prometheus_client missing)")

# ---------- Global Exception Handler ----------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    from fastapi.exceptions import RequestValidationError
    from starlette.exceptions import HTTPException as StarletteHTTPException
    
    # Let FastAPI handle its own exceptions
    if isinstance(exc, (StarletteHTTPException, RequestValidationError)):
        raise exc
    
    request_id = getattr(request.state, "request_id", "unknown")
    # ✅ logger.exception already includes traceback
    logger.exception("Unhandled exception [req_id=%s]", request_id)
    
    if settings.ENVIRONMENT.lower() == "prod":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error", "request_id": request_id}
        )
    else:
        # In dev, re-raise to show full traceback
        raise exc

# ---------- CLI for local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=settings.BACKEND_PORT, reload=True)
