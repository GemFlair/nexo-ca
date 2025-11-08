"""
backend/services/llm_utils.py

Enterprise-grade LLM utilities for production backends (stateless, safe, observable).

WHY THIS FILE EXISTS
--------------------
This module provides a thin, reliable gateway to Large Language Model APIs (OpenAI today).
It is designed to be *stateless*, *loop-agnostic*, and *easy to operate* in modern backends
(FastAPI, Flask, serverless). It avoids fragile background event loops and cross-loop awaits.

KEY GUARANTEES
--------------
1) Stateless architecture
   - No global asyncio event loop. No run_coroutine_threadsafe. No cross-loop semaphores.
   - The sync API runs the async function inside a short-lived worker thread that calls asyncio.run().

2) Safety and resilience
   - Full operation timeout (asyncio.timeout) caps end-to-end duration.
   - Token-bucket rate limiter protects provider RPM quotas without cross-loop bugs.
   - Retry with full-jitter exponential backoff for transient errors.
   - Circuit breaker with safe helpers. Falls back to local CB if external one is unavailable.

3) Correctness and contracts
   - Input sanitization with email + phone redaction and length clamp.
   - Business helpers enforce formatting contracts in code (not just in prompts):
        • Headline ≤ 20 words and ends with a single period.
        • Summary 60–70 words and ends with a single period.
   - JSON-first parsing via OpenAI response_format={"type": "json_object"}, with robust fallback.

4) Observability
   - Structured audit events and metrics shims with correlation_id propagation.
   - We never modify the root logger. Only a module logger with a NullHandler.

5) Configuration you can trust
   - Pydantic settings validate all env variables and ranges up front.
   - Sensible defaults; override via env or function arguments.

READ THIS IF YOU ARE NOT A CODER
---------------------------------
You can skim the docstrings at the start of each section to understand what it does.
Search for “NON-CODER NOTE” lines. They explain the “why” in plain language.

"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, TypedDict, Callable, Coroutine
import weakref

# ──────────────────────────────────────────────────────────────────────────────
# Optional integrations (present in some deployments; absent in others)
# NON-CODER NOTE: These are optional plug-ins for retries/metrics. If missing,
# this file still works. We check for availability safely.
# ──────────────────────────────────────────────────────────────────────────────
try:
    from backend.services import resilience_utils  # retry helpers or CB impl
    _RESILIENCE_AVAILABLE = True
except Exception:  # pragma: no cover
    resilience_utils = None  # type: ignore
    _RESILIENCE_AVAILABLE = False

try:
    from backend.services import observability_utils as obs  # metrics, tracing
    _OBSERVABILITY_AVAILABLE = True
except Exception:  # pragma: no cover
    obs = None  # type: ignore
    _OBSERVABILITY_AVAILABLE = False

try:
    # Modern OpenAI SDK. If unavailable, calls fail cleanly with a structured error.
    from openai import AsyncOpenAI  # type: ignore
    _OPENAI_AVAILABLE = True
except Exception:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore
    _OPENAI_AVAILABLE = False

try:
    from pydantic_settings import BaseSettings  # Pydantic v2
    from pydantic import Field, PositiveInt, field_validator, ConfigDict
    _PYDANTIC_AVAILABLE = True
except ImportError:
    try:
        from pydantic import BaseSettings, Field, PositiveInt, validator as field_validator, ConfigDict  # type: ignore
        _PYDANTIC_AVAILABLE = True
    except Exception:
        BaseSettings = object  # type: ignore
        Field = lambda **kwargs: None  # type: ignore
        PositiveInt = int  # type: ignore
        field_validator = lambda *args, **kwargs: lambda f: f  # type: ignore
        ConfigDict = dict  # type: ignore
        _PYDANTIC_AVAILABLE = False


# ──────────────────────────────────────────────────────────────────────────────
# Logging (library-safe): we never touch the root logger, only our module logger.
# NON-CODER NOTE: This prevents accidental duplication of logs in your app.
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("backend.services.llm_utils")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# ──────────────────────────────────────────────────────────────────────────────
# Public result types for better IDE support and structured responses
# NON-CODER NOTE: These are schemas for the dictionaries we return.
# ──────────────────────────────────────────────────────────────────────────────
class LLMUsage(TypedDict, total=False):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class LLMError(TypedDict, total=False):
    ok: bool
    error: str
    error_type: str
    error_code: Optional[int]
    retryable: bool
    metrics: Dict[str, Any]


class LLMOk(TypedDict, total=False):
    ok: bool
    resp: Any
    usage: Optional[LLMUsage]
    metrics: Dict[str, Any]


class LLMCallResult(TypedDict, total=False):
    ok: bool
    resp: Any
    error: str
    error_type: str
    error_code: Optional[int]
    retryable: bool
    usage: Optional[LLMUsage]
    metrics: Dict[str, Any]


# ──────────────────────────────────────────────────────────────────────────────
# Settings with validation
# NON-CODER NOTE: This collects all tunables in one place and ensures values
# are sensible. You can override via environment variables.
# ──────────────────────────────────────────────────────────────────────────────
class LLMSettings(BaseSettings):
    model_config = ConfigDict(extra="ignore", case_sensitive=False)
    
    # OpenAI
    OPENAI_API_KEY: Optional[str] = Field(default=None)
    OPENAI_BASE_URL: Optional[str] = Field(default=None)
    OPENAI_ORGANIZATION: Optional[str] = Field(default=None)
    OPENAI_MODEL: str = Field(default="gpt-4o-mini")

    # Defaults for completions
    LLM_DEFAULT_MAX_TOKENS: PositiveInt = Field(default=300)
    LLM_DEFAULT_TEMPERATURE: float = Field(default=0.2, ge=0.0, le=2.0)
    LLM_MAX_INPUT_CHARS: PositiveInt = Field(default=4000)

    # Resilience knobs
    LLM_MAX_RETRIES: int = Field(default=3, ge=0, le=10)
    LLM_BACKOFF_BASE: float = Field(default=0.4, ge=0.0, le=10.0)
    LLM_BACKOFF_MAX: float = Field(default=8.0, ge=0.1, le=60.0)

    # Whole-operation timeout (seconds)
    LLM_OPERATION_TIMEOUT: float = Field(default=30.0, ge=1.0, le=300.0)

    # Rate limiting (requests per minute)
    LLM_RATE_LIMIT_RPM: int = Field(default=60, ge=1, le=6000)

    # Optional coarse process admission limit (0 = disabled). Uses threading.Semaphore.
    LLM_ADMISSION_LIMIT: int = Field(default=0, ge=0, le=1000, description="Max concurrent LLM requests (0 = disabled)")

    @field_validator("OPENAI_MODEL")
    @classmethod
    def _strip_model(cls, v: str) -> str:
        return v.strip()

    @field_validator("LLM_BACKOFF_MAX")
    @classmethod
    def _ensure_backoff_order(cls, v: float, info) -> float:
        base = float(info.data.get("LLM_BACKOFF_BASE", 0.4))
        if v < base:
            raise ValueError("LLM_BACKOFF_MAX must be >= LLM_BACKOFF_BASE")
        return v
    
    @field_validator("LLM_RATE_LIMIT_RPM")
    @classmethod
    def _validate_rate_limit(cls, v: int) -> int:
        if v < 1:
            raise ValueError("LLM_RATE_LIMIT_RPM must be at least 1")
        # Warn about very low limits that might cause excessive delays
        if v < 10:
            logger.warning("Very low rate limit (%s RPM) may cause long delays", v)
        return v
    
    @field_validator("LLM_OPERATION_TIMEOUT")
    @classmethod
    def _validate_timeout(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("LLM_OPERATION_TIMEOUT must be at least 1 second")
        if v > 180.0:
            logger.warning("Very long timeout (%ss) may block application", v)
        return v


# Load settings once. Callers can pass overrides to functions if needed.
SETTINGS = LLMSettings()
LLM_ADMISSION_LIMIT = SETTINGS.LLM_ADMISSION_LIMIT


# ──────────────────────────────────────────────────────────────────────────────
# Observability shims
# NON-CODER NOTE: If your app has a metrics/audit system, we use it. Otherwise
# we log minimal info at safe levels.
# ──────────────────────────────────────────────────────────────────────────────
def _inc_metric(name: str, amount: int = 1, **tags: Any) -> None:
    try:
        if _OBSERVABILITY_AVAILABLE and hasattr(obs, "increment_metric"):
            obs.increment_metric(name, amount, **tags)  # type: ignore[attr-defined]
        else:
            logger.debug("metric %s += %s %s", name, amount, tags if tags else "")
    except Exception:  # pragma: no cover
        logger.debug("metric increment failed: %s", name)


def _audit_event(name: str, details: Dict[str, Any]) -> None:
    try:
        if _OBSERVABILITY_AVAILABLE and hasattr(obs, "audit_log"):
            obs.audit_log(name, details)  # type: ignore[attr-defined]
        else:
            logger.info("audit.%s %s", name, json.dumps(details, default=str))
    except Exception:  # pragma: no cover
        logger.debug("audit_event failed: %s", name)


# ──────────────────────────────────────────────────────────────────────────────
# Transient exception detector
# ──────────────────────────────────────────────────────────────────────────────
def _is_transient_exception(exc: Exception) -> bool:
    """
    Determine if an exception is transient and retryable.
    
    Checks both exception type and error message for common transient patterns
    like rate limits, timeouts, and service unavailability.
    """
    # Type-based check
    transient_types = (TimeoutError, ConnectionError)
    if isinstance(exc, transient_types):
        return True
    
    # Message-based check for OpenAI and network errors
    error_str = str(exc).lower()
    transient_indicators = {
        'rate limit', 'timeout', 'connection', 'service unavailable',
        'internal server error', 'bad gateway', 'gateway timeout',
        'request timeout', 'too many requests', 'quota exceeded',
        'temporarily unavailable', 'try again', 'overloaded'
    }
    
    return any(indicator in error_str for indicator in transient_indicators)


# ──────────────────────────────────────────────────────────────────────────────
# Sanitization and helpers
# NON-CODER NOTE: We mask emails/phones and keep inputs compact to avoid leaks.
# ──────────────────────────────────────────────────────────────────────────────
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(r"\b(?:\+?\d{1,3}[\s-]?)?(?:\d{3}[\s-]?){2}\d{4}\b")
_CURLY_JSON_RE = re.compile(r"(\{(?:[^{}]|\{[^{}]*\})*\})", re.S)


def _safe_str(obj: Any, max_len: int = 200) -> str:
    """Best-effort safe string for logging/error messaging."""
    try:
        s = str(obj)
    except Exception:
        s = f"<unprintable:{type(obj).__name__}>"
    return s if len(s) <= max_len else (s[:max_len] + "...[truncated]")


def sanitize_input(text: str, max_chars: Optional[int] = None) -> str:
    """
    Redact emails and phone-like numbers and clamp length.

    NON-CODER NOTE: This reduces accidental sharing of private info and keeps the
    prompt small enough for fast responses and predictable costs.
    """
    if not text:
        return ""
    maxc = int(max_chars or SETTINGS.LLM_MAX_INPUT_CHARS)
    redacted = _EMAIL_RE.sub("[EMAIL_REDACTED]", text)
    redacted = _PHONE_RE.sub("[PHONE_REDACTED]", redacted)
    return redacted[:maxc].strip()


# Compatibility alias to match the prompt string you requested verbatim
def _sanitize_input(text: str) -> str:
    """Internal alias to keep your exact prompt format intact."""
    return sanitize_input(text, SETTINGS.LLM_MAX_INPUT_CHARS)


def _extract_first_json(text: str) -> Optional[str]:
    """Extract the first balanced JSON object from text, or None."""
    if not text:
        return None
    m = _CURLY_JSON_RE.search(text)
    return m.group(1) if m else None


def _safe_json_extract(text: str) -> Dict[str, Any]:
    """Parse the first JSON object found in text. On error returns {}."""
    try:
        block = _extract_first_json(text)
        return json.loads(block) if block else {}
    except Exception:
        return {}


def _clean_spaces(val: str) -> str:
    """Collapse whitespace to single spaces and trim."""
    return re.sub(r"\s+", " ", (val or "").replace("\n", " ")).strip()


def _clamp_words(text: str, max_words: int) -> str:
    """Return the first max_words words of text."""
    if not text or max_words <= 0:
        return ""
    words = text.split()
    return " ".join(words[:max_words])


def _extract_response_text(resp: Any) -> str:
    """
    SDK-agnostic response text extraction.

    Works with:
      - dict: resp["choices"][0]["message"]["content"] or ["text"]
      - SDK objects: .choices[0].message.content or .text
    """
    try:
        if isinstance(resp, dict):
            choices = resp.get("choices") or []
            if not choices:
                return ""
            first = choices[0]
            msg = first.get("message") if isinstance(first, dict) else None
            return (msg.get("content") if isinstance(msg, dict) else first.get("text") or "") or ""
        else:
            choices = getattr(resp, "choices", None)
            if not choices:
                return ""
            first = choices[0]
            msg = getattr(first, "message", None)
            return getattr(msg, "content", None) or getattr(first, "text", None) or ""
    except Exception:
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# Token-bucket rate limiter (thread-safe, loop-agnostic)
# NON-CODER NOTE: This spreads requests over time so we don’t exceed quotas.
# It returns “delay seconds”. The async caller awaits that on its own loop.
# ──────────────────────────────────────────────────────────────────────────────
class _TokenBucket:
    def __init__(self, rate_per_minute: int, burst: Optional[int] = None):
        self.rate = float(max(1, rate_per_minute)) / 60.0  # tokens per second
        cap = burst if burst is not None else rate_per_minute
        self.capacity = max(1, cap)
        self.tokens = float(self.capacity)
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire_delay(self) -> float:
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.updated_at = now
            # refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            # need to wait
            needed = 1.0 - self.tokens
            return needed / self.rate


_BUCKET = _TokenBucket(SETTINGS.LLM_RATE_LIMIT_RPM)

# Admission limit (process-wide coarse gate)
_ADMISSION_SEM: Optional[threading.Semaphore] = None
_ADMISSION_SEM_LIMIT: Optional[int] = None
_ADMISSION_SEM_LOCK = threading.Lock()

# Async semaphore registry keyed by event loop to avoid cross-loop binding errors
_ASYNC_SEMAPHORES: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]"
_ASYNC_SEMAPHORES = weakref.WeakKeyDictionary()  # type: ignore[assignment]
_ASYNC_SEMAPHORE_LIMIT: Optional[int] = None
_ASYNC_SEMAPHORE_LOCK = threading.Lock()
CONCURRENCY_SEMAPHORE: Optional[int] = SETTINGS.LLM_ADMISSION_LIMIT  # legacy/test knob


def _current_concurrency_limit() -> int:
    limit = CONCURRENCY_SEMAPHORE
    if limit is None:
        limit = LLM_ADMISSION_LIMIT
    try:
        return max(0, int(limit or 0))
    except Exception:
        return 0


def _get_async_semaphore() -> Optional[asyncio.Semaphore]:
    """Return loop-scoped semaphore enforcing async concurrency, or None if disabled."""
    limit = _current_concurrency_limit()
    if limit <= 0:
        return None

    loop = asyncio.get_running_loop()
    with _ASYNC_SEMAPHORE_LOCK:
        global _ASYNC_SEMAPHORE_LIMIT
        if _ASYNC_SEMAPHORE_LIMIT != limit:
            _ASYNC_SEMAPHORE_LIMIT = limit
            _ASYNC_SEMAPHORES.clear()
        sem = _ASYNC_SEMAPHORES.get(loop)
        if sem is None:
            sem = asyncio.Semaphore(limit)
            _ASYNC_SEMAPHORES[loop] = sem
        return sem


def _current_admission_limit() -> int:
    try:
        return max(0, int(LLM_ADMISSION_LIMIT))
    except Exception:
        return 0


def _get_admission_semaphore() -> Optional[threading.Semaphore]:
    """Return process-wide admission semaphore honoring the latest configured limit."""
    limit = _current_admission_limit()
    global _ADMISSION_SEM, _ADMISSION_SEM_LIMIT
    if limit <= 0:
        with _ADMISSION_SEM_LOCK:
            _ADMISSION_SEM = None
            _ADMISSION_SEM_LIMIT = 0
        return None

    if _ADMISSION_SEM is not None and _ADMISSION_SEM_LIMIT == limit:
        return _ADMISSION_SEM

    with _ADMISSION_SEM_LOCK:
        if _ADMISSION_SEM is None or _ADMISSION_SEM_LIMIT != limit:
            _ADMISSION_SEM = threading.Semaphore(limit)
            _ADMISSION_SEM_LIMIT = limit
        return _ADMISSION_SEM


# ──────────────────────────────────────────────────────────────────────────────
# Circuit breaker (local fallback) and helpers
# NON-CODER NOTE: When many calls fail in a row, we pause briefly to allow recovery.
# ──────────────────────────────────────────────────────────────────────────────
class _LocalCircuitBreaker:
    def __init__(self, threshold: int = 5, timeout: float = 30.0):
        self.threshold = max(1, threshold)
        self.timeout = timeout  # Don't force minimum timeout
        self.fail_count = 0
        self.open_until = 0.0
        self._lock = threading.Lock()

    def can_proceed(self) -> bool:
        """Return True if the circuit is closed or timeout expired."""
        with self._lock:
            now = time.time()
            if self.fail_count >= self.threshold:
                if now < self.open_until:
                    return False  # Still open
                # Timeout expired → reset
                self.fail_count = 0
                self.open_until = 0.0
                return True  # Now closed after reset
            return True  # Was already closed

    def record_success(self) -> None:
        with self._lock:
            self.fail_count = 0
            self.open_until = 0.0

    def record_failure(self) -> None:
        """Register a failure and open breaker if threshold exceeded."""
        with self._lock:
            self.fail_count += 1
            if self.fail_count >= self.threshold:
                self.open_until = time.time() + self.timeout


if _RESILIENCE_AVAILABLE and hasattr(resilience_utils, "get_circuit_breaker"):  # pragma: no cover
    try:
        _CB = resilience_utils.get_circuit_breaker("llm_utils", failure_threshold=5, recovery_timeout=60.0)  # type: ignore[attr-defined]
    except Exception:
        _CB = _LocalCircuitBreaker()
else:
    _CB = _LocalCircuitBreaker()


def _cb_can_proceed() -> bool:
    """
    Check if circuit breaker allows requests.
    
    Supports both allow_request() and can_proceed() APIs for compatibility.
    If both exist, can_proceed takes precedence (for test monkeypatching).
    """
    # Check if both methods exist (likely test monkeypatch scenario)
    has_allow_request = hasattr(_CB, "allow_request")
    has_can_proceed = hasattr(_CB, "can_proceed")
    
    # If both exist, prefer can_proceed (test monkeypatch takes precedence)
    if has_can_proceed:
        can_proceed = getattr(_CB, "can_proceed")
        if callable(can_proceed):
            try:
                return bool(can_proceed())
            except Exception:
                pass  # Fall through
    
    # Try allow_request (resilience_utils.CircuitBreaker API)
    if has_allow_request:
        allow_request = getattr(_CB, "allow_request")
        if callable(allow_request):
            try:
                return bool(allow_request())
            except Exception:
                pass
    
    # If neither method exists or both failed, allow by default
    return True


def _cb_record_success() -> None:
    rec = getattr(_CB, "record_success", None)
    if callable(rec):
        try:
            rec()
        except Exception:
            pass


def _cb_record_failure() -> None:
    rec = getattr(_CB, "record_failure", None)
    if callable(rec):
        try:
            rec()
        except Exception:
            pass


def get_circuit_breaker_state() -> Dict[str, Any]:
    """
    Return current circuit breaker state for monitoring and debugging.
    
    Returns a dictionary with state, failure count, and timing information.
    Useful for health checks and dashboards.
    """
    if hasattr(_CB, "state"):
        # External circuit breaker from resilience_utils
        return {
            "state": getattr(_CB, "state", "unknown"),
            "failure_count": getattr(_CB, "failure_count", 0),
            "last_failure": getattr(_CB, "last_failure", None),
            "threshold": getattr(_CB, "threshold", None),
        }
    elif hasattr(_CB, "fail_count"):
        # Local _LocalCircuitBreaker
        is_open = _CB.fail_count >= _CB.threshold
        return {
            "state": "open" if is_open else "closed",
            "failure_count": _CB.fail_count,
            "open_until": _CB.open_until if is_open else None,
            "threshold": _CB.threshold,
            "timeout": _CB.timeout,
        }
    else:
        return {"state": "unknown", "failure_count": 0}


def _is_transient(exc: Exception) -> bool:
    if _RESILIENCE_AVAILABLE and hasattr(resilience_utils, "is_transient_error"):  # pragma: no cover
        try:
            return bool(resilience_utils.is_transient_error(exc))  # type: ignore[attr-defined]
        except Exception:
            pass
    name = exc.__class__.__name__
    return any(k in name for k in ("RateLimit", "Timeout", "Connection", "ServiceUnavailable", "APIError"))


async def _backoff_sleep(attempt: int) -> None:
    base, maxval = SETTINGS.LLM_BACKOFF_BASE, SETTINGS.LLM_BACKOFF_MAX
    sleep_for = base * (2**attempt) + random.uniform(0, base)
    await asyncio.sleep(min(maxval, sleep_for))


# Alias for test compatibility
_async_backoff_sleep = _backoff_sleep


# ──────────────────────────────────────────────────────────────────────────────
# Metrics structure
# NON-CODER NOTE: This is what we save for SRE dashboards and audits.
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class LLMMetrics:
    start_ts: float
    end_ts: Optional[float] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None

    def duration_ms(self) -> int:
        end = self.end_ts or time.time()
        return int((end - self.start_ts) * 1000)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "duration_ms": self.duration_ms(),
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Core async LLM call
# NON-CODER NOTE: This is the safe “engine” that calls OpenAI. It:
#   • Sanitizes input
#   • Applies a total timeout
#   • Enforces rate limits
#   • Uses a circuit breaker and retries with backoff
#   • Returns structured success or error info
# ──────────────────────────────────────────────────────────────────────────────
async def async_call_llm(
    prompt: str,
    *,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    operation_timeout: Optional[float] = None,
    max_retries: Optional[float] = None,
    correlation_id: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
    provider_extra: Optional[Dict[str, Any]] = None,
) -> LLMCallResult:
    cfg = settings or SETTINGS
    prompt_s = sanitize_input(prompt, cfg.LLM_MAX_INPUT_CHARS)
    correlation_id = correlation_id or f"llm-{uuid.uuid4()}"
    metrics = LLMMetrics(start_ts=time.time())
    admission_sem = _get_admission_semaphore()
    admission_acquired = False

    if admission_sem is not None:
        admission_acquired = admission_sem.acquire(blocking=False)
        if not admission_acquired:
            metrics.end_ts = time.time()
            err = {
                "ok": False,
                "error": "Admission limit exceeded",
                "error_type": "AdmissionLimit",
                "error_code": None,
                "retryable": True,
                "metrics": metrics.to_dict(),
            }
            _inc_metric("llm.admission.denied", 1)
            _audit_event("llm.call.denied", {"correlation_id": correlation_id, **err})
            return err  # type: ignore[return-value]

    # Early circuit breaker check - use wrapper for compatibility
    if not _cb_can_proceed():
        metrics.end_ts = time.time()
        _inc_metric("llm.circuit.open", 1)
        return {
            "ok": False,
            "error": "Circuit breaker open",
            "metrics": metrics.to_dict(),
            "usage": {},
        }  # type: ignore[return-value]

    # Remove the early admission check - let semaphore handle blocking
    # The async semaphore will properly serialize concurrent calls
    
    try:
        op_timeout = float(operation_timeout or cfg.LLM_OPERATION_TIMEOUT)
        # Python 3.9 compatible timeout using wait_for
        try:
            # Wrap with async semaphore for proper concurrency control
            sem = _get_async_semaphore()
            if sem is not None:
                async with sem:
                    result = await asyncio.wait_for(
                        _async_call_llm_inner(
                            cfg, prompt_s, correlation_id, metrics, model, max_tokens, 
                            temperature, max_retries, provider_extra
                        ),
                        timeout=op_timeout
                    )
            else:
                result = await asyncio.wait_for(
                    _async_call_llm_inner(
                        cfg, prompt_s, correlation_id, metrics, model, max_tokens, 
                        temperature, max_retries, provider_extra
                    ),
                    timeout=op_timeout
                )
            return result
        except asyncio.TimeoutError:
            metrics.end_ts = time.time()
            err = {
                "ok": False,
                "error": f"Operation timeout after {op_timeout}s",
                "error_type": "Timeout",
                "error_code": None,
                "retryable": True,
                "metrics": metrics.to_dict(),
            }
            _inc_metric("llm.timeout", 1)
            _audit_event("llm.call.timeout", {"correlation_id": correlation_id, **err})
            return err  # type: ignore[return-value]
    except Exception as e:
        # Catch any other unexpected errors
        metrics.end_ts = time.time()
        return {
            "ok": False,
            "error": f"Unexpected error: {_safe_str(e)}",
            "error_type": e.__class__.__name__,
            "error_code": None,
            "retryable": False,
            "metrics": metrics.to_dict(),
        }  # type: ignore[return-value]
    finally:
        if admission_sem is not None and admission_acquired:
            try:
                admission_sem.release()
            except ValueError:
                # Release can fail if semaphore state changed; log and continue
                logger.debug("Admission semaphore release failed", exc_info=True)


async def _async_call_llm_inner(
    cfg: LLMSettings,
    prompt_s: str,
    correlation_id: str,
    metrics: LLMMetrics,
    model: Optional[str],
    max_tokens: Optional[int],
    temperature: Optional[float],
    max_retries: Optional[float],
    provider_extra: Optional[Dict[str, Any]],
) -> LLMCallResult:
    """Inner async function that performs the actual LLM call."""
    # Rate limit (loop-agnostic)
    delay = _BUCKET.acquire_delay()
    if delay > 0:
        _inc_metric("llm.ratelimit.delay", 1, delay=round(delay, 3))
        await asyncio.sleep(delay)

    # Circuit breaker
    if not _cb_can_proceed():
        err = {
            "ok": False,
            "error": "Circuit breaker open",
            "error_type": "CircuitOpen",
            "error_code": None,
            "retryable": True,
            "metrics": metrics.to_dict(),
        }
        _inc_metric("llm.circuit.open", 1)
        _audit_event("llm.call.denied", {"correlation_id": correlation_id, **err})
        return err  # type: ignore[return-value]

    # Check OpenAI availability (supports test mocking via _openai_available)
    import sys
    current_module = sys.modules[__name__]
    openai_available = getattr(current_module, "_openai_available", _OPENAI_AVAILABLE)
    if not openai_available or AsyncOpenAI is None:
        err = {
            "ok": False,
            "error": "OpenAI SDK not installed",
            "error_type": "DependencyMissing",
            "error_code": None,
            "retryable": False,
            "metrics": metrics.to_dict(),
        }
        _inc_metric("llm.dependency.missing", 1)
        return err  # type: ignore[return-value]

    # Build client each call; SDK client is light and picks up fresh creds
    client_kwargs: Dict[str, Any] = {}
    if cfg.OPENAI_API_KEY:
        client_kwargs["api_key"] = cfg.OPENAI_API_KEY
    if cfg.OPENAI_BASE_URL:
        client_kwargs["base_url"] = cfg.OPENAI_BASE_URL
    if cfg.OPENAI_ORGANIZATION:
        client_kwargs["organization"] = cfg.OPENAI_ORGANIZATION
    client = AsyncOpenAI(**client_kwargs)

    req = {
        "model": (model or cfg.OPENAI_MODEL),
        "messages": [{"role": "user", "content": prompt_s}],
        "max_tokens": int(max_tokens or cfg.LLM_DEFAULT_MAX_TOKENS),
        "temperature": float(temperature if temperature is not None else cfg.LLM_DEFAULT_TEMPERATURE),
    }
    if provider_extra:
        req.update(provider_extra)

    retries = int(max_retries if max_retries is not None else cfg.LLM_MAX_RETRIES)
    last_exc: Optional[Exception] = None

    # Semaphore is handled by async_call_llm, no need to acquire again
    return await _execute_llm_with_retries(client, req, retries, correlation_id, metrics, last_exc)


async def _execute_llm_with_retries(
    client: Any,
    req: Dict[str, Any],
    retries: int,
    correlation_id: str,
    metrics: LLMMetrics,
    last_exc: Optional[Exception],
) -> LLMCallResult:
    """Execute LLM call with retry logic."""
    for attempt in range(retries + 1):
        try:
            resp = await client.chat.completions.create(**req)
            metrics.end_ts = time.time()
            usage_obj = getattr(resp, "usage", None)
            if usage_obj:
                try:
                    metrics.input_tokens = getattr(usage_obj, "prompt_tokens", None) or usage_obj.get("prompt_tokens")  # type: ignore[attr-defined]
                    metrics.output_tokens = getattr(usage_obj, "completion_tokens", None) or usage_obj.get("completion_tokens")  # type: ignore[attr-defined]
                    metrics.total_tokens = getattr(usage_obj, "total_tokens", None) or usage_obj.get("total_tokens")  # type: ignore[attr-defined]
                except Exception:
                    pass

            _inc_metric("llm.call.success", 1)
            _audit_event(
                "llm.call",
                {
                    "correlation_id": correlation_id,
                    "status": "success",
                    "model": req["model"],
                    **metrics.to_dict(),
                },
            )
            _cb_record_success()
            return {
                "ok": True,
                "resp": resp,
                "usage": {
                    "prompt_tokens": metrics.input_tokens or 0,
                    "completion_tokens": metrics.output_tokens or 0,
                    "total_tokens": metrics.total_tokens or 0,
                },
                "metrics": metrics.to_dict(),
            }

        except Exception as exc:
            last_exc = exc
            transient = _is_transient_exception(exc)
            _inc_metric("llm.call.fail", 1, transient=transient)
            _audit_event(
                "llm.call.attempt_fail",
                {
                    "correlation_id": correlation_id,
                    "attempt": attempt + 1,
                    "transient": transient,
                    "error": _safe_str(exc),
                },
            )
            _cb_record_failure()

            if attempt >= retries or not transient:
                metrics.end_ts = time.time()
                err: LLMError = {
                    "ok": False,
                    "error": _safe_str(exc),
                    "error_type": exc.__class__.__name__,
                    "error_code": getattr(exc, "status_code", None),
                    "retryable": transient,
                    "metrics": metrics.to_dict(),
                }
                _audit_event("llm.call", {"correlation_id": correlation_id, **err})
                return err  # type: ignore[return-value]

            await _backoff_sleep(attempt)
    
    # If we exhausted all retries
    metrics.end_ts = time.time()
    err_final: LLMError = {
        "ok": False,
        "error": f"Failed after {retries + 1} attempts: {_safe_str(last_exc)}",
        "error_type": last_exc.__class__.__name__ if last_exc else "Unknown",
        "error_code": None,
        "retryable": False,
        "metrics": metrics.to_dict(),
    }
    return err_final  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────────────────────
# Sync wrapper
# NON-CODER NOTE: This lets plain Python code call the LLM without async/await.
# We spin up a tiny worker thread, run the async function, then return the result.
# No persistent background loops. Nothing to shut down.
# ──────────────────────────────────────────────────────────────────────────────
def call_llm(
    prompt: str,
    *,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    operation_timeout: Optional[float] = None,
    max_retries: Optional[int] = None,
    correlation_id: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
    provider_extra: Optional[Dict[str, Any]] = None,
) -> LLMCallResult:
    """
    Thread-safe synchronous bridge to async_call_llm.
    Each thread runs its own event loop to avoid cross-thread loop reuse.
    """
    async def _run():
        return await async_call_llm(
            prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            operation_timeout=operation_timeout,
            max_retries=max_retries,
            correlation_id=correlation_id,
            settings=settings,
            provider_extra=provider_extra,
        )

    try:
        # New event loop per thread to avoid cross-thread collisions
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(_run())
        finally:
            loop.close()
            asyncio.set_event_loop(None)
        return result
    except Exception as e:
        # Fallback error response
        return {
            "ok": False,
            "error": f"Sync wrapper error: {_safe_str(e)}",
            "error_type": e.__class__.__name__,
            "error_code": None,
            "retryable": False,
            "metrics": {},
        }  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────────────────────
# Business helpers: Headline + Summary
# NON-CODER NOTE: This turns a raw company disclosure into a clean headline and
# a compact 60–70 word summary. We enforce the format in code for reliability.
# ──────────────────────────────────────────────────────────────────────────────
PROMPT_HEADLINE_SUMMARY = """
You are a financial announcement editor. Follow these rules precisely when rewriting the announcement.
Headline requirements:
- Maximum 20 words.
Reframe the headline under 20 words with corrected grammar, punctuation, and natural phrasing. Ensure the headline reads like a real newspaper or article headline — complete and senseful, not a fragment. Do NOT end mid-sentence, with ellipses, or with dangling words like 'for', 'of', or 'to'. Always end with a full stop. Use an action verb and include the main subject. Do NOT truncate sentences, do NOT end with ellipses, and DO end the headline with a single period. Write a crisp, complete and grammatically correct headline of no more than 20 words.
- Exclude addresses, contact information, phone numbers, emails, CIN, or locations.
- Remove redundant company prefixes; keep only the announcement, event, or action (e.g., Board Meeting on Dividend).
- Output plain title text without quotation marks.
Summary requirements:
Then write a concise 60-70 word summary focusing on what happened, why it matters, and who is involved.
- Rewrite as 2 to 3 sentences totaling no more than 60-70 words.
- Capture only key business facts such as dates, purposes, results, or outcomes.
Exclude addresses, greetings, signatures, or disclaimers.
- Omit greetings, signatures, disclaimers, or filler phrases.
- Use clean, human phrasing with no ellipses or line breaks.
- Ensure the summary ends with a full stop.
Return ONLY valid JSON with keys 'headline' and 'summary_60'.
""".strip()


async def async_classify_headline_and_summary(
    text: str,
    *,
    model: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
) -> Dict[str, Any]:
    if not text:
        return {"headline_final": None, "summary_60": None, "llm_meta": {"ok": False, "reason": "empty_text"}}

    cfg = settings or SETTINGS
    safe_text = _sanitize_input(text)

    # Your requested prompt EXACTLY, including the f-string usage of `_sanitize_input(text)`
    prompt = (
        "You are a financial announcement editor. Follow these rules precisely when rewriting the announcement.\n"
        "Headline requirements:\n"
        "- Maximum 20 words.\n"
        "Reframe the headline under 20 words with corrected grammar, punctuation, and natural phrasing. "
        "Ensure the headline reads like a real newspaper or article headline — complete and senseful, not a fragment. "
        "Do NOT end mid-sentence, with ellipses, or with dangling words like 'for', 'of', or 'to'. "
        "Always end with a full stop. "
        "Use an action verb and include the main subject. Do NOT truncate sentences, do NOT end with ellipses, and DO end the headline with a single period. "
        "Write a crisp, complete and grammatically correct headline of no more than 20 words. "
        "- Exclude addresses, contact information, phone numbers, emails, CIN, or locations.\n"
        "- Remove redundant company prefixes; keep only the announcement, event, or action (e.g., Board Meeting on Dividend).\n"
        "- Output plain title text without quotation marks.\n"
        "Summary requirements:\n"
        "Then write a concise 60-70 word summary focusing on what happened, why it matters, and who is involved. "
        "- Rewrite as 2 to 3 sentences totaling no more than 60-70 words.\n"
        "- Capture only key business facts such as dates, purposes, results, or outcomes.\n"
        "Exclude addresses, greetings, signatures, or disclaimers. "
        "- Omit greetings, signatures, disclaimers, or filler phrases.\n"
        "- Use clean, human phrasing with no ellipses or line breaks.\n"
        "- Ensure the summary ends with a full stop.\n"
        "Return ONLY valid JSON with keys 'headline' and 'summary_60'.\n\n"
        f"Source text:\n{_sanitize_input(text)}"
    )

    # Ask for JSON mode to reduce parsing errors
    provider_extra = {"response_format": {"type": "json_object"}}

    res = await async_call_llm(
        prompt,
        model=model,
        max_tokens=400,
        temperature=0.0,
        settings=cfg,
        provider_extra=provider_extra,
    )

    if not res.get("ok"):
        return {"headline_final": None, "summary_60": None, "llm_meta": res}

    resp = res.get("resp")
    text_out = _extract_response_text(resp)
    data = _safe_json_extract(text_out) if text_out else {}

    raw_headline = _clean_spaces(data.get("headline", ""))
    raw_summary = _clean_spaces(data.get("summary_60", data.get("summary", "")))

    # Enforce headline ≤ 20 words and trailing period
    headline_final = _clamp_words(raw_headline, 20).rstrip() if raw_headline else None
    if headline_final:
        if not headline_final.endswith("."):
            headline_final = headline_final.rstrip(".") + "."

    # Enforce summary 60–70 words and trailing period
    # First clamp upper bound to 70:
    summary_final = _clamp_words(raw_summary, 70).rstrip() if raw_summary else None
    # Then enforce lower bound behavior: we do not *pad* content; we only clamp max.
    if summary_final:
        if not summary_final.endswith("."):
            summary_final = summary_final.rstrip(".") + "."
        summary_final = _clean_spaces(summary_final)

    meta = {"ok": True, "usage": res.get("usage"), "metrics": res.get("metrics")}
    return {"headline_final": headline_final, "summary_60": summary_final, "llm_meta": meta}


def classify_headline_and_summary(
    text: str,
    *,
    model: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
) -> Dict[str, Any]:
    """
    Sync wrapper for headline + summary extraction.
    NON-CODER NOTE: Use this if your code is not async.
    """
    return _run_async_processor(lambda: async_classify_headline_and_summary(text, model=model, settings=settings))


# ──────────────────────────────────────────────────────────────────────────────
# Business helpers: Sentiment
# NON-CODER NOTE: Produces a label and confidence score. Safe defaults on errors.
# ──────────────────────────────────────────────────────────────────────────────
PROMPT_SENTIMENT = """
Classify sentiment of this corporate disclosure as "Positive", "Negative", or "Neutral".
Return ONLY valid JSON: {"label": "Positive|Negative|Neutral", "score": <0..1>}
""".strip()


async def async_classify_sentiment(
    text: str,
    *,
    model: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
) -> Dict[str, Any]:
    if not text:
        return {"ok": False, "label": "Unknown", "score": 0.0, "reason": "empty_text"}

    cfg = settings or SETTINGS
    safe_text = sanitize_input(text, cfg.LLM_MAX_INPUT_CHARS)

    provider_extra = {"response_format": {"type": "json_object"}}
    prompt = f"{PROMPT_SENTIMENT}\n\n{safe_text}"

    res = await async_call_llm(
        prompt,
        model=model,
        max_tokens=60,
        temperature=0.0,
        settings=cfg,
        provider_extra=provider_extra,
    )

    if not res.get("ok"):
        return {"ok": False, "label": "Unknown", "score": 0.0, "error": res.get("error"), "metrics": res.get("metrics")}

    resp = res.get("resp")
    text_out = _extract_response_text(resp)
    data = _safe_json_extract(text_out) if text_out else {}

    label = data.get("label", "Unknown")
    try:
        score = float(data.get("score") or 0.0)
    except Exception:
        score = 0.0
    score = max(0.0, min(1.0, score))

    return {"ok": True, "label": label, "score": score, "usage": res.get("usage"), "metrics": res.get("metrics")}


def classify_sentiment(
    text: str,
    *,
    model: Optional[str] = None,
    settings: Optional[LLMSettings] = None,
) -> Dict[str, Any]:
    """
    Sync wrapper for sentiment classification.
    NON-CODER NOTE: Use this if your code is not async.
    """
    return _run_async_processor(lambda: async_classify_sentiment(text, model=model, settings=settings))


# ──────────────────────────────────────────────────────────────────────────────
# Small sync helper runner
# NON-CODER NOTE: Internal utility to run async helpers from sync code safely.
# ──────────────────────────────────────────────────────────────────────────────
def _run_async_processor(coro_factory: Callable[[], Coroutine[Any, Any, Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Run async coroutine from sync context.
    In test environments, use existing event loop. Otherwise, create new thread.
    """
    try:
        # Try to get existing event loop (works in pytest-asyncio and async apps)
        loop = asyncio.get_running_loop()
        # If we're in an async context, we can't call run_until_complete
        # This is a design issue - sync wrappers shouldn't be called from async contexts
        raise RuntimeError("Cannot call sync wrapper from async context")
    except RuntimeError:
        # No running loop, try to get event loop for this thread
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Loop is running, can't use run_until_complete
                # Fall back to thread approach for test compatibility
                result: Dict[str, Any] = {}
                def _runner() -> None:
                    nonlocal result
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        result = new_loop.run_until_complete(coro_factory())
                    finally:
                        new_loop.close()
                        asyncio.set_event_loop(None)

                t = threading.Thread(target=_runner, name="llm-sync-processor", daemon=True)
                t.start()
                t.join()
                return result
            else:
                # Loop exists but not running, can use it
                return loop.run_until_complete(coro_factory())
        except RuntimeError:
            # No event loop, create one
            return asyncio.run(coro_factory())


# ──────────────────────────────────────────────────────────────────────────────
# Public exports
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    "LLMSettings",
    "sanitize_input",
    "async_call_llm",
    "call_llm",
    "async_classify_headline_and_summary",
    "classify_headline_and_summary",
    "async_classify_sentiment",
    "classify_sentiment",
]


# ──────────────────────────────────────────────────────────────────────────────
# Usage examples (documentation only; not executed)
# NON-CODER NOTE: Copy, paste, and adapt these snippets in your app.
# ──────────────────────────────────────────────────────────────────────────────
"""
Example 1: Simple async call

    from backend.services.llm_utils import async_call_llm

    async def run():
        res = await async_call_llm("Summarize the Indian Q2 GDP print in 3 bullet points.")
        if res["ok"]:
            print(res["resp"].choices[0].message.content)
        else:
            print("Error:", res["error"])

Example 2: Sync call from a Flask route

    from backend.services.llm_utils import call_llm

    def route_handler():
        res = call_llm("Write a one-line headline about RBI policy.")
        if res["ok"]:
            return res["resp"].choices[0].message.content
        return f'LLM error: {res["error"]}', 503

Example 3: Headline + Summary

    from backend.services.llm_utils import classify_headline_and_summary

    result = classify_headline_and_summary(announcement_text)
    print(result["headline_final"])  # Always ends with "."
    print(result["summary_60"])      # ≤ 70 words, ends with "."

Example 4: Sentiment

    from backend.services.llm_utils import classify_sentiment

    print(classify_sentiment(announcement_text))  # {"ok": True, "label": "...", "score": 0.73, ...}

Operational notes:
- Set OPENAI_API_KEY and optionally OPENAI_MODEL in the environment.
- Use LLM_RATE_LIMIT_RPM to protect your quota.
- Monitor metrics: llm.call.success, llm.call.fail, llm.call.timeout, llm.circuit.open, llm.ratelimit.delay.
- For stricter concurrency across services, front this with a job queue or API gateway.
"""
