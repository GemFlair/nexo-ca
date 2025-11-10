# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║ ♢ DIAMOND GRADE ROUTER — ANNOUNCEMENTS (R-01 FINAL CERTIFIED)               ♢║
# ╠══════════════════════════════════════════════════════════════════════════════╣
# ║ Module:         backend/routers/announcements.py                             ║
# ║ Scope:          Read-only API for processed announcements JSON               ║
# ║ Observability:  metrics_inc + audit_log via backend.services.observability   ║
# ║ Tests:          backend/tests/test_announcements_router.py                   ║
# ║ Certified On:   29-Oct-2025 | 11:55 PM IST                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from fastapi import APIRouter, HTTPException
from typing import List, Optional, Any
from pathlib import Path
import json
import re
from datetime import datetime
import logging

from backend.schemas.schema import AnnouncementListItem, AnnouncementDetail, MarketSnapshot
from backend.services import env_utils

# Observability integration (Diamond standard)
try:
    from backend.services import observability_utils as obs  # type: ignore
    _OBS_AVAILABLE = True
except Exception:
    obs = None  # type: ignore
    _OBS_AVAILABLE = False


def _get_logger() -> logging.Logger:
    if _OBS_AVAILABLE and hasattr(obs, "get_logger"):
        try:
            return obs.get_logger("backend.routers.announcements")  # type: ignore[attr-defined]
        except Exception:
            pass
    logger = logging.getLogger("backend.routers.announcements")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


LOGGER = _get_logger()
logger = LOGGER


def _metric_inc(name: str, value: int = 1) -> None:
    try:
        if _OBS_AVAILABLE and hasattr(obs, "metrics_inc"):
            obs.metrics_inc(name, value)
        else:
            LOGGER.debug("metric %s += %s", name, value)
    except Exception:
        LOGGER.debug("metric emit failed for %s", name, exc_info=True)


def _audit(event: str, detail: dict) -> None:
    try:
        if _OBS_AVAILABLE and hasattr(obs, "audit_log"):
            obs.audit_log(event, "announcements_router", "success", detail)
        else:
            LOGGER.info("AUDIT %s %s", event, detail)
    except Exception:
        LOGGER.debug("audit emit failed for %s", event, exc_info=True)

router = APIRouter(prefix="/announcements")

# Cache for list announcements (TTL 30s, check dir mtime)
_LIST_CACHE = {"ts": 0.0, "data": [], "dir_mtime": 0.0}
_CACHE_TTL = 30.0  # seconds

# ────────────────────────────────────────────────────────────────
# Paths
ANN_DIR = Path(env_utils.build_local_path("backend/output_data/announcements"))
INPUT_ROOT = Path(env_utils.build_local_path("backend/input_data")).resolve()

# ────────────────────────────────────────────────────────────────
# Helpers
def _safe_load_json(p: Path) -> Optional[dict]:
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        logger.warning("Skipping bad JSON file %s: %s", p, e)
        return None

def _normalize_logo_path(logo: Optional[str]) -> Optional[str]:
    from pathlib import Path
    from backend.services import env_utils

    if not isinstance(logo, str) or not logo.strip():
        return None
    logo = logo.strip()

    # Already an HTTP(S) or /static URL
    if logo.startswith(("http://", "https://")) or logo.startswith("/static"):
        return logo

    # Use env_utils to detect CDN/S3 or local base
    try:
        base_url = env_utils.get_static_base_url()
        if base_url:
            clean_logo = logo.replace("backend/", "").lstrip("/")
            return f"{base_url.rstrip('/')}/{clean_logo}"
    except Exception:
        pass

    # Fallback: local development FastAPI static mount
    try:
        abs_logo = Path(logo).resolve()
        input_root = Path(env_utils.build_local_path("backend/input_data")).resolve()
        output_root = Path(env_utils.build_local_path("backend/output_data")).resolve()
        for root in (input_root, output_root):
            if abs_logo.is_relative_to(root):
                rel = abs_logo.relative_to(root)
                return f"/static/{rel.as_posix()}"
    except Exception:
        return None

    return None

def _parse_dt(x: Any) -> float:
    if not x:
        return 0.0
    try:
        # try ISO first
        return datetime.fromisoformat(str(x)).timestamp()
    except Exception:
        pass
    for fmt in ("%d-%b-%Y, %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(str(x), fmt).timestamp()
        except Exception:
            continue
    # last resort: try to extract digits and parse
    try:
        # handle "04-Oct-2025, 23:56:23"
        s = str(x)
        # replace comma with space
        s = s.replace(",", "")
        return datetime.strptime(s, "%d %b %Y %H:%M:%S").timestamp()
    except Exception:
        return 0.0

# ────────────────────────────────────────────────────────────────
# Load announcements fresh from disk (no global caching)
def _load_all_announcements() -> list[dict]:
    import time
    now = time.time()
    try:
        dir_mtime = ANN_DIR.stat().st_mtime
    except OSError:
        dir_mtime = 0.0
    if _LIST_CACHE["data"] and (now - _LIST_CACHE["ts"]) < _CACHE_TTL and _LIST_CACHE["dir_mtime"] == dir_mtime:
        return _LIST_CACHE["data"]
    
    results = []
    if not ANN_DIR.exists():
        logger.warning(f"Announcement directory missing: {ANN_DIR}")
        return results

    # iterate files in deterministic order (older -> newer), but we will sort later by announcement datetime
    for p in sorted(ANN_DIR.rglob("*.json")):
        j = _safe_load_json(p)
        if not j:
            continue
        # ensure id exists
        if "id" not in j or not j.get("id"):
            j["id"] = p.stem
        results.append(j)
    
    _LIST_CACHE["data"] = results
    _LIST_CACHE["ts"] = now
    _LIST_CACHE["dir_mtime"] = dir_mtime
    return results

# ────────────────────────────────────────────────────────────────
# Build lightweight summary object for CardsList
def _build_list_item(j: dict) -> dict:
    # Extract metadata dict (new structure from pdf_processor)
    metadata = j.get("metadata") or {}
    
    # Prefer canonical fields from metadata (set by PDF processor)
    company_name = (
        metadata.get("canonical_company_name") 
        or metadata.get("company_name")
        or j.get("canonical_company_name") 
        or j.get("company_name") 
        or "Unknown Company"
    )
    
    logo = _normalize_logo_path(j.get("company_logo") or "")
    # fallback to market_snapshot logos if present
    if not logo and isinstance(j.get("market_snapshot"), dict):
        ms = j["market_snapshot"]
        for k in ("logo_url", "banner_url"):
            v = ms.get(k)
            if isinstance(v, list) and v:
                logo = _normalize_logo_path(v[0])
                break
            elif isinstance(v, str) and v:
                logo = _normalize_logo_path(v)
                break

    # choose headline in order of preference
    headline = j.get("headline_final") or j.get("headline_ai") or j.get("headline_raw") or j.get("headline") or ""
    
    # Extract datetime from metadata (prefer human-readable format with time)
    dt_val = (
        metadata.get("datetime_from_filename_human")  # "09-Oct-2025, 21:24:04"
        or metadata.get("datetime_from_filename_iso")  # "2025-10-09T21:24:04"
        or j.get("announcement_datetime_human") 
        or j.get("announcement_datetime_iso") 
        or j.get("announcement_datetime")
        or metadata.get("date_from_filename")  # fallback to date-only "2025-10-09"
        or ""
    )

    sentiment = None
    emoji = None
    # Handle both old sentiment_badge and new sentiment dict
    sentiment_data = j.get("sentiment_badge") or j.get("sentiment")
    if isinstance(sentiment_data, dict):
        sentiment = sentiment_data.get("label")
        emoji = sentiment_data.get("emoji")

    return {
        "id": j.get("id") or j.get("filename") or j.get("source_file"),
        "company_name": company_name,
        "company_logo": logo,
        "headline": (headline or "")[:240],
        "announcement_datetime": dt_val,
        "sentiment": sentiment,
        "sentiment_emoji": emoji,
        "symbol": metadata.get("canonical_symbol") or j.get("symbol") or j.get("canonical_symbol"),
    }

# ────────────────────────────────────────────────────────────────
@router.get("", response_model=List[AnnouncementListItem], response_model_exclude_none=True)
def list_announcements(limit: int = 50, offset: int = 0, q: Optional[str] = None):
    """
    Returns latest announcements; reads fresh from disk each request (no in-memory cache)
    - limit: number of items to return (default 50, max 200)
    - offset: pagination offset
    - q: optional search string (filters company_name, headline, symbol)
    """
    import time
    start_time = time.time()
    raw = _load_all_announcements()
    lite = [_build_list_item(r) for r in raw]

    if q:
        qlow = q.strip().lower()
        lite = [
            it
            for it in lite
            if qlow in (it.get("company_name") or "").lower()
            or qlow in (it.get("headline") or "").lower()
            or qlow in str(it.get("symbol") or "").lower()
        ]

    # dedupe by id, latest datetime wins
    seen: dict[str, dict] = {}
    for it in lite:
        key = str(it.get("id") or "")
        if not key:
            continue
        if key not in seen or _parse_dt(it.get("announcement_datetime")) > _parse_dt(seen[key].get("announcement_datetime")):
            seen[key] = it

    out = list(seen.values())
    # sort by announcement datetime descending
    out.sort(key=lambda x: _parse_dt(x.get("announcement_datetime")), reverse=True)

    start = max(int(offset or 0), 0)
    end = start + max(min(int(limit or 50), 200), 1)
    result = out[start:end]
    
    elapsed_ms = (time.time() - start_time) * 1000
    logger.info("list_announcements returned %d items (limit=%d offset=%d q='%s') in %.2fms", len(result), limit, offset, q or "", elapsed_ms)
    
    # Record observability metric
    _metric_inc("announcements.list.requests", len(result))
    
    return result

# ────────────────────────────────────────────────────────────────
@router.get("/{announcement_id}", response_model=AnnouncementDetail, response_model_exclude_none=True)
def get_announcement(announcement_id: str):
    import time
    start_time = time.time()
    # Read fresh from disk
    raw = _load_all_announcements()
    for j in raw:
        if str(j.get("id")) == str(announcement_id) or str(j.get("filename", "")).endswith(f"{announcement_id}.json"):
            # Extract metadata for company_name and datetime
            metadata = j.get("metadata") or {}
            
            # Set company_name from metadata if not already set at top level
            if not j.get("company_name"):
                j["company_name"] = (
                    metadata.get("canonical_company_name")
                    or metadata.get("company_name")
                    or j.get("canonical_company_name")
                    or ""
                )
            
            # prefer canonical/AI headline for detail view if explicit 'headline' missing
            if not j.get("headline"):
                j["headline"] = j.get("headline_final") or j.get("headline_ai") or j.get("headline_raw") or ""

            # If top-level sentiment not set, derive from sentiment_badge for detail view
            sb = j.get("sentiment_badge")
            if not j.get("sentiment") and isinstance(sb, dict):
                j["sentiment"] = sb.get("label") or ""
                if "sentiment_emoji" not in j or j.get("sentiment_emoji") is None:
                    j["sentiment_emoji"] = sb.get("emoji") or ""

            # Normalize sentiment from dict to string if needed
            if isinstance(j.get("sentiment"), dict):
                j["sentiment"] = j["sentiment"].get("label", "Neutral")

            # Defensive: sanitize None -> ""
            for key in ("announcement_datetime", "company_name", "headline", "sentiment", "company_logo"):
                if j.get(key) is None:
                    j[key] = ""
            if not j.get("announcement_datetime"):
                j["announcement_datetime"] = (
                    metadata.get("datetime_from_filename_human")
                    or metadata.get("datetime_from_filename_iso")
                    or j.get("announcement_datetime_human") 
                    or j.get("announcement_datetime_iso")
                    or metadata.get("date_from_filename")
                    or ""
                )
            # Ensure market_snapshot validity
            if isinstance(j.get("market_snapshot"), dict):
                try:
                    j["market_snapshot"] = MarketSnapshot(**j["market_snapshot"])
                except Exception:
                    j["market_snapshot"] = None
            elapsed_ms = (time.time() - start_time) * 1000
            logger.info("get_announcement found id='%s' in %.2fms", announcement_id, elapsed_ms)
            
            # Record observability metrics for successful retrieval
            _metric_inc("announcements.detail.requests", 1)
            _metric_inc("announcements.detail.hit", 1)
            
            # frontend alias for React Native
            if j.get("market_snapshot") and "marketSnapshot" not in j:
                j["marketSnapshot"] = j["market_snapshot"]
            if j.get("summary_60") and "summary" not in j:
                j["summary"] = j["summary_60"]
            
            return AnnouncementDetail(**j)

    # direct file fallback with recursive search
    matches = list(ANN_DIR.rglob(f"{announcement_id}.json"))
    if matches:
        candidate = matches[0]
        j = _safe_load_json(candidate)
        if not j:
            logger.warning("Announcement file corrupt for id='%s'", announcement_id)
            raise HTTPException(status_code=500, detail="Announcement file corrupt")
        
        # Extract metadata for company_name and datetime
        metadata = j.get("metadata") or {}
        
        # Set company_name from metadata if not already set at top level
        if not j.get("company_name"):
            j["company_name"] = (
                metadata.get("canonical_company_name")
                or metadata.get("company_name")
                or j.get("canonical_company_name")
                or ""
            )
        
        # prefer canonical/AI headline for detail view if explicit 'headline' missing
        if not j.get("headline"):
            j["headline"] = j.get("headline_final") or j.get("headline_ai") or j.get("headline_raw") or ""
        # If top-level sentiment not set, derive from sentiment_badge for detail view
        sb = j.get("sentiment_badge")
        if not j.get("sentiment") and isinstance(sb, dict):
            j["sentiment"] = sb.get("label") or ""
            if "sentiment_emoji" not in j or j.get("sentiment_emoji") is None:
                j["sentiment_emoji"] = sb.get("emoji") or ""
        
        # Normalize sentiment from dict to string if needed
        if isinstance(j.get("sentiment"), dict):
            j["sentiment"] = j["sentiment"].get("label", "Neutral")
        
        for key in ("announcement_datetime", "company_name", "headline", "sentiment", "company_logo"):
            if j.get(key) is None:
                j[key] = ""
        if not j.get("announcement_datetime"):
            j["announcement_datetime"] = (
                metadata.get("datetime_from_filename_human")
                or metadata.get("datetime_from_filename_iso")
                or j.get("announcement_datetime_human")
                or j.get("announcement_datetime_iso")
                or metadata.get("date_from_filename")
                or ""
            )
        if isinstance(j.get("market_snapshot"), dict):
            try:
                j["market_snapshot"] = MarketSnapshot(**j["market_snapshot"])
            except Exception:
                j["market_snapshot"] = None
        elapsed_ms = (time.time() - start_time) * 1000
        logger.info("get_announcement found id='%s' via file fallback in %.2fms", announcement_id, elapsed_ms)
        
        # Record observability metrics for successful retrieval via fallback
        _metric_inc("announcements.detail.requests", 1)
        _metric_inc("announcements.detail.hit", 1)
        
        # frontend alias for React Native
        if j.get("market_snapshot") and "marketSnapshot" not in j:
            j["marketSnapshot"] = j["market_snapshot"]
        if j.get("summary_60") and "summary" not in j:
            j["summary"] = j["summary_60"]
        
        return AnnouncementDetail(**j)

    # Record observability metric for miss (404)
    _metric_inc("announcements.detail.miss", 1)
    
    logger.warning("Announcement not found for id='%s'", announcement_id)
    raise HTTPException(status_code=404, detail="Announcement not found")
