# tests/test_image_utils.py
"""
Diamond Test Suite — Image Utils (R-04 Final, corrected)
--------------------------------------------------------
Robust, CI-friendly tests for backend.services.image_utils.

Key fixes included:
 - Avoid real S3 I/O by disabling aws_utils.get_s3_fs.
 - Deterministic cache testing by replacing the cached function
   with a new cached wrapper around a test stub (ensures counting).
 - Defensive cache_clear and thread-pool cleanup.
"""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest

# Try safe import of aws_utils for monkeypatching (may be absent in some dev envs)
try:
    import backend.services.aws_utils as aws_utils  # type: ignore
except Exception:  # pragma: no cover - defensive
    aws_utils = None

# Module under test
import backend.services.image_utils as iu


# -------------------------
# Fixtures: isolation & cleanup
# -------------------------
@pytest.fixture(autouse=True)
def isolated_env(monkeypatch, tmp_path):
    """
    Per-test isolation:
      - Redirect processed dirs to tmp_path
      - Clear TTL cache (if present)
      - Shutdown thread pool
      - Disable live aws_utils.get_s3_fs (if available)
      - Use short TTLs for tests
    """
    # Redirect processed dirs
    logos_dir = tmp_path / "processed_logos"
    banners_dir = tmp_path / "processed_banners"
    logos_dir.mkdir(parents=True, exist_ok=True)
    banners_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(iu, "PROCESSED_LOGOS", logos_dir, raising=False)
    monkeypatch.setattr(iu, "PROCESSED_BANNERS", banners_dir, raising=False)

    # Short TTLs for faster tests
    monkeypatch.setattr(iu, "IMAGE_CACHE_TTL_SECONDS", 2, raising=False)
    monkeypatch.setattr(iu, "IMAGE_CACHE_MAXSIZE", 128, raising=False)

    # Defensive cache clear
    cached_fn = getattr(iu, "_find_first_existing_uncached", None)
    if cached_fn is not None and hasattr(cached_fn, "cache_clear"):
        try:
            cached_fn.cache_clear()
        except Exception:
            pass

    # Ensure clean thread pool
    try:
        iu.shutdown_thread_pool(wait=True)
    except Exception:
        pass

    # Disable production aws_utils get_s3_fs to avoid network/auth issues during tests
    if aws_utils is not None:
        try:
            monkeypatch.setattr(aws_utils, "get_s3_fs", lambda *a, **k: (None, None), raising=False)
        except Exception:
            pass

    # Reset in-memory counters if present
    if hasattr(iu, "_COUNTERS"):
        for k in list(iu._COUNTERS.keys()):
            iu._COUNTERS[k] = 0

    # Default: disable S3 unless test opts in
    monkeypatch.setattr(iu, "S3_PROCESSED_IMAGES_PATH", "", raising=False)
    monkeypatch.setattr(iu, "CDN_BASE_URL", "", raising=False)
    monkeypatch.setattr(iu, "URL_BASE", "/static_test", raising=False)

    yield

    # Cleanup
    cached_fn = getattr(iu, "_find_first_existing_uncached", None)
    if cached_fn is not None and hasattr(cached_fn, "cache_clear"):
        try:
            cached_fn.cache_clear()
        except Exception:
            pass
    try:
        iu.shutdown_thread_pool(wait=True)
    except Exception:
        pass


@pytest.fixture
def mock_aws_success(monkeypatch):
    """
    Fake S3 success: ensure fsspec present and make _s3_exists_with_retry return True.
    """
    monkeypatch.setattr(iu, "fsspec", mock.Mock(), raising=False)
    monkeypatch.setattr(iu, "_s3_exists_with_retry", lambda fs, uri, att=3, delay=0.2: True, raising=False)


@pytest.fixture
def mock_aws_failure(monkeypatch):
    """
    Fake S3 failure: ensure fsspec present and make _s3_exists_with_retry return False.
    """
    monkeypatch.setattr(iu, "fsspec", mock.Mock(), raising=False)
    monkeypatch.setattr(iu, "_s3_exists_with_retry", lambda fs, uri, att=3, delay=0.2: False, raising=False)


# -------------------------
# Helpers & filename generation
# -------------------------
def test_normalize_and_slugify():
    assert iu.normalize_symbol("  Tata Motors  ") == "TATA_MOTORS"
    assert iu.normalize_symbol("") == ""
    assert iu.slugify_company("Reliance Industries Ltd.") == "reliance_industries_ltd"
    assert iu.slugify_company("  ") == ""


def test_candidate_generators_order():
    logos = iu.candidate_logo_filenames("HDFC", "HDFC Bank Ltd")
    assert logos[0].endswith("_logo.png")
    banners = iu.candidate_banner_filenames("TCS", "Tata Consultancy Services")
    assert any(x.endswith(".webp") for x in banners)


def test_encode_and_s3_uri_helpers():
    assert iu._ensure_s3_uri("bucket/key.png").startswith("s3://")
    assert iu._ensure_s3_uri("s3://bucket/key.png") == "s3://bucket/key.png"
    encoded = iu._encode_key_for_cdn("path/with space.png")
    assert "%20" in encoded or encoded != "path/with space.png"


# -------------------------
# Local lookup & placeholder
# -------------------------
def test_local_logo_lookup(tmp_path):
    logos_dir: Path = iu.PROCESSED_LOGOS
    (logos_dir / "FOO_logo.png").write_bytes(b"PNGDATA")

    candidate, url = iu.get_logo_path("FOO", "Foo Corp")
    assert candidate.local_path is not None
    assert Path(candidate.local_path).exists()
    assert "processed_logos" in url or url.startswith(iu.URL_BASE)


def test_placeholder_when_missing():
    candidate, url = iu.get_logo_path("NONEXISTENT", "No Corp")
    assert candidate.filename in (iu.PLACEHOLDER_LOGO, )
    assert candidate.public_url is not None
    assert iu.PLACEHOLDER_LOGO in url or iu.PLACEHOLDER_BANNER in url


# -------------------------
# S3-related behaviour (fsspec + boto3 mocks)
# -------------------------
def test_s3_lookup_with_fsspec(monkeypatch):
    monkeypatch.setattr(iu, "S3_PROCESSED_IMAGES_PATH", "s3://mybucket/prefix", raising=False)
    monkeypatch.setattr(iu, "CDN_BASE_URL", "https://cdn.example.com", raising=False)
    monkeypatch.setattr(iu, "_s3_exists_with_retry", lambda fs, uri, att=3, delay=0.2: True, raising=False)

    candidate, url = iu.get_logo_path("FOO", "Foo Corp")
    assert candidate.s3_uri is not None
    assert candidate.s3_uri.startswith("s3://")
    assert url.startswith("https://cdn.example.com")
    assert candidate.local_path is None


def test_s3_lookup_with_boto3(monkeypatch):
    monkeypatch.setattr(iu, "S3_PROCESSED_IMAGES_PATH", "s3://bkt", raising=False)
    monkeypatch.setattr(iu, "fsspec", None, raising=False)
    monkeypatch.setattr(iu, "CDN_BASE_URL", "https://cdn.cdn", raising=False)

    class FakeS3Client:
        def head_object(self, Bucket, Key):
            if Key.endswith("BAR_logo.png"):
                return {"ResponseMetadata": {"HTTPStatusCode": 200}}
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    monkeypatch.setattr(iu, "boto3", mock.Mock(client=lambda *a, **k: FakeS3Client()), raising=False)

    candidate, url = iu.get_logo_path("BAR", "Bar Corp")
    assert candidate.s3_uri is not None
    assert candidate.public_url is not None
    assert url.startswith("https://cdn.cdn")


def test_s3_preferred_over_local(monkeypatch):
    (iu.PROCESSED_LOGOS / "RELIANCE_logo.png").write_bytes(b"X")
    monkeypatch.setattr(iu, "S3_PROCESSED_IMAGES_PATH", "s3://prefer-bkt", raising=False)
    monkeypatch.setattr(iu, "CDN_BASE_URL", "https://cdn.pref", raising=False)
    monkeypatch.setattr(iu, "_s3_exists_with_retry", lambda fs, uri, att=3, delay=0.2: True, raising=False)

    candidate, url = iu.get_logo_path("RELIANCE", "Reliance")
    assert candidate.s3_uri is not None, "Expected S3 candidate when S3 exists"
    assert candidate.local_path is None
    assert "cdn.pref" in url


# -------------------------
# TTL cache behaviour (time-travel using mutable container)
# -------------------------
def test_ttl_cache_expiry(monkeypatch):
    monkeypatch.setattr(iu, "IMAGE_CACHE_TTL_SECONDS", 1, raising=False)
    t = [1000.0]

    def fake_time():
        return t[0]

    monkeypatch.setattr(iu, "time", mock.Mock(time=fake_time), raising=False)

    calls = {"n": 0}

    @iu.ttl_cache(ttl_seconds=1)
    def expensive(x):
        calls["n"] += 1
        return f"v{calls['n']}"

    a = expensive("A")
    assert calls["n"] == 1
    b = expensive("A")
    assert a == b
    assert calls["n"] == 1
    t[0] += 2.0
    c = expensive("A")
    assert calls["n"] == 2
    expensive.cache_clear()
    t[0] += 0.5
    d = expensive("A")
    assert calls["n"] == 3


# -------------------------
# Async wrappers
# -------------------------
@pytest.mark.asyncio
async def test_async_get_logo_path_offloads_thread(monkeypatch):
    iu._ensure_thread_pool(max_workers=2)
    main_thread = threading.get_ident()

    def blocking_get_logo(symbol, name):
        assert threading.get_ident() != main_thread, "blocking_get_logo ran on main thread"
        return iu.ImageCandidate(filename="X_logo.png", public_url="/x"), "/x"

    monkeypatch.setattr(iu, "get_logo_path", blocking_get_logo, raising=True)
    res = await iu.async_get_logo_path("X", "X Inc")
    assert isinstance(res, tuple)
    assert res[0].filename == "X_logo.png"


# -------------------------
# Cache wrapper: replace cached function with new cached wrapper around fake_uncached
# -------------------------
def test_find_first_existing_cache_and_clear(monkeypatch):
    cnt = {"n": 0}

    def fake_uncached(base_dir, candidates):
        cnt["n"] += 1
        return iu.ImageCandidate(
            filename=iu.PLACEHOLDER_LOGO,
            local_path=str(base_dir / iu.PLACEHOLDER_LOGO),
            public_url=f"{iu.URL_BASE}/{base_dir.name}/{iu.PLACEHOLDER_LOGO}",
        )

    # Build a fresh cached wrapper around our fake_uncached using the module's ttl_cache
    new_cached = iu.ttl_cache(iu.IMAGE_CACHE_TTL_SECONDS, iu.IMAGE_CACHE_MAXSIZE)(fake_uncached)

    # Replace module's cached function so find_first_existing uses our predictable wrapper
    monkeypatch.setattr(iu, "_find_first_existing_uncached", new_cached, raising=True)

    # First call -> underlying called once (MISS)
    r1 = iu.find_first_existing(iu.PROCESSED_LOGOS, ["X"])
    # Second call with same args -> should be cache HIT
    r2 = iu.find_first_existing(iu.PROCESSED_LOGOS, ["X"])
    assert cnt["n"] == 1, f"Underlying called {cnt['n']} times; expected 1 (cache hit)"

    # Clear cache and call again -> underlying should be called (MISS)
    assert hasattr(new_cached, "cache_clear")
    new_cached.cache_clear()
    r3 = iu.find_first_existing(iu.PROCESSED_LOGOS, ["X"])
    assert cnt["n"] == 2, f"Underlying called {cnt['n']} times; expected 2 after clear"


# -------------------------
# Thread pool shutdown idempotence
# -------------------------
def test_thread_pool_shutdown_idempotent():
    p1 = iu._ensure_thread_pool(max_workers=2)
    assert p1 is not None
    iu.shutdown_thread_pool(wait=True)
    iu.shutdown_thread_pool(wait=False)
    p2 = iu._ensure_thread_pool()
    assert p2 is not None
    assert p2 is not p1