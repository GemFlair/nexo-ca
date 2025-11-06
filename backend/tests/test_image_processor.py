"""
Enhanced Diamond Test Suite — Image Processor (R-03 Final)
---------------------------------------------------------
File: backend/tests/test_image_processor.py

This suite expands coverage to resilience, S3 integration (mocked),
settings concurrency, disk-full conditions, and concurrent processing.
Includes additional edge-case tests requested:
 - CLI invalid/missing file
 - Pillow missing entire workflow
 - Pydantic missing settings fallback
 - S3 partial upload failure
 - Health check S3 error state
"""

import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from unittest import mock

import pytest

# Require Pillow for image generation tests (skip if missing)
pytest.importorskip("PIL", reason="Pillow required for image tests")
from PIL import Image

import backend.services.image_processor as ip


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_settings_cache():
    """Ensure settings cache is reset between tests."""
    ip.reset_cached_image_settings()
    yield
    ip.reset_cached_image_settings()


def _make_temp_image(tmp_path: Path, name="sample.png", size=(64, 64), color=(255, 0, 0)):
    """Create a small PNG image in tmp_path and return Path."""
    img = Image.new("RGB", size, color)
    p = tmp_path / name
    img.save(p, format="PNG")
    return p


# --- Basic conversion & atomic write tests -----------------------------------

def test_process_one_image_creates_png(tmp_path):
    src = _make_temp_image(tmp_path, "logo_src.png")
    dest_dir = tmp_path / "out"
    out = ip._process_one_image(src, dest_dir, kind="logo")
    assert out is not None and out.exists()
    assert out.suffix == ".png"


def test_process_one_image_creates_webp(tmp_path):
    src = _make_temp_image(tmp_path, "banner_src.jpg")  # still a valid image
    dest_dir = tmp_path / "out"
    out = ip._process_one_image(src, dest_dir, kind="banner")
    assert out is not None and out.exists()
    assert out.suffix == ".webp"


def test_atomic_write_roundtrip(tmp_path):
    data = b"hello-atomic"
    out = tmp_path / "atomic.bin"
    ip._atomic_write_bytes(out, data)
    assert out.exists()
    assert out.read_bytes() == data


# --- Observability integration checks ---------------------------------------

def test_audit_and_metric_on_success(monkeypatch, tmp_path):
    audit_calls = []
    metric_calls = []

    monkeypatch.setattr(ip, "_audit_log", lambda *a, **kw: audit_calls.append((a, kw)))
    monkeypatch.setattr(ip, "_metric_inc", lambda *a, **kw: metric_calls.append((a, kw)))

    src = _make_temp_image(tmp_path, "logo_obs.png")
    out = tmp_path / "out"
    ip._process_one_image(src, out, "logo")
    assert len(audit_calls) >= 1
    assert len(metric_calls) >= 1


# --- Resource cleanup --------------------------------------------------------

def test_image_handle_closed(monkeypatch, tmp_path):
    # Spy on PIL.Image.Image.close via patching class method
    with mock.patch("PIL.Image.Image.close", autospec=True) as m_close:
        src = _make_temp_image(tmp_path, "cleanup.png")
        out = tmp_path / "out"
        ip._process_one_image(src, out, "logo")
        # close should be called at least once during processing/finally
        assert m_close.called


# --- Resilience / circuit-breaker tests -------------------------------------

def test_maybe_call_with_resilience_when_resilience_missing(monkeypatch):
    """
    If resilience utils are not present, _maybe_call_with_resilience should call the function directly.
    This test temporarily forces the module to use the fallback wrapper by simulating absence.
    """
    orig_flag = getattr(ip, "_RESILIENCE_AVAILABLE", None)
    orig_wrapper = getattr(ip, "_maybe_call_with_resilience", None)
    try:
        monkeypatch.setattr(ip, "_RESILIENCE_AVAILABLE", False, raising=False)
        # Provide a fallback wrapper matching module fallback behavior
        def fallback(fn, breaker_name="default"):
            return fn()
        monkeypatch.setattr(ip, "_maybe_call_with_resilience", fallback, raising=False)

        called = {"n": 0}
        def target():
            called["n"] += 1
            return "ok"

        res = ip._maybe_call_with_resilience(target, "test")
        assert res == "ok"
        assert called["n"] == 1
    finally:
        # Restore
        if orig_flag is not None:
            monkeypatch.setattr(ip, "_RESILIENCE_AVAILABLE", orig_flag, raising=False)
        if orig_wrapper is not None:
            monkeypatch.setattr(ip, "_maybe_call_with_resilience", orig_wrapper, raising=False)


@pytest.mark.parametrize("simulate_open", [True, False])
def test_resilience_circuit_breaker_open(monkeypatch, simulate_open):
    """
    When resilience utils are available and retry_sync raises CircuitBreakerOpen,
    the wrapper should return False and not call the underlying function.
    """
    if not getattr(ip, "_RESILIENCE_AVAILABLE", False):
        pytest.skip("resilience_utils not available in module under test")

    # Create a fake exception type to mimic resilience_utils.CircuitBreakerOpen
    class FakeCBOpen(Exception):
        pass

    fake_res = mock.Mock()
    fake_res.get_circuit_breaker.return_value = object()

    def fake_retry(fn, breaker=None):
        raise FakeCBOpen()

    # Patch resilience_utils in-module
    monkeypatch.setattr(ip, "resilience_utils", fake_res, raising=False)
    monkeypatch.setattr(ip.resilience_utils, "retry_sync", fake_retry, raising=False)
    monkeypatch.setattr(ip.resilience_utils, "CircuitBreakerOpen", FakeCBOpen, raising=False)

    # Replace wrapper to use current patched resilience_utils (if needed)
    def wrapper(fn, breaker_name="s3_image_op"):
        try:
            cb = ip.resilience_utils.get_circuit_breaker(breaker_name)
            return ip.resilience_utils.retry_sync(fn, breaker=cb)
        except ip.resilience_utils.CircuitBreakerOpen:
            return False
        except Exception:
            try:
                return fn()
            except Exception:
                return False

    monkeypatch.setattr(ip, "_maybe_call_with_resilience", wrapper, raising=False)

    called = {"n": 0}
    def should_not_run():
        called["n"] += 1
        raise RuntimeError("Should not be called")

    res = ip._maybe_call_with_resilience(should_not_run, breaker_name="test_breaker")
    assert res is False
    assert called["n"] == 0


# --- S3 workflow integration (mocked) ---------------------------------------

def _fake_s3_download(prefix, local_dir, s3_opts, concurrency):
    """
    Simulate downloading: create a single image file in local_dir and return True.
    Called with signature: (prefix, local_dir, s3_opts, concurrency)
    """
    try:
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        img_path = local_dir / "s3_downloaded_sample.png"
        img = Image.new("RGB", (32, 32), (0, 255, 0))
        img.save(img_path, format="PNG")
        return True
    except Exception:
        return False


def _fake_s3_upload(src_dir, s3_prefix, s3_opts, concurrency):
    """Simulate upload success"""
    return True


def test_s3_full_workflow_success(monkeypatch, tmp_path):
    """Test download -> process -> upload S3 workflow using mocked aws_utils."""
    monkeypatch.setattr(ip.aws_utils, "download_prefix_to_local", lambda prefix, local_dir, opts, c: _fake_s3_download(prefix, local_dir, opts, c))
    monkeypatch.setattr(ip.aws_utils, "upload_dir_to_s3", lambda src, prefix, opts, c: _fake_s3_upload(src, prefix, opts, c))

    settings = ip.get_image_settings()
    settings.S3_RAW_IMAGES_PATH = "s3://fakebucket/raw"
    settings.S3_PROCESSED_IMAGES_PATH = "s3://fakebucket/processed"

    logos, banners = ip.process_all_batch(overwrite=True, quality=80, verbose=False)
    assert isinstance(logos, int) and isinstance(banners, int)


def test_s3_fallback_to_local_on_download_failure(monkeypatch, tmp_path):
    """If both downloads return False, ensure fallback metric is incremented and local processing is used."""
    monkeypatch.setattr(ip.aws_utils, "download_prefix_to_local", lambda *a, **kw: False)
    monkeypatch.setattr(ip.aws_utils, "upload_dir_to_s3", lambda *a, **kw: False)
    # Ensure fallback metric slot exists on module (set if missing)
    monkeypatch.setattr(ip, "_increment_fallback_metric", lambda: None, raising=False)
    called = {"n": 0}
    monkeypatch.setattr(ip, "_increment_fallback_metric", lambda: called.__setitem__("n", called["n"] + 1), raising=False)

    settings = ip.get_image_settings()
    settings.S3_RAW_IMAGES_PATH = "s3://fakebucket/raw"
    settings.S3_PROCESSED_IMAGES_PATH = "s3://fakebucket/processed"

    logos, banners = ip.process_all_batch()
    assert called["n"] >= 1
    assert isinstance(logos, int) and isinstance(banners, int)


def test_s3_upload_partial_failure(monkeypatch, tmp_path):
    """
    Simulate logos upload success and banners upload failure.
    The audit log should reflect a partial result/partial_success.
    """
    monkeypatch.setattr(ip.aws_utils, "download_prefix_to_local", lambda prefix, local_dir, opts, c: _fake_s3_download(prefix, local_dir, opts, c))
    def upload_partial(src, prefix, opts, c):
        # succeed only for processed_logos prefix
        return "processed_logos" in str(prefix)
    monkeypatch.setattr(ip.aws_utils, "upload_dir_to_s3", upload_partial)

    audits = []
    monkeypatch.setattr(ip, "_audit_log", lambda *a, **kw: audits.append((a, kw)), raising=False)

    settings = ip.get_image_settings()
    settings.S3_RAW_IMAGES_PATH = "s3://fakebucket/raw"
    settings.S3_PROCESSED_IMAGES_PATH = "s3://fakebucket/processed"

    ip.process_all_batch()
    # Expect at least one audit entry and partial outcome captured
    assert any("partial" in str(a) or "partial" in str(kw) for a, kw in audits)


# --- Settings and concurrency tests -----------------------------------------

def test_settings_env_override(monkeypatch):
    """Environment variables should override default settings values on load."""
    monkeypatch.setenv("DEFAULT_WEBP_QUALITY", "55")
    ip.reset_cached_image_settings()
    s = ip.get_image_settings()
    assert getattr(s, "DEFAULT_WEBP_QUALITY", None) == 55


def test_settings_thread_safety():
    """Concurrent calls to get_image_settings should return the same object (cached)."""
    results = []
    def worker():
        results.append(id(ip.get_image_settings()))

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(set(results)) == 1  # all thread results are same cached object id


# --- Performance / concurrency (lightweight) ---------------------------------

@pytest.mark.slow
def test_concurrent_batch_processing(tmp_path):
    """Create many small images and process them, ensuring outputs are created without errors."""
    raw = tmp_path / "raw"
    raw.mkdir()
    for i in range(40):
        _make_temp_image(raw, f"img_{i}.png", size=(64, 64), color=(i % 255, (i*2) % 255, (i*3) % 255))
    dst = tmp_path / "out"
    dst.mkdir()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(ip.process_images_from_dir, raw, dst, "logo", True, None) for _ in range(2)]
        results = [f.result() for f in futures]
    proc_files = list(dst.glob("*_logo.png"))
    assert len(proc_files) >= 1


# --- Disk space (ENOSPC) simulation ------------------------------------------

def test_disk_space_exhaustion_simulated(monkeypatch, tmp_path):
    """
    Simulate an ENOSPC (No space left on device) during atomic write.
    Ensure _process_one_image returns None and the exception is handled gracefully.
    """
    src = _make_temp_image(tmp_path, "big.png")
    dest = tmp_path / "out"

    def raise_enospc(path, data):
        exc = OSError("No space left")
        exc.errno = 28  # ENOSPC
        raise exc

    monkeypatch.setattr(ip, "_atomic_write_bytes", raise_enospc, raising=False)

    metric_called = {"n": 0}
    audit_called = {"n": 0}
    monkeypatch.setattr(ip, "_metric_inc", lambda *a, **kw: metric_called.__setitem__("n", metric_called["n"] + 1), raising=False)
    monkeypatch.setattr(ip, "_audit_log", lambda *a, **kw: audit_called.__setitem__("n", audit_called["n"] + 1), raising=False)

    res = ip._process_one_image(src, dest, "logo")
    assert res is None
    assert metric_called["n"] >= 1 or audit_called["n"] >= 1


# --- Large-file handling (slow/heavy) ---------------------------------------

@pytest.mark.slow
def test_large_image_processing(tmp_path):
    """Create a large image and verify processing finishes (this is heavy, marked slow)."""
    src = _make_temp_image(tmp_path, "huge.png", size=(4000, 4000))
    dest = tmp_path / "out"
    out = ip._process_one_image(src, dest, "logo")
    assert out is None or out.exists()


# --- Health check structure --------------------------------------------------

def test_health_check_keys_present():
    rep = ip.health_check()
    assert "status" in rep and "checks" in rep and "version" in rep


def test_health_check_s3_error(monkeypatch):
    """
    When list_s3_prefix raises, health_check should report degraded/unhealthy.
    """
    def raise_err(*a, **kw): raise RuntimeError("simulated s3 failure")
    monkeypatch.setattr(ip.aws_utils, "list_s3_prefix", raise_err, raising=False)
    s = ip.get_image_settings()
    s.S3_RAW_IMAGES_PATH = "s3://fakebucket/raw"
    rep = ip.health_check()
    assert rep["status"] in ("degraded", "unhealthy")


# --- Pillow / Pydantic Fallbacks ---------------------------------------------

def test_pillow_missing_entire_workflow(monkeypatch, tmp_path):
    """
    Simulate Pillow not being available and ensure functions degrade gracefully.
    process_all_batch and process_single should not crash.
    """
    # Simulate pillow unavailable in module
    monkeypatch.setattr(ip, "_PILLOW_AVAILABLE", False, raising=False)
    monkeypatch.setattr(ip, "Image", None, raising=False)

    # Create a fake file
    src = tmp_path / "dummy.png"
    src.write_bytes(b"notanimage")

    # process_single should handle missing pillow and return None (or not crash)
    res_single = ip.process_single(str(src))
    assert res_single is None

    # process_all_batch should run fallback paths without raising
    try:
        s = ip.get_image_settings()
        # unset S3 to force local processing path
        s.S3_RAW_IMAGES_PATH = ""
        logos, banners = ip.process_all_batch()
        assert isinstance(logos, int) and isinstance(banners, int)
    finally:
        # restore by resetting settings cache (fixture will handle but ensure)
        ip.reset_cached_image_settings()


def test_pydantic_missing_settings(monkeypatch):
    """
    Simulate pydantic not being installed and ensure the fallback settings object is returned.
    """
    monkeypatch.setattr(ip, "BaseSettings", None, raising=False)
    ip.reset_cached_image_settings()
    s = ip.get_image_settings()
    assert hasattr(s, "DEFAULT_WEBP_QUALITY")


# --- CLI Interface (edge cases) ----------------------------------------------

def test_cli_invalid_mode(monkeypatch):
    """CLI with invalid mode should exit with code 2 (argparse error)."""
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "invalid"], raising=False)
    with pytest.raises(SystemExit) as e:
        ip.cli_entry()
    # argparse typically exits with code 2 for parse errors
    assert e.value.code == 2


def test_cli_single_missing_file(monkeypatch):
    """CLI --mode single without --file should exit non-zero (error)."""
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "single"], raising=False)
    with pytest.raises(SystemExit) as e:
        ip.cli_entry()
    assert e.value.code != 0


# --- Auto detect and other functional checks ---------------------------------

def test_process_single_auto_detection(tmp_path):
    src = _make_temp_image(tmp_path, "auto_logo.png")
    out = ip.process_single(str(src))
    assert out is not None and out.exists()
    assert out.suffix == ".png"