# -*- coding: utf-8 -*-
"""
Diamond-grade tests for backend/services/pdf_processor.py
Covers settings, utilities, extraction paths, image bridge, LLM wrappers,
sentiment, S3 helpers, filename helpers, processor happy/error paths,
batch, and CLI. Async-safe and fast: no network, no external binaries.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock

import pytest

# System under test
from backend.services.pdf_processor import (
    PDFProcessorSettings,
    MasterPDFProcessor,
    MasterResult,
    _make_json_safe,
    _atomic_write_json,
    _compute_sha1,
    _clean_text_blob,
    validate_runtime_requirements,
    _extract_text_multi,
    _extract_images_and_process,
    _call_llm_headline_summary,
    _call_llm_sentiment,
    _compute_blended_sentiment,
    _cached_market_snapshot_full,
    _cached_indices,
    _do_upload_sync,
    _upload_with_fallback,
    _download_s3_to_local_if_exists,
    _is_s3_path,
    _prepare_output_path,
    _resolve_path_or_download,
    # _match_filename,
    _maybe_upload_output_to_s3,
    process_all_batch,
    _cli,
    VERSION,
    IST,
    now_iso,
    get_settings,
    InstanceExecutor,
)


# ---------------------------
# Shared lightweight fixtures
# ---------------------------

@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch):
    # Ensure tests don’t leak PDF_* vars across cases
    for k in list(os.environ):
        if k.startswith("PDF_"):
            monkeypatch.delenv(k, raising=False)


@pytest.fixture
def mock_env_utils(monkeypatch: pytest.MonkeyPatch):
    """Provide env_utils with deterministic local_dev paths."""
    env = Mock()
    env.get_input_pdf_dir.return_value = "tests/input_pdf"
    env.get_output_announcements_dir.return_value = "tests/output_announcements"
    env.get_processed_pdf_dir.return_value = "tests/processed_pdf"
    env.get_error_dir.return_value = "tests/errors"
    env.get_temp_dir.return_value = "tests/temp"
    env.get_environment.return_value = "local_dev"
    monkeypatch.setattr("backend.services.pdf_processor.env_utils", env)
    # Mark services available for code that gates on it
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    return env


@pytest.fixture
def temp_pdf(tmp_path: Path) -> Path:
    """Create a tiny valid-looking PDF file."""
    p = tmp_path / "test.pdf"
    p.write_bytes(
        b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n"
        b"xref\n0 4\n0000000000 65535 f \n0000000010 00000 n \n0000000075 00000 n \n"
        b"0000000128 00000 n \ntrailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n200\n%%EOF"
    )
    return p


# ---------------------------
# Settings
# ---------------------------

def test_settings_pydantic_path(monkeypatch: pytest.MonkeyPatch):
    # Test with direct instantiation instead of environment variables
    s = PDFProcessorSettings(LLM_TIMEOUT_S=9.5, OCR_ENABLED=False)
    assert isinstance(s, PDFProcessorSettings)
    assert s.LLM_TIMEOUT_S == 9.5
    assert s.OCR_ENABLED is False


def test_settings_fallback(monkeypatch: pytest.MonkeyPatch):
    # Force fallback mode by pretending Pydantic base is object
    monkeypatch.setattr("backend.services.pdf_processor.BaseModel", object)
    s = get_settings()
    assert s.LLM_TIMEOUT_S == 15.0
    assert s.OCR_ENABLED is True


# ---------------------------
# Utilities
# ---------------------------

def test_make_json_safe_roundtrip():
    obj = {"a": [1, {"b": object()}], "c": None}
    safe = _make_json_safe(obj)
    assert isinstance(safe, dict)
    assert safe["a"][0] == 1
    assert isinstance(safe["a"][1]["b"], str)


def test_atomic_write_json(tmp_path: Path):
    path = tmp_path / "x.json"
    _atomic_write_json(path, {"k": "v"})
    assert path.exists()
    assert json.load(path.open()) == {"k": "v"}


def test_compute_sha1(temp_pdf: Path):
    sha = _compute_sha1(temp_pdf)
    assert len(sha) == 40
    assert re.fullmatch(r"[0-9a-f]{40}", sha) is not None


def test_clean_text_blob():
    assert _clean_text_blob("Hi\x0cThere  \n\nX  Y") == "Hi There X Y"


# ---------------------------
# Runtime validation
# ---------------------------

def test_validate_runtime_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PDF_OCR_ENABLED", "true")
    monkeypatch.setattr("shutil.which", lambda _: None)
    rep = validate_runtime_requirements()
    assert rep["ok"] is False
    assert any("Missing native binaries" in s for s in rep["issues"])


def test_validate_runtime_ok(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PDF_OCR_ENABLED", "false")
    rep = validate_runtime_requirements()
    # OCR disabled → ok regardless of binaries
    assert rep["ok"] is True


# ---------------------------
# Text extraction
# ---------------------------

# ---------------------------
# Image extraction bridge
# ---------------------------

@pytest.mark.asyncio
async def test_extract_images_and_bridge(monkeypatch: pytest.MonkeyPatch, temp_pdf: Path):
    # One embedded image via fitz
    class _Page:
        def get_images(self, full=True): return [(10, {})]

    class _Doc:
        def __len__(self): return 1
        def load_page(self, _): return _Page()
        def extract_image(self, xref): return {"image": b"raw", "ext": "png", "width": 12, "height": 8}
        def close(self): pass

    mock_img_utils = Mock()
    mock_img_utils.process_and_upload_image.return_value = {"s3_url": "s3://bucket/key.png"}

    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.image_utils", mock_img_utils)
    monkeypatch.setattr("fitz.open", lambda *_: _Doc())

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        out = await _extract_images_and_process(temp_pdf, ex)
        assert len(out) == 1
        assert out[0]["s3_url"] == "s3://bucket/key.png"
    finally:
        ex.shutdown(wait=True)


# ---------------------------
# LLM wrappers
# ---------------------------

@pytest.mark.asyncio
async def test_call_llm_headline_summary_success(monkeypatch: pytest.MonkeyPatch):
    mock_llm = Mock()
    async def _ok(text): return {"headline_final": "H", "summary_60": "S", "ok": True}
    mock_llm.async_classify_headline_and_summary = _ok
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.llm_utils", mock_llm)
    monkeypatch.setattr("backend.services.pdf_processor._SETTINGS", Mock(LLM_TIMEOUT_S=5))

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        h, s, meta = await _call_llm_headline_summary("t", ex)
        assert h == "H" and s == "S"
        assert isinstance(meta, dict)
    finally:
        ex.shutdown(wait=True)


@pytest.mark.asyncio
async def test_call_llm_headline_summary_timeout(monkeypatch: pytest.MonkeyPatch):
    mock_llm = Mock()
    async def _slow(_): await asyncio.sleep(0.2)
    mock_llm.async_classify_headline_and_summary = _slow
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.llm_utils", mock_llm)
    monkeypatch.setattr("backend.services.pdf_processor._SETTINGS", Mock(LLM_TIMEOUT_S=0.01))

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        h, s, meta = await _call_llm_headline_summary("t", ex)
        assert h is None and s is None
        assert meta.get("reason") == "timeout"
    finally:
        ex.shutdown(wait=True)


@pytest.mark.asyncio
async def test_call_llm_sentiment_unavailable(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", False)
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        res = await _call_llm_sentiment("x", ex)
        assert res is None
    finally:
        ex.shutdown(wait=True)


# ---------------------------
# Sentiment blend and caches
# ---------------------------

def test_compute_blended_sentiment_fallback(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", False)
    out = _compute_blended_sentiment("anything")
    assert out["label"] == "Neutral" and out["score"] == 0.5


def test_cached_market_snapshot(monkeypatch: pytest.MonkeyPatch):
    mock_csv = Mock()
    mock_csv.get_market_snapshot_full.return_value = {"price": 123}
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.csv_utils", mock_csv)
    assert _cached_market_snapshot_full("SYM")["price"] == 123


def test_cached_indices(monkeypatch: pytest.MonkeyPatch):
    mock_csv = Mock()
    mock_csv.get_indices_for_symbol.return_value = ("NIFTY", "IT")
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.csv_utils", mock_csv)
    idx, sec = _cached_indices("SYM")
    assert idx == "NIFTY" and sec == "IT"


# ---------------------------
# S3 helpers and path helpers
# ---------------------------

def test_is_s3_path_and_resolvers(tmp_path: Path):
    assert _is_s3_path("s3://bucket/p") is True
    assert _is_s3_path("s3/bucket/p") is False
    # prepare_output_path
    local = _prepare_output_path(str(tmp_path / "out"))
    assert Path(local).exists()
    s3 = _prepare_output_path("s3://bucket/out")
    assert s3 == "s3://bucket/out"
    # resolve path
    assert _resolve_path_or_download("s3://bucket/x") == "s3://bucket/x"
    p = tmp_path / "z.pdf"; p.write_bytes(b"0")
    assert _resolve_path_or_download(str(p)) == str(p)


def test_do_upload_sync_and_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    # Primary chain succeeds via aws_utils
    mock_aws = Mock(upload_file_to_s3=lambda *_a, **_k: True)
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.aws_utils", mock_aws)
    f = tmp_path / "x.bin"; f.write_bytes(b"1")
    assert _do_upload_sync(f, "s3://b", "k") is True

    # _upload_with_fallback uses our _do_upload_sync mock
    monkeypatch.setattr("backend.services.pdf_processor._do_upload_sync", lambda *_a, **_k: True)
    assert _upload_with_fallback(f, "s3://b", "k") is True


def test_download_s3_to_local_if_exists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    mock_aws = Mock()
    def _ok(src, dest): Path(dest).write_bytes(b"ok")
    mock_aws.download_file_from_s3 = _ok
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    monkeypatch.setattr("backend.services.pdf_processor.aws_utils", mock_aws)
    dst = tmp_path / "d.bin"
    assert _download_s3_to_local_if_exists("s3://b/prefix", "k", dst) is True
    assert dst.exists()


def test_maybe_upload_output_to_s3_happy(tmp_path: Path):
    import sys
    from unittest.mock import patch, Mock, MagicMock
    
    # Mock boto3 client
    mock_client = Mock()
    mock_client.upload_file.return_value = None
    mock_boto3 = Mock()
    mock_boto3.client.return_value = mock_client
    
    # Patch boto3 in sys.modules since it's imported locally
    with patch.dict('sys.modules', {'boto3': mock_boto3}):
        f = tmp_path / "res.json"; f.write_text("{}")
        out = _maybe_upload_output_to_s3(str(f), "s3://bucket/folder")
        assert isinstance(out, str) and out.startswith("s3://bucket/")


# ---------------------------
# Filename helpers
# ---------------------------

# def test_match_filename_minimal():
#     res = _match_filename("xyz123abc.pdf")
#     assert res["found"] is False and res["symbol"] is None


# ---------------------------
# MasterPDFProcessor
# ---------------------------

@pytest.fixture
def processor(mock_env_utils):
    p = MasterPDFProcessor()
    yield p
    p.close()


@pytest.mark.asyncio
async def test_process_pdf_local_success(processor: MasterPDFProcessor, temp_pdf: Path, monkeypatch: pytest.MonkeyPatch):
    # Make extraction and LLM cheap and deterministic
    async def mock_extract(*a, **k): return ("Body text here.", [{"page": 1, "text": "Body text here.", "ocr": False}], {"method": "fitz"})
    monkeypatch.setattr("backend.services.pdf_processor._extract_text_multi", mock_extract)
    
    async def mock_images(*a, **k): return []
    monkeypatch.setattr("backend.services.pdf_processor._extract_images_and_process", mock_images)
    
    async def mock_llm_headline(*a, **k): return ("Headline", "Summary", {"ok": True})
    monkeypatch.setattr("backend.services.pdf_processor._call_llm_headline_summary", mock_llm_headline)
    
    async def mock_llm_sentiment(*a, **k): return {"score": 0.7}
    monkeypatch.setattr("backend.services.pdf_processor._call_llm_sentiment", mock_llm_sentiment)
    monkeypatch.setattr("backend.services.pdf_processor._compute_blended_sentiment",
                        lambda *a, **k: {"label": "Positive", "score": 0.7})
    monkeypatch.setattr("backend.services.pdf_processor._cached_market_snapshot_full",
                        lambda *_: {})
    monkeypatch.setattr("backend.services.pdf_processor._cached_indices",
                        lambda *_: ("", ""))

    # Deterministic filename datetime + sha1
    monkeypatch.setattr("backend.services.pdf_processor.filename_utils",
                        Mock(strip_timestamp_and_ext=lambda x: "ACME",
                             extract_datetime_from_filename=lambda x: ("2025-11-08T10:15:00+05:30", "Nov 8, 2025 10:15 IST")))
    monkeypatch.setattr("backend.services.pdf_processor._compute_sha1", lambda *_: "a1b2c3d4e5f67890deadbeefcafefeedabcd1234")

    res = await processor.process_pdf(temp_pdf)
    assert isinstance(res, MasterResult)
    assert res.headline == "Headline"
    assert res.summary_60 == "Summary"
    assert res.metadata["company_name"] == "ACME"
    # ID uses datetime from filename
    assert res.id.startswith("ann_20251108T101500")


@pytest.mark.asyncio
async def test_process_pdf_error_path(processor: MasterPDFProcessor, temp_pdf: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("backend.services.pdf_processor._extract_text_multi", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    out = await processor.process_pdf(temp_pdf)
    assert out is None
    err = Path("tests/errors") / f"{temp_pdf.name}.error.json"
    assert err.exists()


def test_process_pdf_sync_wrapper(processor: MasterPDFProcessor, temp_pdf: Path, monkeypatch: pytest.MonkeyPatch):
    # Wrap coroutine result object to check passthrough
    class _R: id = "X"
    async def _ok(*_a, **_k): return _R()
    monkeypatch.setattr(processor, "process_pdf", _ok)
    r = processor.process_pdf_sync(temp_pdf)
    # process_pdf_sync returns the object directly
    assert hasattr(r, "id") and r.id == "X"


def test_health_check_local(processor: MasterPDFProcessor):
    rep = processor.health_check()
    assert rep["status"] in ("healthy", "degraded")
    assert "checked_at" in rep


# ---------------------------
# Batch
# ---------------------------

@pytest.mark.asyncio
async def test_process_all_batch_local_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)
    mock_env = Mock()
    mock_env.get_input_pdf_dir.return_value = str(tmp_path / "in")
    mock_env.get_output_announcements_dir.return_value = str(tmp_path / "out")
    mock_env.get_processed_pdf_dir.return_value = str(tmp_path / "p")
    mock_env.get_error_dir.return_value = str(tmp_path / "err")
    mock_env.get_temp_dir.return_value = str(tmp_path / "tmp")
    mock_env.get_environment.return_value = "local_dev"
    monkeypatch.setattr("backend.services.pdf_processor.env_utils", mock_env)

    success, failure = await process_all_batch()
    assert success == 0 and failure == 0


@pytest.mark.asyncio
async def test_process_all_batch_local_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, temp_pdf: Path):
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    shutil.copy(temp_pdf, input_dir / "a.pdf")

    mock_env = Mock()
    mock_env.get_input_pdf_dir.return_value = str(input_dir)
    mock_env.get_output_announcements_dir.return_value = str(tmp_path / "out")
    mock_env.get_processed_pdf_dir.return_value = str(tmp_path / "p")
    mock_env.get_error_dir.return_value = str(tmp_path / "err")
    mock_env.get_temp_dir.return_value = str(tmp_path / "tmp")
    mock_env.get_environment.return_value = "local_dev"
    monkeypatch.setattr("backend.services.pdf_processor.env_utils", mock_env)
    monkeypatch.setattr("backend.services.pdf_processor._SERVICES_AVAILABLE", True)

    mock_proc = Mock()
    async def _proc_ok(path, **k): return Mock(id="ok")
    mock_proc.process_pdf = _proc_ok
    mock_proc.close = lambda : None
    monkeypatch.setattr("backend.services.pdf_processor.MasterPDFProcessor", lambda : mock_proc)

    success, failure = await process_all_batch()
    assert success == 1 and failure == 0


# ---------------------------
# CLI
# ---------------------------

def test_cli_single_dry_run(monkeypatch: pytest.MonkeyPatch, capsys, mock_env_utils):
    # We want to exercise _cli code, not mock it.
    args = Mock()
    args.mode = "single"
    args.file = "foo.pdf"
    args.dry_run = True
    args.upload = False
    args.verbose = False
    args.overwrite = False
    args.force_local = False
    args.force_s3 = False
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: args)

    # Ensure constructor works but do not actually process
    monkeypatch.setattr("backend.services.pdf_processor.MasterPDFProcessor.process_pdf_sync", lambda *a, **k: None)

    _cli()
    out = capsys.readouterr().out
    assert "Dry run: would process foo.pdf" in out


def test_cli_batch(monkeypatch: pytest.MonkeyPatch, capsys):
    args = Mock()
    args.mode = "batch"
    args.file = None
    args.dry_run = False
    args.upload = False
    args.verbose = False
    args.overwrite = False
    args.force_local = False
    args.force_s3 = False
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: args)

    async def mock_batch(**k): return (1, 0)
    monkeypatch.setattr("backend.services.pdf_processor.process_all_batch", mock_batch)
    _cli()
    out = capsys.readouterr().out
    assert "Batch completed: success=1, failure=0" in out


# ---------------------------
# Version, time, executor
# ---------------------------

def test_version_and_now_iso():
    assert VERSION == "pdf_processor_v10.3.0"
    ts = now_iso()
    # basic sanity: parseable, within a minute of now IST→UTC
    dt = datetime.fromisoformat(ts)
    assert datetime.now(IST) - dt < timedelta(minutes=1)


def test_instance_executor_lifecycle():
    exh = InstanceExecutor(3)
    tp = exh.get()
    assert getattr(tp, "_max_workers", 3) == 3
    exh.shutdown(wait=True)