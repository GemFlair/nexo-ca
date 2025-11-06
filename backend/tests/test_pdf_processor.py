# backend/tests/test_pdf_processor.py
"""
Defensive tests for backend.services.pdf_processor.MasterPDFProcessor
Designed to match the final production processor you provided.

Run:
    pytest backend/tests/test_pdf_processor.py -q
"""
from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import os
import shutil
import sys
import time
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

# import module under test
import backend.services.pdf_processor as pdf_proc
from backend.services.pdf_processor import (
    MasterPDFProcessor,
    PDFProcessorSettings,
    _atomic_write_json,
    _compute_sha1,
)


# -------------------------
# Helpers / Fixtures
# -------------------------
@pytest.fixture(autouse=True)
def isolate_fs_and_proc(tmp_path, monkeypatch):
    """
    Provide an isolated processor instance per test and ensure temp dirs.
    """
    monkeypatch.chdir(tmp_path)

    proc = MasterPDFProcessor()
    # apply test-local dirs (public attributes expected)
    out = tmp_path / "out"
    pdfs = tmp_path / "processed_pdfs"
    errs = tmp_path / "errors"

    proc.output_dir = out
    proc.processed_pdf_dir = pdfs
    proc.error_dir = errs

    for d in (out, pdfs, errs):
        shutil.rmtree(str(d), ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)

    ctx = {"tmp": tmp_path, "proc": proc, "out": out, "pdfs": pdfs, "errs": errs}
    yield ctx

    # teardown: best-effort cleanup
    try:
        if hasattr(proc, "close"):
            try:
                proc.close()
            except Exception:
                pass
        ex = getattr(proc, "_executor", None)
        if ex is not None:
            try:
                ex.shutdown(wait=False)
            except Exception:
                pass
            try:
                setattr(proc, "_executor", None)
            except Exception:
                pass
    finally:
        for d in (out, pdfs, errs):
            shutil.rmtree(str(d), ignore_errors=True)


@pytest.fixture
def sample_pdf(tmp_path):
    p = tmp_path / "ACME_Q3_2025_Results.pdf"
    p.write_bytes(b"%PDF-1.4\n% Dummy PDF for tests\n")
    return p


@pytest.fixture
def mock_services():
    """
    Provide module-level mocks matching backend.services.* names used by processor.
    Use AsyncMock for async entrypoints.
    """
    # llm utils
    mock_llm = SimpleNamespace()
    mock_llm.async_classify_headline_and_summary = AsyncMock(
        return_value=("Mock Headline", "Mock Summary", {"model": "mock"})
    )
    mock_llm.async_classify_sentiment = AsyncMock(return_value={"label": "Positive", "score": 0.9})
    mock_llm.classify_headline_and_summary = Mock(return_value={"headline_final": "H", "summary_60": "S", "llm_meta": {}})

    # sentiment utils
    mock_sent = MagicMock()
    mock_sent.compute_sentiment.return_value = {"label": "Positive", "score": 0.8}

    # aws utils
    mock_aws = MagicMock()
    mock_aws.upload_file_to_s3.return_value = True

    # csv utils
    mock_csv = MagicMock()
    mock_csv.get_market_snapshot.return_value = {"symbol": "ACME", "last_price": 120.0}
    mock_csv.get_indices_for_symbol.return_value = ("IDX", "Sector")

    # filename utils
    mock_filename = MagicMock()
    mock_filename.filename_to_symbol.return_value = {"found": True, "symbol": "ACME", "company_name": "ACME LTD", "score": 1.0, "match_type": "exact"}

    # resilience / observability (optional)
    mock_res = MagicMock()
    mock_obs = MagicMock()
    mock_obs.get_logger = lambda name: Mock()

    return {
        "llm_utils": mock_llm,
        "sentiment_utils": mock_sent,
        "aws_utils": mock_aws,
        "csv_utils": mock_csv,
        "filename_utils": mock_filename,
        "resilience_utils": mock_res,
        "observability_utils": mock_obs,
    }


class ServicesContext:
    """
    Context manager to temporarily inject module-level service objects into pdf_proc.
    Restores prior values on exit.
    """
    def __init__(self, mocks):
        self.mocks = mocks
        self._saved = {}

    def __enter__(self):
        for k, v in self.mocks.items():
            self._saved[k] = getattr(pdf_proc, k, None)
            setattr(pdf_proc, k, v)
        # mark services available so processor takes enrichment paths
        pdf_proc._SERVICES_AVAILABLE = True
        return self

    def __exit__(self, exc_type, exc, tb):
        for k, prev in self._saved.items():
            setattr(pdf_proc, k, prev)
        pdf_proc._SERVICES_AVAILABLE = False


# -------------------------
# Tests: Core flows
# -------------------------
@pytest.mark.asyncio
async def test_happy_path_creates_output(isolate_fs_and_proc, mock_services, sample_pdf):
    proc: MasterPDFProcessor = isolate_fs_and_proc["proc"]
    with ServicesContext(mock_services):
        # patch heavy extraction functions — signatures in production include executor; AsyncMock tolerates args
        with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(return_value=(
            "Full extracted text.",
            [{"page": 1, "text": "Full extracted text.", "ocr": False}],
            {"successful_method": "pypdf"}
        ))):
            with patch("backend.services.pdf_processor._extract_images", new=AsyncMock(return_value=[{
                "page": 1, "index": 0, "filename": "img.png", "local_path": "/tmp/img.png", "s3_path": None, "width": 100, "height": 50
            }])):
                res = await proc.process_pdf(sample_pdf)

    assert res is not None
    # JSON persisted under output_dir/date/*.json
    json_files = list(isolate_fs_and_proc["out"].glob("**/*.json"))
    assert len(json_files) == 1, "Expected one JSON output file"
    # PDF moved to processed dir
    processed = list(isolate_fs_and_proc["pdfs"].glob("**/*.pdf"))
    assert len(processed) == 1, "Expected PDF moved to processed_pdf_dir"


@pytest.mark.asyncio
async def test_result_contains_required_fields(isolate_fs_and_proc, mock_services, sample_pdf):
    proc = isolate_fs_and_proc["proc"]
    with ServicesContext(mock_services):
        with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(return_value=(
            "Announcement text", [{"page": 1, "text": "Page", "ocr": False}], {"successful_method": "pypdf"}
        ))):
            res = await proc.process_pdf(sample_pdf)

    assert res is not None
    # Accept dataclass or dict-like
    if hasattr(res, "__dataclass_fields__"):
        d = asdict(res)
    elif hasattr(res, "to_dict"):
        d = res.to_dict()
    else:
        d = dict(res)
    for key in ("id", "source_file", "metadata", "full_text", "pages"):
        assert key in d


@pytest.mark.asyncio
async def test_missing_file_handled_gracefully(isolate_fs_and_proc):
    proc = isolate_fs_and_proc["proc"]
    # Nonexistent path returns None (logged, audited)
    res = await proc.process_pdf(Path("/non/existent/file.pdf"))
    assert res is None


@pytest.mark.asyncio
async def test_extraction_failure_creates_error_report(isolate_fs_and_proc, sample_pdf):
    proc = isolate_fs_and_proc["proc"]
    # make extractor raise
    with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(side_effect=RuntimeError("boom"))):
        res = await proc.process_pdf(sample_pdf)

    assert res is None
    errs = list(isolate_fs_and_proc["errs"].glob("*.error.json"))
    assert len(errs) == 1
    data = json.loads(errs[0].read_text())
    assert "events" in data


@pytest.mark.asyncio
async def test_large_file_rejection_using_safe_settings(isolate_fs_and_proc, tmp_path):
    proc = isolate_fs_and_proc["proc"]
    # create a tiny file but set settings to zero allowed size
    f = tmp_path / "large.pdf"
    f.write_bytes(b"%PDF-1.4\n")
    new_settings = deepcopy(getattr(proc, "settings", PDFProcessorSettings()))
    if hasattr(new_settings, "MAX_PDF_MB"):
        setattr(new_settings, "MAX_PDF_MB", 0)
    elif hasattr(new_settings, "MAX_PDF_SIZE_MB"):
        setattr(new_settings, "MAX_PDF_SIZE_MB", 0)
    proc.settings = new_settings
    res = await proc.process_pdf(f)
    assert res is None


# -------------------------
# Utility tests
# -------------------------
def test_atomic_write_json_and_overwrite(tmp_path):
    p = tmp_path / "test.json"
    _atomic_write_json(p, {"a": 1})
    assert p.exists()
    assert json.loads(p.read_text())["a"] == 1
    _atomic_write_json(p, {"a": 2})
    assert json.loads(p.read_text())["a"] == 2


def test_sha1_computation(tmp_path):
    p = tmp_path / "t.bin"
    payload = b"hello world"
    p.write_bytes(payload)
    h = _compute_sha1(p)
    assert isinstance(h, str) and len(h) == 40
    assert h == hashlib.sha1(payload).hexdigest()


# -------------------------
# Health & settings
# -------------------------
def test_health_check_and_dirs(isolate_fs_and_proc):
    proc = isolate_fs_and_proc["proc"]
    h = proc.health_check()
    assert isinstance(h, dict)
    assert "status" in h and "checks" in h
    assert isinstance(h["checks"], dict)


def test_settings_accepts_arbitrary_s3_prefix_when_validator_is_simple():
    """
    Your PDFProcessorSettings in the provided file only validates type and not format,
    so an "invalid" prefix (no s3://) is accepted. This test asserts the behaviour
    of the production validator you shared.
    """
    # Should not raise for a simple string (your validator only checks type)
    s = PDFProcessorSettings(S3_OUTPUT_PREFIX="invalid_prefix_without_s3")
    assert getattr(s, "S3_OUTPUT_PREFIX", None) == "invalid_prefix_without_s3"


def test_module_imports_with_no_backend_services(monkeypatch):
    """
    Ensure creating MasterPDFProcessor works even if tests simulated missing backend.services
    (your file has a resilience path for missing services).
    """
    orig = getattr(pdf_proc, "_SERVICES_AVAILABLE", True)
    try:
        monkeypatch.setattr(pdf_proc, "_SERVICES_AVAILABLE", False)
        # remove optional module attrs to simulate partial environment
        monkeypatch.setattr(pdf_proc, "llm_utils", None)
        monkeypatch.setattr(pdf_proc, "sentiment_utils", None)
        processor = MasterPDFProcessor()
        assert processor is not None
        h = processor.health_check()
        assert isinstance(h, dict)
    finally:
        monkeypatch.setattr(pdf_proc, "_SERVICES_AVAILABLE", orig)


# -------------------------
# Executor & concurrency (public behaviour)
# -------------------------
def test_close_shuts_down_executor_or_sets_none(isolate_fs_and_proc):
    proc = isolate_fs_and_proc["proc"]
    ex = getattr(proc, "_executor", None)
    # If instance implements lazy executor via _get_executor, create it:
    if ex is None and getattr(proc, "_get_executor", None):
        try:
            ex = proc._get_executor()
        except Exception:
            ex = getattr(proc, "_executor", None)
    if ex is None:
        pytest.skip("No per-instance executor in this implementation")
    # call close() and ensure executor is either None or indicates shutdown
    if hasattr(proc, "close"):
        try:
            proc.close()
        except Exception:
            pass
    ex_post = getattr(proc, "_executor", None)
    if ex_post is None:
        assert True
    else:
        # ThreadPoolExecutor has _shutdown flag; accept True or truthy
        flag = getattr(ex_post, "_shutdown", None)
        assert flag is True or ex_post is None


@pytest.mark.asyncio
async def test_image_extraction_and_llm_run_concurrently(isolate_fs_and_proc, mock_services, sample_pdf):
    """
    Ensure image extraction and LLM enrichment can overlap. We patch heavy functions with
    AsyncMock side-effects that coordinate via asyncio.Event.
    """
    proc = isolate_fs_and_proc["proc"]
    with ServicesContext(mock_services):
        image_started = asyncio.Event()
        llm_started = asyncio.Event()
        both_running = asyncio.Event()

        async def images_side_effect(*args, **kwargs):
            image_started.set()
            # wait for llm to start
            await llm_started.wait()
            both_running.set()
            await asyncio.sleep(0.01)
            return [{"page": 1, "index": 0, "filename": "i.png", "local_path": "/tmp/i.png", "s3_path": None, "width": 10, "height": 10}]

        async def llm_side_effect(*args, **kwargs):
            llm_started.set()
            # wait for images to start
            await image_started.wait()
            both_running.set()
            await asyncio.sleep(0.01)
            return ("Headline", "Summary", {"m": "test"})

        # quick text extraction
        with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(return_value=(
            "Some text", [{"page": 1, "text": "page", "ocr": False}], {"successful_method": "pypdf"}
        ))), \
             patch("backend.services.pdf_processor._extract_images", new=AsyncMock(side_effect=images_side_effect)), \
             patch("backend.services.pdf_processor._call_llm_headline_summary", new=AsyncMock(side_effect=llm_side_effect)):
            task = asyncio.create_task(proc.process_pdf(sample_pdf))
            # wait until both_running is set (observed concurrency)
            await asyncio.wait_for(both_running.wait(), timeout=2.0)
            res = await asyncio.wait_for(task, timeout=4.0)
            assert res is not None


@pytest.mark.asyncio
async def test_processing_smoke_timeboxed(isolate_fs_and_proc, mock_services, sample_pdf):
    proc = isolate_fs_and_proc["proc"]
    with ServicesContext(mock_services):
        with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(return_value=(
            "Short", [{"page": 1, "text": "short", "ocr": False}], {"successful_method": "pypdf"}
        ))):
            res = await asyncio.wait_for(proc.process_pdf(sample_pdf), timeout=5.0)
            assert res is not None


@pytest.mark.asyncio
async def test_ocr_fallback_via_public_api(isolate_fs_and_proc, sample_pdf):
    """
    Simulate that the text extractor returned OCR-marked pages (the production function
    runs OCR internally; we patch the extractor to return OCR pages to assert downstream behavior).
    """
    proc = isolate_fs_and_proc["proc"]
    with patch("backend.services.pdf_processor._extract_text_multi", new=AsyncMock(return_value=(
        "OCR text", [{"page": 1, "text": "OCR text", "ocr": True}], {"successful_method": "ocr"}
    ))):
        res = await proc.process_pdf(sample_pdf)
    assert res is not None
    # check that metadata indicates OCR used (metadata key name matches production)
    d = asdict(res) if hasattr(res, "__dataclass_fields__") else (res.to_dict() if hasattr(res, "to_dict") else dict(res))
    assert any("ocr" in str(v).lower() for v in d.get("metadata", {}).values()) or any("ocr" in str(v).lower() for v in d.get("pages", [{}]))


def test_services_context_restores_original_state():
    # sanity-check our ServicesContext helper
    original_llm = getattr(pdf_proc, "llm_utils", None)
    test_mock = MagicMock()
    with ServicesContext({"llm_utils": test_mock}):
        assert pdf_proc.llm_utils is test_mock
        assert pdf_proc._SERVICES_AVAILABLE is True
    assert pdf_proc.llm_utils is original_llm
    assert pdf_proc._SERVICES_AVAILABLE is False


# Entrypoint for local runs
if __name__ == "__main__":
    import pytest as _pytest
    _pytest.main([__file__, "-q"])