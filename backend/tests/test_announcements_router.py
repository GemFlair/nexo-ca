import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.routers import announcements


@pytest.fixture(autouse=True)
def reset_cache(monkeypatch):
    announcements._LIST_CACHE = {"ts": 0.0, "data": [], "dir_mtime": 0.0}
    calls = {"metrics": [], "audits": []}

    def fake_metric(name: str, value: int = 1) -> None:
        calls["metrics"].append((name, value))

    def fake_audit(event: str, detail: dict, status: str = "success") -> None:
        calls["audits"].append((event, status, detail))

    monkeypatch.setattr(announcements, "_metric_inc", fake_metric)
    monkeypatch.setattr(announcements, "_audit", fake_audit)
    return calls


@pytest.fixture
def sample_ann_dir(tmp_path: Path, monkeypatch):
    ann_payload = {
        "id": "ann_demo_1",
        "company_name": "Demo Corp",
        "announcement_datetime_iso": datetime.now(timezone.utc).isoformat(),
        "summary_60": "Sample summary",
        "headline_final": "Demo headline",
        "market_snapshot": {
            "symbol": "DEMO",
            "company_name": "Demo Corp",
        },
    }
    file_path = tmp_path / "ann_demo_1.json"
    file_path.write_text(json.dumps(ann_payload), encoding="utf-8")
    monkeypatch.setattr(announcements, "ANN_DIR", tmp_path)
    return tmp_path


@pytest.fixture
def client(sample_ann_dir):
    app = FastAPI()
    app.include_router(announcements.router)
    return TestClient(app)


def test_list_announcements_returns_data(client: TestClient, reset_cache):
    response = client.get("/announcements")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["company_name"] == "Demo Corp"
    assert reset_cache["metrics"][0][0] == "announcements.list.requests"


def test_get_announcement_success(client: TestClient, reset_cache):
    response = client.get("/announcements/ann_demo_1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["company_name"] == "Demo Corp"
    metric_names = [name for name, _ in reset_cache["metrics"]]
    assert "announcements.detail.requests" in metric_names
    assert "announcements.detail.hit" in metric_names


def test_get_announcement_not_found(client: TestClient, reset_cache):
    response = client.get("/announcements/missing")
    assert response.status_code == 404
    metric_names = [name for name, _ in reset_cache["metrics"]]
    assert "announcements.detail.miss" in metric_names
