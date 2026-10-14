import time

import pytest
from fastapi.testclient import TestClient

from sportslake.ingestion.differ import SnapshotDiffer
from sportslake.mock_api import server as srv
from sportslake.mock_api.scenario import synthetic_scenario

H = {"x-apisports-key": "test"}


@pytest.fixture
def client():
    srv.STATE.restart(3, 1, 10, 42, 0, "synthetic", 1.0)
    srv.STATE.used_today = 0
    srv.STATE.recent.clear()
    return TestClient(srv.app)


def test_auth_and_quota(client):
    r = client.get("/fixtures", params={"live": "all"})
    assert r.json()["errors"]["token"]
    srv.STATE.minute_limit = 3
    try:
        for _ in range(3):
            r = client.get("/fixtures", params={"live": "all"}, headers=H)
            assert not r.json()["errors"]
        r = client.get("/fixtures", params={"live": "all"}, headers=H)
        assert "rateLimit" in r.json()["errors"]
        assert r.headers["X-RateLimit-Remaining"] == "0"
    finally:
        srv.STATE.minute_limit = 10


def test_response_shape(client):
    sc = next(iter(srv.STATE.scenarios.values()))
    sc.kickoff_ts = time.time() - 30 * 60 / srv.STATE.speed
    r = client.get("/fixtures", params={"live": "all"}, headers=H).json()
    fx = [x for x in r["response"] if x["fixture"]["id"] == sc.fixture_id][0]
    assert fx["fixture"]["status"]["short"] == "1H" and fx["fixture"]["status"]["elapsed"] == 31
    assert "events" in fx and fx["league"]["id"] == 39


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5, 6, 7, 8])
def test_differ_converges_to_truth(seed):
    """Theo dõi cả trận bằng differ -> tập sự kiện cuối trùng đáp án (kể cả VAR, sửa tên)."""
    sc = synthetic_scenario(1000 + seed, 0.0, seed, var_prob=1.0, correction_prob=0.5)
    srv.STATE.scenarios = {sc.fixture_id: sc}
    d = SnapshotDiffer("mock")
    for step in range(0, sc.total_len + 400, 45):
        sc.kickoff_ts = time.time() - step / srv.STATE.speed
        d.diff_fixture(srv._fixture_json(sc, time.time(), True), 10_000 + step)
    truth = srv.admin_truth(sc.fixture_id)
    assert set(d.memory[str(sc.fixture_id)].events) == {e["event_id"] for e in truth["events"]}
    assert truth["var_cancellations"] == 1
