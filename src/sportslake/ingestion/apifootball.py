"""Client tối giản cho API-Football v3 (và Mock API có cùng giao diện)."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)


class QuotaExceeded(RuntimeError):
    pass


@dataclass
class ApiResponse:
    endpoint: str
    params: dict
    status_code: int
    body: dict
    received_ts_ms: int
    latency_ms: float
    remaining_day: int | None
    remaining_minute: int | None

    @property
    def items(self) -> list:
        return self.body.get("response") or []

    @property
    def errors(self):
        return self.body.get("errors")


class ApiFootballClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 15.0):
        if not api_key:
            raise ValueError("Thiếu APIFOOTBALL_KEY. Điền vào file .env hoặc dùng COLLECTOR_MODE=mock")
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"x-apisports-key": api_key, "Accept": "application/json"})
        self.timeout = timeout

    def get(self, endpoint: str, params: dict | None = None) -> ApiResponse:
        params = params or {}
        t0 = time.time()
        r = self.session.get(f"{self.base_url}/{endpoint.lstrip('/')}", params=params, timeout=self.timeout)
        t1 = time.time()
        try:
            body = r.json()
        except ValueError:
            body = {"errors": {"http": r.text[:500]}, "response": []}
        resp = ApiResponse(
            endpoint=endpoint, params=params, status_code=r.status_code, body=body,
            received_ts_ms=int(t1 * 1000), latency_ms=(t1 - t0) * 1000,
            remaining_day=_int(r.headers.get("x-ratelimit-requests-remaining")),
            remaining_minute=_int(r.headers.get("X-RateLimit-Remaining")),
        )
        errs = body.get("errors")
        if errs and isinstance(errs, dict) and any(k in errs for k in ("requests", "rateLimit")):
            raise QuotaExceeded(str(errs))
        if r.status_code == 429:
            raise QuotaExceeded("HTTP 429")
        if errs:
            log.warning("API trả lỗi %s %s: %s", endpoint, params, errs)
        return resp

    # tiện ích
    def live_fixtures(self, league_ids: list[str] | None = None) -> ApiResponse:
        live = "-".join(league_ids) if league_ids else "all"
        return self.get("fixtures", {"live": live})

    def fixtures_by_date(self, date: str) -> ApiResponse:
        return self.get("fixtures", {"date": date})

    def fixture_events(self, fixture_id: str) -> ApiResponse:
        return self.get("fixtures/events", {"fixture": fixture_id})

    def fixture_statistics(self, fixture_id: str) -> ApiResponse:
        return self.get("fixtures/statistics", {"fixture": fixture_id})


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
