"""Đồng bộ Elasticsearch: lỗi tạm thời phải làm vi lô thất bại; lỗi 4xx ghi nhật ký và bỏ qua; _id ổn định."""
import pytest

from sportslake.streaming import es_sync


class Row(dict):
    def asDict(self):
        return dict(self)


def fake_bulk(statuses):
    """statuses: mã HTTP cho từng tài liệu (2xx = thành công)."""
    def _sb(es, actions, **kw):
        for a, st in zip(list(actions), statuses):
            yield st < 300, {"index": {"_id": a["_id"], "status": st, "error": None if st < 300 else "x"}}
    return _sb


def test_transient_error_raises(monkeypatch):
    import elasticsearch.helpers as h
    monkeypatch.setattr(h, "streaming_bulk", fake_bulk([201, 503]))
    with pytest.raises(es_sync.EsSyncError):
        es_sync._bulk(None, "idx", [Row(match_id="m", batch_id=1), Row(match_id="m", batch_id=2)],
                      lambda d: f"{d['match_id']}|{d['batch_id']}")


def test_permanent_error_logged_not_raised(monkeypatch, tmp_path):
    import elasticsearch.helpers as h
    monkeypatch.setattr(h, "streaming_bulk", fake_bulk([201, 400]))
    monkeypatch.setattr("sportslake.config.DATA_DIR", tmp_path)
    seen = []

    def id_fn(d):
        seen.append(f"{d['match_id']}|{d['batch_id']}")
        return seen[-1]
    n = es_sync._bulk(None, "idx", [Row(match_id="m", batch_id=1), Row(match_id="m", batch_id=2)], id_fn)
    assert n == 1 and seen == ["m|1", "m|2"]
    assert (tmp_path / "metrics" / "es_rejected.jsonl").read_text().count("\n") == 1
