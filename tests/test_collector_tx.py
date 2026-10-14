"""Collector chỉ lưu trạng thái differ khi Kafka đã xác nhận mọi thông điệp; nếu lỗi thì hoàn tác và gửi lại."""
import json

from sportslake.ingestion import collector as col
from sportslake.ingestion.differ import SnapshotDiffer
from sportslake.ingestion.scheduler import Scheduler
from sportslake.kafka_io import AvroEncoder, DeliveryTracker
from sportslake.config import TOPIC_EVENTS, TOPIC_STATE, TOPIC_STATS


class FakeProducer:
    def __init__(self):
        self.fail = False
        self.sent = []

    def produce(self, topic, key, value, on_delivery=None):
        self.sent.append(topic)
        on_delivery("BROKER_DOWN" if self.fail else None, None)

    def poll(self, t=0):
        pass

    def flush(self, t=0):
        return 0


def make_collector(tmp_path):
    c = object.__new__(col.Collector)
    c.mode, c.source, c.stop = "mock", "mock", False
    c.producer, c.tracker = FakeProducer(), DeliveryTracker()
    c.enc_events, c.enc_state, c.enc_stats = (AvroEncoder(TOPIC_EVENTS, schema_id=1), AvroEncoder(TOPIC_STATE, schema_id=1),
                                              AvroEncoder(TOPIC_STATS, schema_id=1))
    c.differ = SnapshotDiffer("mock", tmp_path / "differ.json")
    c.scheduler = Scheduler()
    c._last_logged, c._pending_final, c._finalized_now = {}, set(), []
    return c


FX = {"fixture": {"id": 7, "timestamp": 1, "status": {"short": "1H", "elapsed": 20}},
      "league": {"id": 39}, "teams": {"home": {"id": 1, "name": "A"}, "away": {"id": 2, "name": "B"}},
      "goals": {"home": 1, "away": 0},
      "events": [{"time": {"elapsed": 12, "extra": None}, "team": {"id": 1, "name": "A"},
                  "player": {"id": 9, "name": "P"}, "assist": {}, "type": "Goal", "detail": "Normal Goal"}]}


def test_failed_delivery_rolls_back_and_resends(tmp_path):
    c = make_collector(tmp_path)
    c.producer.fail = True
    assert c._transaction(lambda: c.handle_fixture(FX, 1000)) is False
    assert not (tmp_path / "differ.json").exists()          # không lưu trạng thái khi gửi lỗi
    c.producer.fail = False
    c.producer.sent.clear()
    assert c._transaction(lambda: c.handle_fixture(FX, 2000)) is True
    assert c.producer.sent.count(TOPIC_EVENTS) == 1           # bàn thắng được gửi lại
    saved = json.loads((tmp_path / "differ.json").read_text())
    assert "7" in saved


def test_success_then_restart_does_not_resend(tmp_path):
    c = make_collector(tmp_path)
    assert c._transaction(lambda: c.handle_fixture(FX, 1000)) is True
    c2 = make_collector(tmp_path)                             # khởi động lại, đọc trạng thái đã lưu
    c2._transaction(lambda: c2.handle_fixture(FX, 2000))
    assert c2.producer.sent.count(TOPIC_EVENTS) == 0
