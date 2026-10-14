from sportslake.ingestion.differ import SnapshotDiffer, assign_event_ids


def fx(events, status="1H", elapsed=30, home=0, away=0, fid=1):
    return {"fixture": {"id": fid, "timestamp": 1700000000, "status": {"short": status, "long": "x", "elapsed": elapsed, "extra": None},
                        "venue": {"name": "V"}},
            "league": {"id": 39, "name": "PL", "season": 2026, "round": "R1"},
            "teams": {"home": {"id": 10, "name": "A"}, "away": {"id": 20, "name": "B"}},
            "goals": {"home": home, "away": away}, "events": events}


def ev(t, typ, detail, team=10, player="P1", extra=None):
    return {"time": {"elapsed": t, "extra": extra}, "team": {"id": team, "name": "A" if team == 10 else "B"},
            "player": {"id": hash(player) % 1000, "name": player}, "assist": {"id": None, "name": None},
            "type": typ, "detail": detail, "comments": None}


def test_new_update_retract_cycle():
    d = SnapshotDiffer("test")
    g = ev(10, "Goal", "Normal Goal", player="Kane")
    out, st = d.diff_fixture(fx([g], home=1), 1000)
    assert [e["op"] for e in out] == ["UPSERT"] and out[0]["category"] == "GOAL"
    assert st and st["home_goals"] == 1
    goal_id = out[0]["event_id"]
    # cùng ảnh chụp -> không gì mới
    same, st = d.diff_fixture(fx([g], home=1), 2000)
    assert same == [] and st is None
    # sửa cầu thủ (đổi cả id) -> UPSERT cùng event_id, seq lớn hơn
    g2 = dict(g, player={"id": 5, "name": "Son"})
    out2, _ = d.diff_fixture(fx([g2], home=1), 3000)
    assert len(out2) == 1 and out2[0]["event_id"] == goal_id
    assert out2[0]["op"] == "UPSERT" and out2[0]["player_name"] == "Son" and out2[0]["seq"] == 3000
    # bàn thắng biến mất (VAR) -> RETRACT
    out3, st3 = d.diff_fixture(fx([], home=0), 4000)
    assert [e["op"] for e in out3] == ["RETRACT"] and out3[0]["event_id"] == out2[0]["event_id"]
    assert st3["home_goals"] == 0


def test_var_cancel_keep_style():
    d = SnapshotDiffer("test")
    g = ev(50, "Goal", "Normal Goal", player="X")
    d.diff_fixture(fx([g], status="2H", elapsed=51, home=1), 1000)
    var = ev(52, "Var", "Goal cancelled", player="X")
    out, _ = d.diff_fixture(fx([g, var], status="2H", elapsed=53, home=0), 2000)
    ops = sorted((e["op"], e["category"]) for e in out)
    assert ops == [("RETRACT", "GOAL"), ("UPSERT", "VAR")]


def test_same_minute_group_gets_distinct_ids():
    subs = [ev(60, "subst", "Substitution 1", player="A"), ev(60, "subst", "Substitution 2", player="B")]
    ids = [eid for eid, _ in assign_event_ids("1", subs)]
    assert len(set(ids)) == 2


def test_clock_and_period():
    d = SnapshotDiffer("test")
    out, _ = d.diff_fixture(fx([ev(45, "Card", "Yellow Card", extra=2), ev(90, "Card", "Red Card", extra=3)],
                               status="2H", elapsed=90), 1)
    by = {e["event_detail"]: e for e in out}
    assert by["Yellow Card"]["period"] == 1 and by["Yellow Card"]["minute"] == 47
    assert by["Red Card"]["period"] == 2 and by["Red Card"]["category"] == "CARD_RED"
    assert by["Red Card"]["match_clock_s"] > by["Yellow Card"]["match_clock_s"]


def test_state_persistence(tmp_path):
    p = tmp_path / "state.json"
    d = SnapshotDiffer("test", p)
    d.diff_fixture(fx([ev(10, "Goal", "Normal Goal")], home=1), 1000)
    d.save()
    d2 = SnapshotDiffer("test", p)
    out, st = d2.diff_fixture(fx([], home=0), 2000)
    assert [e["op"] for e in out] == ["RETRACT"]


def test_name_correction_with_same_player_id():
    """Nguồn sửa tên cầu thủ nhưng giữ nguyên player id -> vẫn phải sinh UPSERT."""
    d = SnapshotDiffer("test")
    g = ev(10, "Goal", "Normal Goal", player="Kane")
    first, _ = d.diff_fixture(fx([g], home=1), 1000)
    fixed = dict(g, player={"id": g["player"]["id"], "name": "H. Kane"})
    out, _ = d.diff_fixture(fx([fixed], home=1), 2000)
    assert len(out) == 1 and out[0]["event_id"] == first[0]["event_id"] and out[0]["player_name"] == "H. Kane"


def test_checkpoint_restore_resends():
    d = SnapshotDiffer("test")
    d.diff_fixture(fx([], home=0), 1000)
    cp = d.checkpoint()
    g = ev(20, "Goal", "Normal Goal")
    out, _ = d.diff_fixture(fx([g], home=1), 2000)
    assert len(out) == 1
    d.restore(cp)  # giả lập gửi Kafka thất bại
    again, st = d.diff_fixture(fx([g], home=1), 3000)
    assert len(again) == 1 and again[0]["event_id"] == out[0]["event_id"] and st["home_goals"] == 1
