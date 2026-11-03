"""回扫（已有结论的集再问一遍新源，2026-11-02）的离线用例。"""

import json
import queue
import threading
import time

import pytest

import download_tv as d
import pipeline as p
import tv_ids_to_links as m

BX = "bcdnxw.hakunaymatata.com"


def _jsonl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _row(tid, ep, urls, fetched_at=100, season=1):
    return {"tmdbId": str(tid), "season": season, "episode": ep, "urls": urls, "fetched_at": fetched_at}


def _vid(host=BX, name="a"):
    return {"url": f"https://{host}/{name}.mp4?sign=x&t=100", "provider": "vidlink", "type": "mp4"}


def _hls(host="sun1.finalsignal.top"):
    return {"url": f"https://{host}/fine/x.m3u8", "provider": "vidup", "type": "m3u8"}


FAKE = {"url": "https://a.b.workers.dev/video.m3u8?q=1", "provider": "vidup", "type": "m3u8"}


@pytest.fixture
def box(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "FAKE_LINK_HOSTS", ("workers.dev",))
    return tmp_path


def _candidates(box, ids=("1", "2", "3", "4", "5", "6"), **kw):
    return m.backfill_candidates(
        box / "results.jsonl", box / "fail.txt", box / "success.jsonl", box / "dead.jsonl",
        box / "ledger.jsonl", "vidrock", ids, {BX}, **kw)


def _ledgers(box):
    _jsonl(box / "results.jsonl", [
        _row(1, 1, [_vid()]),                       # 只有 bcdnxw
        _row(1, 2, [_hls()]),                       # 有别的链接、还没成功
        _row(1, 3, [_vid()]),                       # 已成功：不问
        _row(1, 4, [_vid(host="bcdn.hakunaymatata.com")]),
        _row(2, 1, [FAKE]),                         # 只有假链接
        _row(3, 1, [_vid(host="bcdn.hakunaymatata.com")]),    # 画质判死
        _row(3, 2, [_vid()]),                       # 硬字幕判死：不问
        _row(3, 3, [_vid()], fetched_at=50),
        _row(3, 3, [FAKE], fetched_at=60),          # 同集较新的行是假链接：按最新行
    ])
    (box / "fail.txt").write_text("1\t1\t9\n4\t1\t1\n5\t-\t-\n5\t1\t1\n6\t1\t1\tretry-exhausted\n", encoding="utf-8")
    _jsonl(box / "success.jsonl", [{"tmdbId": "1", "season": 1, "episode": 3, "uploaded": True}])
    _jsonl(box / "dead.jsonl", [
        {"tmdbId": "3", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": 200},
        {"tmdbId": "3", "season": 1, "episode": 2, "reason_class": "hardsub", "dead_at": 200},
    ])


def test_candidates_are_classified_in_priority_order_and_skip_what_must_not_be_asked(box):
    _ledgers(box)
    got = [(cat, key) for cat, key, _nodes in _candidates(box)]
    assert got == [
        ("bad_vidlink", ("1", 1, 1)),
        ("other_failed", ("1", 1, 2)),
        ("other_failed", ("1", 1, 4)),
        ("linked_no_source", ("1", 1, 9)),
        ("quality_dead", ("3", 1, 1)),
        ("fake_only", ("2", 1, 1)),
        ("fake_only", ("3", 1, 3)),
        ("no_source", ("4", 1, 1)),
    ]


def test_candidates_keep_real_nodes_only_where_they_still_matter(box):
    _ledgers(box)
    items = {key: nodes for _c, key, nodes in _candidates(box)}
    assert items[("1", 1, 1)] == [_vid()]
    assert items[("3", 1, 1)] == [], "判死集的旧节点已确定不达标"


def test_candidates_respect_ids_categories_ledger_and_deferred(box):
    _ledgers(box)
    assert {k[0] for _c, k, _n in _candidates(box, ids=("2",))} == {"2"}
    only = _candidates(box, categories=("quality_dead", "bad_vidlink"))
    assert [c for c, _k, _n in only] == ["quality_dead", "bad_vidlink"], "顺序按配置"
    _jsonl(box / "ledger.jsonl", [
        {"tmdbId": "1", "season": 1, "episode": 1, "provider": "vidrock", "status": "dead"},
        {"tmdbId": "1", "season": 1, "episode": 2, "provider": "other", "status": "ok"},
    ])
    keys = [k for _c, k, _n in _candidates(box)]
    assert ("1", 1, 1) not in keys, "这个源问过的不再问"
    assert ("1", 1, 2) in keys, "别的源问过不算"
    assert ("9", 1, 1) in [k for _c, k, _n in _candidates(box, ids=("9",), deferred=[("9", 1, 1)])]


class _Fetch:
    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def __call__(self, tid, s, e, providers=None, stop_event=None):
        self.calls.append(((tid, s, e), providers))
        answer = self.answers[(tid, s, e)]
        answer = answer.pop(0) if isinstance(answer, list) else answer
        if answer in ("dead", "retry", "deferred"):
            return answer, None
        return "ok", {"tmdbId": tid, "season": s, "episode": e, "fetched_at": 500,
                      "urls": [{"url": answer, "provider": "vidrock", "type": "m3u8", "link_checked": True}]}


def _run(box, monkeypatch, items, answers, **kw):
    fake = _Fetch(answers)
    monkeypatch.setattr(m, "process_episode", fake)
    out = []
    counts, left = m.run_backfill(box / "results.jsonl", box / "ledger.jsonl", items, on_result=out.append,
                                  provider="vidrock", workers=2, per_minute=60000, retry_wait=0, **kw)
    return fake, out, counts, left


def test_answers_are_saved_queued_and_remembered(box, monkeypatch):
    items = [("bad_vidlink", ("1", 1, 1), [_vid()]), ("quality_dead", ("2", 1, 1), []),
             ("no_source", ("3", 1, 1), [])]
    fake, out, counts, left = _run(box, monkeypatch, items, {
        ("1", 1, 1): "https://o/1.m3u8", ("2", 1, 1): "https://o/2.m3u8", ("3", 1, 1): "dead"}, max_passes=1)
    assert all(providers == ["vidrock"] for _k, providers in fake.calls), "只问这一个源"
    by_key = {r["tmdbId"]: r for r in out}
    assert [u["url"] for u in by_key["1"]["urls"]] == ["https://o/1.m3u8", _vid()["url"]], "新节点排前，旧节点留着"
    assert by_key["1"]["backfill"] == "vidrock" and "revive_dead" not in by_key["1"]
    assert [u["url"] for u in by_key["2"]["urls"]] == ["https://o/2.m3u8"] and by_key["2"]["revive_dead"] is True
    saved = [json.loads(x) for x in (box / "results.jsonl").read_text(encoding="utf-8").splitlines()]
    assert sorted(r["tmdbId"] for r in saved) == ["1", "2"]
    ledger = [json.loads(x) for x in (box / "ledger.jsonl").read_text(encoding="utf-8").splitlines()]
    assert sorted((r["tmdbId"], r["status"]) for r in ledger) == [("1", "ok"), ("2", "ok"), ("3", "dead")]
    assert not left and counts[("no_source", "dead")] == 1
    assert m._load_backfill_asked(box / "ledger.jsonl", "vidrock") == {("1", 1, 1), ("2", 1, 1), ("3", 1, 1)}


def test_transient_errors_are_retried_then_left_unrecorded(box, monkeypatch):
    items = [("no_source", ("1", 1, 1), []), ("no_source", ("2", 1, 1), [])]
    fake, out, _counts, left = _run(box, monkeypatch, items, {
        ("1", 1, 1): ["retry", "https://o/1.m3u8"], ("2", 1, 1): ["retry", "deferred", "retry"]}, max_passes=3)
    assert [r["tmdbId"] for r in out] == ["1"]
    assert [i[1] for i in left] == [("2", 1, 1)]
    assert len([c for c in fake.calls if c[0] == ("2", 1, 1)]) == 3
    assert m._load_backfill_asked(box / "ledger.jsonl", "vidrock") == {("1", 1, 1)}, "瞬时失败不记账，下次接着问"


def test_one_bad_episode_does_not_stop_the_scan(box, monkeypatch):
    def fetch(tid, s, e, providers=None, stop_event=None):
        if tid == "1":
            raise RuntimeError("坏了")
        return "dead", None

    monkeypatch.setattr(m, "process_episode", fetch)
    _counts, left = m.run_backfill(box / "r.jsonl", box / "ledger.jsonl",
                                   [("no_source", ("1", 1, 1), []), ("no_source", ("2", 1, 1), [])],
                                   provider="vidrock", workers=1, per_minute=60000, max_passes=1)
    assert [i[1] for i in left] == [("1", 1, 1)]
    assert m._load_backfill_asked(box / "ledger.jsonl", "vidrock") == {("2", 1, 1)}


def test_stop_event_ends_the_scan_without_touching_the_rest(box, monkeypatch):
    stop = threading.Event()
    asked = []

    def fetch(tid, s, e, providers=None, stop_event=None):
        asked.append(tid)
        stop.set()
        return "dead", None

    monkeypatch.setattr(m, "process_episode", fetch)
    items = [("no_source", (str(i), 1, 1), []) for i in range(50)]
    m.run_backfill(box / "r.jsonl", box / "ledger.jsonl", items, stop_event=stop, provider="vidrock",
                   workers=1, per_minute=60000, retry_wait=9999)
    assert asked == ["0"]


def test_rate_limiter_paces_callers():
    now = [0.0]
    limiter = m._RateLimiter(60, clock=lambda: now[0])
    waits = []

    class Stop:
        def wait(self, seconds):
            waits.append(seconds)

    for _ in range(3):
        limiter.wait(Stop())
    assert waits == [1.0, 2.0], "每秒一个"
    now[0] = 100.0
    limiter.wait(Stop())
    assert waits == [1.0, 2.0], "空闲过后不攒着补发"


def test_start_backfill_reads_download_ledgers_and_reports(box, monkeypatch, capsys):
    _ledgers(box)
    monkeypatch.setattr(m, "_load_download_cfg", lambda: {
        "success_log": str(box / "success.jsonl"), "download_dead_log": str(box / "dead.jsonl"),
        "link_health": {"broken_vidlink_hosts": [BX]}})
    monkeypatch.setitem(m._BACKFILL_CFG, "ledger", str(box / "ledger.jsonl"))
    monkeypatch.setattr(m, "BACKFILL_PER_MINUTE", 60000.0)
    monkeypatch.setattr(m, "process_episode", lambda *a, **k: ("dead", None))
    m._start_backfill(box / "results.jsonl", box / "fail.txt", ["1"], {("1", 1, 8): [m.FAKE_LINK_MARK]}, None, None)
    out = capsys.readouterr().out
    assert "回扫开始" in out and "回扫结束" in out
    assert ("1", 1, 8) in m._load_backfill_asked(box / "ledger.jsonl", "vidrock"), "只有假链接而推迟的集也问"


def test_shipped_backfill_config():
    # 2026-11-03 随 vidrock 停用关掉（用户定），其余设置留着备恢复。
    assert m.BACKFILL_ENABLED is False and m.BACKFILL_PROVIDER == "vidrock"
    assert m.BACKFILL_CATEGORIES[0] == "bad_vidlink" and m.BACKFILL_CATEGORIES[-1] == "no_source"
    assert m.BACKFILL_PER_MINUTE <= 120


# ------------------------------------------------------------ 下载侧放回判死集

@pytest.fixture
def dead_env(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "in.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "dead.jsonl"))
    return tmp_path


def test_only_newer_backfill_rows_revive_quality_dead(dead_env):
    _jsonl(dead_env / "dead.jsonl", [
        {"tmdbId": "1", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": 100},
        {"tmdbId": "2", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": 900},
        {"tmdbId": "3", "season": 1, "episode": 1, "reason_class": "hardsub", "dead_at": 100},
        {"tmdbId": "4", "season": 1, "episode": 1, "manual": True, "reason_class": "quality", "dead_at": 100},
        {"tmdbId": "5", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": 100},
    ])
    rows = [{**_row(i, 1, [_hls()], fetched_at=500), "revive_dead": True} for i in (1, 2, 3, 4)]
    rows.append(_row(5, 1, [_hls()], fetched_at=500))
    _jsonl(dead_env / "in.jsonl", rows)
    assert d.load_revived_dead_keys({"1_S01E01", "2_S01E01", "3_S01E01", "4_S01E01", "5_S01E01"}) == {"1_S01E01"}


def test_revive_tolerates_missing_files(dead_env):
    assert d.load_revived_dead_keys({"1_S01E01"}) == set()
    assert d.load_revived_dead_keys(set()) == set()


def test_queue_lets_backfill_results_through_for_already_delivered_episodes():
    q = queue.Queue()
    source = p.QueueEntrySource(q, backlog=[_row(1, 1, [_vid()])])
    assert source.poll()[0] == "item"
    q.put(_row(1, 1, [_hls()]))
    q.put({**_row(1, 1, [_hls()]), "backfill": "vidrock"})
    state, entry = source.poll()
    assert state == "item" and entry["backfill"] == "vidrock"
    assert source.skipped_duplicates == 1, "普通重复照旧跳过"


def _pipeline(tmp_path, monkeypatch, entries, dead_rows, queue_items=()):
    from test_overlay_tv import _pipeline_env, _scan

    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda path: (_scan(), []))
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "dead.jsonl"))
    _jsonl(tmp_path / "dead.jsonl", dead_rows)
    q = queue.Queue()
    for item in queue_items:
        q.put(item)
    q.put(p._SENTINEL)
    monkeypatch.setattr(d, "ListEntrySource", lambda items: p.QueueEntrySource(q, backlog=items))
    d._run_pipeline()
    return downloaded


def test_run_downloads_revived_episode_from_saved_row(tmp_path, monkeypatch):
    now = int(time.time())
    dead = [{"tmdbId": "1", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": now - 100},
            {"tmdbId": "1", "season": 1, "episode": 2, "reason_class": "quality", "dead_at": now - 100}]
    entries = [{**_row(1, 1, [_hls()], fetched_at=now), "title": "x", "revive_dead": True},
               {**_row(1, 2, [_hls()], fetched_at=now), "title": "x"}]
    downloaded = _pipeline(tmp_path, monkeypatch, entries, dead)
    assert [key for key, _url in downloaded] == ["1_S01E01"], "没带放回标记的判死集照旧跳过"


def test_run_downloads_revived_episode_arriving_in_this_run(tmp_path, monkeypatch):
    now = int(time.time())
    dead = [{"tmdbId": "2", "season": 1, "episode": 1, "reason_class": "quality", "dead_at": now + 100}]
    item = {**_row(2, 1, [_hls()], fetched_at=now + 200), "title": "x", "revive_dead": True, "backfill": "vidrock"}
    downloaded = _pipeline(tmp_path, monkeypatch, [], dead, queue_items=[item])
    assert [key for key, _url in downloaded] == ["2_S01E01"]


def test_run_does_not_revive_an_episode_that_already_succeeded(tmp_path, monkeypatch):
    """放回标记只对判死集生效：已成功的集留在已处理名单里，真实的 process_one_entry 会跳过它。"""
    from test_overlay_tv import _pipeline_env, _scan

    now = int(time.time())
    item = {**_row(3, 1, [_hls()], fetched_at=now), "title": "x", "revive_dead": True, "backfill": "vidrock"}
    _pipeline_env(tmp_path, monkeypatch, [], lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda path: (_scan(), []))
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "dead.jsonl"))
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"3_S01E01"})
    real = d.process_one_entry
    seen = {}

    def spy(entry, ids):
        seen[d.record_episode_key(entry)] = d.record_episode_key(entry) in ids
        return real(entry, ids)

    monkeypatch.setattr(d, "process_one_entry", spy)
    q = queue.Queue()
    q.put(item)
    q.put(p._SENTINEL)
    monkeypatch.setattr(d, "ListEntrySource", lambda items: p.QueueEntrySource(q, backlog=items))
    d._run_pipeline()
    assert seen == {"3_S01E01": True}
