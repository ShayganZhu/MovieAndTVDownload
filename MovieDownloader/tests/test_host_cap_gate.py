"""host_limits（按 CDN 主机的并发上限 + 让行队列，2026-10-24）的离线用例。

背景：sun.paleoak.top 回源过载时，慢片占满下载槽位、把快片饿死。HostCapGate
把这类片的在途数压在上限内，多出的整片让行；派发那一刻链接太旧就先重取；
让行太久且有 mp4 的片改走 mp4。

重点锁死几件错了会静默丢片 / 卡死 / 白烧配额的事：
  - 让行的片本轮内必须下完：来源取空后有让行就报 wait，不能 done；
  - 名额必须归还，否则闸门永久关死；
  - 派发前重取无果时必须沿用原链接，不能让片消失；
  - 让行队列满了要停止从来源取片（把反压还给取流侧）。
"""

import random
import threading
import time

import pytest
import requests

import download_movies as d
import pipeline as p


PALEO = "sun.paleoak.top"
HAKUNA = "bcdn.hakunaymatata.com"
NOW = 1_800_000_000


def _limits(**override):
    cfg = {
        "max_concurrent": 2,
        "refetch_after_minutes": {"vidup": 120, "videasy": 180},
        "jit_refetch_max_per_movie": 2,
        "mp4_fallback_after_minutes": 120,
        "max_deferred": 3,
        "playlist_500_means_expired": True,
    }
    cfg.update(override)
    return d._parse_host_limits({PALEO: cfg})


@pytest.fixture(autouse=True)
def _host_limits(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", _limits())


def pal(tid, provider="videasy", age_min=None, mp4=False):
    """第一个节点在 paleoak 的片；mp4=True 时再带一个 vidlink mp4 备用节点。"""
    urls = [{"url": f"https://{PALEO}/r6/{tid}/master.m3u8",
             "provider": provider, "type": "m3u8"}]
    if mp4:
        urls.append({"url": f"https://{HAKUNA}/{tid}.mp4",
                     "provider": "vidlink", "type": "mp4"})
    entry = {"tmdbId": tid, "urls": urls}
    if age_min is not None:
        entry["fetched_at"] = NOW - age_min * 60
    return entry


def fast(tid):
    return {"tmdbId": tid, "urls": [{"url": f"https://{HAKUNA}/{tid}.mp4",
                                     "provider": "vidlink", "type": "mp4"}]}


class Clock:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, minutes):
        self.t += minutes * 60


class Src:
    """列表来源：取空后 closed=False 报 wait、closed=True 报 done。

    带 pipeline 首轮来源（QueueEntrySource）的两个能力：能回送重取结果、
    能回答某 id 的重取结果是否在缓冲里。
    """

    def __init__(self, items=(), closed=True, delivers=True, refetcher=None):
        self.items = list(items)
        self.closed = closed
        self.delivers_refetch_results = delivers
        self.buffered = set()
        self.polls = 0
        # 真实链路里重取结果在被来源收走之前一直算「有下文」（is_pending）；
        # 来源把它交出去的同时才注销。这里同样在交出时注销。
        self.refetcher = refetcher

    def poll(self):
        self.polls += 1
        if self.items:
            item = self.items.pop(0)
            if self.refetcher is not None:
                self.refetcher.pending.discard(item.get("tmdbId"))
            return "item", item
        return ("done", None) if self.closed else ("wait", None)

    def has_buffered(self, key):
        return key in self.buffered


class Refetcher:
    def __init__(self):
        self.dispatched = []
        self.pending = set()

    def dispatch(self, entries):
        for entry in entries:
            self.dispatched.append(entry["tmdbId"])
            self.pending.add(entry["tmdbId"])
        return len(entries)

    def is_pending(self, key):
        return key in self.pending


def make_gate(src, refetcher=None, extra=None, clock=None):
    gate = d.HostCapGate(
        d.HOST_LIMITS, refetcher=refetcher,
        extra_active=(lambda host: extra.get(host, 0)) if extra is not None else None,
        clock=clock or Clock(),
    )
    gate.set_source(src)
    return gate


def drain(gate):
    """一直 poll 到不再出片，返回 (出片 id 列表, 末次状态)。"""
    out = []
    while True:
        state, entry = gate.poll()
        if state != "item":
            return out, state
        out.append(entry)


def ids(entries):
    return [e["tmdbId"] for e in entries]


# ------------------------------------------------------------ 上限与让行

def test_uncapped_entries_pass_through_in_order():
    src = Src([fast("1"), fast("2"), {"tmdbId": "3", "urls": ["https://x/a.m3u8"]}])
    got, state = drain(make_gate(src))
    assert ids(got) == ["1", "2", "3"]
    assert state == "done"


def test_capped_entries_beyond_limit_wait_while_fast_ones_flow():
    """🔴 本功能的目的：慢主机的片满额后让行，后面的快片照常派发。"""
    src = Src([pal("p1"), pal("p2"), pal("p3"), fast("f4"), pal("p5"), fast("f6")])
    gate = make_gate(src)
    got, state = drain(gate)
    assert ids(got) == ["p1", "p2", "f4", "f6"]
    # 来源已取空，但还有两部在让行：本轮不能收尾。
    assert state == "wait"
    assert gate.stats["deferred"] == 2


def test_release_frees_a_slot_and_deferred_go_first_in_first_out():
    src = Src([pal("p1"), pal("p2"), pal("p3"), pal("p4")])
    gate = make_gate(src)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2"]

    gate.release(got[0])
    got2, state = drain(gate)
    assert ids(got2) == ["p3"] and state == "wait"

    gate.release(got[1])
    gate.release(got2[0])
    got3, state = drain(gate)
    assert ids(got3) == ["p4"]
    gate.release(got3[0])
    assert drain(gate) == ([], "done")


def test_release_is_idempotent_and_ignores_unreserved_entries():
    """未占名额的（快片 / mp4 兜底副本）和重复归还都不能把计数减成负数。"""
    src = Src([pal("p1"), pal("p2"), pal("p3")])
    gate = make_gate(src)
    got, _ = drain(gate)
    gate.release(got[0])
    gate.release(got[0])
    gate.release(fast("x"))
    gate.release(None)
    got2, _ = drain(gate)
    assert ids(got2) == ["p3"]           # 只空出一个位，不是两个
    assert drain(gate) == ([], "done")


def test_fresh_arrival_does_not_jump_ahead_of_deferred():
    """有空位时，新到的同主机片也要排在已让行的后面（先来先走）。"""
    src = Src([pal("p1"), pal("p2"), pal("p3")], closed=False)
    gate = make_gate(src)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2"]
    gate.release(got[0])
    src.items.append(pal("p4"))
    got2, _ = drain(gate)
    assert ids(got2) == ["p3"]


def test_extra_occupancy_counts_against_the_limit():
    """第一个节点不在该主机、却切到该主机下载的片，同样占位。"""
    extra = {PALEO: 1}
    src = Src([pal("p1"), pal("p2")])
    gate = make_gate(src, extra=extra)
    got, state = drain(gate)
    assert ids(got) == ["p1"] and state == "wait"
    extra[PALEO] = 0
    assert ids(drain(gate)[0]) == ["p2"]


def test_entry_whose_second_node_is_capped_is_not_held_back():
    """只看第一个节点：paleoak 只是备用节点的片不受上限约束。"""
    entry = fast("f1")
    entry["urls"].append({"url": f"https://{PALEO}/r6/f1/master.m3u8",
                          "provider": "videasy", "type": "m3u8"})
    src = Src([pal("p1"), pal("p2"), entry])
    got, _ = drain(make_gate(src))
    assert ids(got) == ["p1", "p2", "f1"]


def test_full_deferred_queue_stops_pulling_from_source():
    """🔴 让行队列满了就不再从来源取片：否则取流侧感受不到反压。

    代价是排在后面的快片也暂时取不到（队头阻塞）——这正是把反压还给
    取流侧的方式；名额一空就恢复。
    """
    src = Src([pal(f"p{i}") for i in range(1, 7)] + [fast("f7")])
    gate = make_gate(src)          # 上限 2，让行队列上限 3
    got, state = drain(gate)
    assert ids(got) == ["p1", "p2"] and state == "wait"
    assert ids(src.items) == ["f7"], "队列满了还在从来源取片"

    gate.release(got[0])
    got2, _ = drain(gate)
    # 空出的位给最早让行的 p3；p6 补进让行队列；f7 终于取到。
    assert ids(got2) == ["p3", "f7"]


def test_entries_without_tmdb_id_are_not_gated():
    src = Src([pal("p1"), pal("p2"), {"urls": pal("x")["urls"]}])
    got, _ = drain(make_gate(src))
    assert len(got) == 3


# ------------------------------------------------------------ 派发前重取

@pytest.mark.parametrize("provider,age,refetched", [
    ("vidup", 119, False), ("vidup", 121, True),
    ("videasy", 179, False), ("videasy", 181, True),
    ("vidfast", 121, True),     # 没单独配的 provider 按最小值（120）
])
def test_stale_link_is_refetched_right_before_dispatch(provider, age, refetched):
    r = Refetcher()
    src = Src([pal("p1", provider=provider, age_min=age)])
    got, state = drain(make_gate(src, refetcher=r))
    if refetched:
        assert got == [] and state == "wait"
        assert r.dispatched == ["p1"]
    else:
        assert ids(got) == ["p1"] and r.dispatched == []


def test_refetched_entry_comes_back_through_source_and_is_dispatched():
    r = Refetcher()
    src = Src([pal("p1", age_min=200)], closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r)
    assert drain(gate) == ([], "wait")

    src.items.append(pal("p1", age_min=1))      # 新链接经来源回来
    got, _ = drain(gate)
    assert ids(got) == ["p1"]
    assert got[0]["fetched_at"] == NOW - 60
    src.closed = True
    gate.release(got[0])
    assert drain(gate) == ([], "done")


def test_refetch_without_result_falls_back_to_original_link():
    """🔴 重取无果（源站没给新链接）时必须沿用原链接，不能让片凭空消失。"""
    r = Refetcher()
    src = Src([pal("p1", age_min=200)])
    gate = make_gate(src, refetcher=r)
    assert drain(gate) == ([], "wait")

    r.pending.discard("p1")
    got, _ = drain(gate)
    assert ids(got) == ["p1"]
    assert got[0]["fetched_at"] == NOW - 200 * 60    # 原链接
    assert r.dispatched == ["p1"]                    # 没有再投一次
    assert gate.stats["jit_no_result"] == 1


def test_result_still_in_source_buffer_is_not_mistaken_for_no_result():
    """结果已出取流线程、但还压在来源缓冲里：不能当成无果去派原链接。"""
    r = Refetcher()
    src = Src([pal("p1", age_min=200)], closed=False)
    gate = make_gate(src, refetcher=r)
    drain(gate)
    r.pending.discard("p1")
    src.buffered.add("p1")
    assert drain(gate) == ([], "wait")
    assert gate.stats["jit_no_result"] == 0


def test_refetch_budget_exhausted_dispatches_original_link():
    r = Refetcher()
    src = Src([pal("p1", age_min=200)])
    gate = make_gate(src, refetcher=r)
    gate._jit_counts["p1"] = 2                      # 本例上限 2，已用完
    got, _ = drain(gate)
    assert ids(got) == ["p1"] and r.dispatched == []
    assert gate.stats["jit_budget_out"] == 1


def test_no_refetch_when_source_cannot_deliver_results():
    """来源回送不了重取结果（第 2 轮起的 list 来源）时不能做派发前重取，
    否则新链接回不来，闸门会一直等。"""
    r = Refetcher()
    src = Src([pal("p1", age_min=200)], delivers=False)
    got, _ = drain(make_gate(src, refetcher=r))
    assert ids(got) == ["p1"] and r.dispatched == []


def test_deferred_entry_is_checked_at_dispatch_not_when_deferred():
    """让行时不重取（排几个小时又会过期），轮到派发那一刻才查年龄。"""
    r = Refetcher()
    clock = Clock()
    src = Src([pal("p1", age_min=1), pal("p2", age_min=1), pal("p3", age_min=1)])
    gate = make_gate(src, refetcher=r, clock=clock)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2"] and r.dispatched == []

    clock.advance(200)                             # p3 在让行队列里放旧了
    gate.release(got[0])
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["p3"]


def test_refetched_entry_rejoins_queue_ahead_of_later_arrivals():
    """让行过的片送去重取，新链接回来时回到队首，不丢排队位置。"""
    r = Refetcher()
    clock = Clock()
    src = Src([pal("p1"), pal("p2", age_min=170), pal("p3")], closed=False, refetcher=r)
    gate = d.HostCapGate(_limits(max_concurrent=1), refetcher=r, clock=clock)
    gate.set_source(src)
    got, _ = drain(gate)
    assert ids(got) == ["p1"]                     # p2 p3 让行
    clock.advance(20)                             # p2 年龄 190 > 180
    gate.release(got[0])
    got2, _ = drain(gate)
    assert r.dispatched == ["p2"]
    assert ids(got2) == ["p3"]                    # p2 去换链接，空位给 p3
    # p2 的新链接回来时 p3 还在下：回到队首，排在后来的 p5 前面。
    src.items += [pal("p5"), pal("p2", age_min=0)]
    drain(gate)
    gate.release(got2[0])
    assert ids(drain(gate)[0]) == ["p2"]


# ------------------------------------------------------------ mp4 兜底

def test_mp4_fallback_after_long_wait_moves_mp4_first_and_keeps_paleoak():
    clock = Clock()
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True), pal("p4")])
    gate = make_gate(src, clock=clock)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2"]

    clock.advance(119)
    assert drain(gate) == ([], "wait")
    clock.advance(2)
    got2, state = drain(gate)
    assert ids(got2) == ["p3"] and state == "wait"
    urls = got2[0]["urls"]
    assert HAKUNA in urls[0]["url"] and PALEO in urls[1]["url"]
    assert gate.stats["mp4_fallback"] == 1
    # 兜底不占名额：p1 p2 仍占着两个位，p4（无 mp4）继续让行。
    gate.release(got2[0])
    assert drain(gate) == ([], "wait")
    gate.release(got[0])
    assert ids(drain(gate)[0]) == ["p4"]


def test_mp4_fallback_disabled_when_minutes_is_zero(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", _limits(mp4_fallback_after_minutes=0))
    clock = Clock()
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True)])
    gate = make_gate(src, clock=clock)
    drain(gate)
    clock.advance(600)
    assert drain(gate) == ([], "wait")


def test_mp4_fallback_refetches_stale_links_and_keeps_its_wait_time():
    """兜底前同样查年龄；换回新链接后不重新计时，下一次直接兜底。"""
    r = Refetcher()
    clock = Clock()
    src = Src([pal("p1"), pal("p2"), pal("p3", age_min=0, mp4=True)],
              closed=False, refetcher=r)
    monkeypatch_limits = _limits(mp4_fallback_after_minutes=150)
    gate = d.HostCapGate(monkeypatch_limits, refetcher=r, clock=clock)
    gate.set_source(src)
    drain(gate)
    clock.advance(140)                             # 让行 140 < 150，还不到兜底
    assert drain(gate) == ([], "wait")
    clock.advance(50)                              # 让行 190：该兜底了，但链接 190 > 180
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["p3"]
    assert gate.stats["mp4_fallback"] == 0

    fresh = pal("p3", mp4=True)
    fresh["fetched_at"] = clock.t
    src.items.append(fresh)
    got, _ = drain(gate)
    assert ids(got) == ["p3"] and HAKUNA in got[0]["urls"][0]["url"]


# ------------------------------------------------------------ 链接年龄与启动预检

def test_is_stale_entry_uses_stricter_host_threshold(monkeypatch):
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 43200)
    now = NOW
    assert d.is_stale_entry(pal("1", provider="vidup", age_min=121), now)
    assert not d.is_stale_entry(pal("1", provider="vidup", age_min=119), now)
    slow_mp4 = fast("2")
    slow_mp4["fetched_at"] = NOW - 121 * 60
    assert not d.is_stale_entry(slow_mp4, now)
    slow_mp4["fetched_at"] = NOW - 13 * 3600
    assert d.is_stale_entry(slow_mp4, now)


def test_startup_precheck_leaves_capped_host_entries_to_the_gate(monkeypatch):
    """pipeline 模式的启动预检不投 paleoak 片：它们由闸门在派发时重取。"""
    r = Refetcher()
    monkeypatch.setattr(d, "async_refetch_hook", r)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 43200)
    old_fast = fast("f1")
    old_fast["fetched_at"] = int(time.time()) - 13 * 3600
    old_pal = pal("p1")
    old_pal["fetched_at"] = int(time.time()) - 13 * 3600
    assert d.dispatch_stale_entries_async([old_pal, old_fast], {}) == 1
    assert r.dispatched == ["f1"]


def test_parse_host_limits_zero_cap_disables_whole_host():
    assert d._parse_host_limits({PALEO: {"max_concurrent": 0}}) == {}
    assert d._parse_host_limits(None) == {}
    cfg = _limits()[PALEO]
    assert cfg["refetch_after_seconds"] == {"vidup": 7200, "videasy": 10800}
    assert cfg["mp4_fallback_after_seconds"] == 7200


def test_shipped_config_values():
    """config.yaml 里这次定下的取值（2026-10-24 与用户确认）。"""
    shipped = d._parse_host_limits(d._CFG.get("host_limits"))[PALEO]
    assert shipped["max_concurrent"] == 6
    assert shipped["refetch_after_seconds"] == {"vidup": 7200, "videasy": 10800}
    assert shipped["mp4_fallback_after_seconds"] == 7200
    assert shipped["max_deferred"] == 1000
    assert shipped["playlist_500_means_expired"] is True
    assert d._CFG["max_workers"] == 20
    assert d._CFG["download_queue_depth"] == 40


# ------------------------------------------------------------ 播放列表 500

class _Resp:
    def __init__(self, status):
        self.status_code = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        error = requests.HTTPError(f"{self.status_code} Server Error")
        error.response = self
        raise error


class _Session:
    def __init__(self, status):
        self.status = status

    def request(self, *args, **kwargs):
        return _Resp(self.status)


@pytest.mark.parametrize("parse", ["master", "media"])
def test_capped_host_playlist_500_is_marked_for_refetch(parse, monkeypatch):
    """vidup 在 paleoak 上的过期链接返回 500：必须挂需重新取流。"""
    monkeypatch.setattr(d, "get_session", lambda: _Session(500))
    monkeypatch.setattr(d, "PLAYLIST_RETRY_MAX", 1)
    url = f"https://{PALEO}/b3f8c21a/x/master.m3u8"
    with pytest.raises(RuntimeError) as excinfo:
        if parse == "master":
            d.parse_master_playlist(url)
        else:
            d.parse_media_playlist(url)
    message = str(excinfo.value)
    assert d.needs_refetch(message)
    assert not d._classify_failure(message)


@pytest.mark.parametrize("url,status,limits", [
    ("https://moon.zenoak.top/x/master.m3u8", 500, None),     # 别的主机
    (f"https://{PALEO}/x/master.m3u8", 502, None),            # 别的 5xx
    (f"https://{PALEO}/x/master.m3u8", 500, {}),              # 功能关闭
])
def test_other_playlist_5xx_stays_transient(url, status, limits, monkeypatch):
    if limits is not None:
        monkeypatch.setattr(d, "HOST_LIMITS", limits)
    monkeypatch.setattr(d, "get_session", lambda: _Session(status))
    with pytest.raises(RuntimeError) as excinfo:
        d.parse_master_playlist(url, retries=1)
    assert not d.needs_refetch(str(excinfo.value))


def test_segment_500_on_capped_host_stays_transient(monkeypatch):
    """分片不走播放列表包装：过载时的分片 500 仍是瞬时错误、照常重试。"""
    monkeypatch.setattr(d, "get_session", lambda: _Session(500))
    with pytest.raises(RuntimeError) as excinfo:
        d.request_with_retry("GET", f"https://{PALEO}/r6/s/seg", retries=1)
    assert not d.needs_refetch(str(excinfo.value))


# ------------------------------------------------------------ 节点循环的额外占用

def _node_loop_env(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_mp4_preflight", lambda *a, **k: None)
    seen = []

    def fake_mp4(node, *a, **k):
        raise RuntimeError("mp4 boom")

    def fake_master(url, retries=None, headers=None):
        seen.append(d.host_extra_active(PALEO))
        raise RuntimeError("paleoak boom")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_mp4)
    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    return seen


def test_falling_back_onto_capped_host_counts_as_extra(tmp_path, monkeypatch):
    """mp4 兜底失败回落到 paleoak：下载期间记一笔额外占用，结束必须归还。"""
    seen = _node_loop_env(tmp_path, monkeypatch)
    entry = d._mp4_first(pal("p1", mp4=True))
    _, ok, _ = d.process_one_entry(entry, set())
    assert ok is False
    assert seen == [1]
    assert d.host_extra_active(PALEO) == 0


def test_first_node_on_capped_host_is_not_double_counted(tmp_path, monkeypatch):
    """第一个节点就在 paleoak：闸门已占名额，节点循环不再另记。"""
    seen = _node_loop_env(tmp_path, monkeypatch)
    _, ok, _ = d.process_one_entry(pal("p1", mp4=True), set())
    assert ok is False
    assert seen == [0]
    assert d.host_extra_active(PALEO) == 0


# ------------------------------------------------------------ 端到端：主循环

def _run_pipeline_bounded(timeout=30):
    """在线程里跑 _run_pipeline，超时即判失败。

    闸门若漏还名额或收不了尾，主循环会永远等下去；直接调用会让整个测试套件
    挂死而不是报错。线程设为 daemon，超时后随测试进程退出。
    """
    outcome = {}

    def _target():
        try:
            d._run_pipeline()
        except BaseException as exc:  # noqa: BLE001
            outcome["exc"] = exc

    worker = threading.Thread(target=_target, daemon=True)
    worker.start()
    worker.join(timeout)
    assert not worker.is_alive(), f"_run_pipeline {timeout}s 内没有收尾（闸门卡死？）"
    if "exc" in outcome:
        raise outcome["exc"]


def _streaming_env(tmp_path, monkeypatch, items, workers=8, depth=16):
    for name, filename in (
        ("FAILED_LOG", "failed.jsonl"), ("SUCCESS_LOG", "success.jsonl"),
        ("DOWNLOAD_OK_LOG", "download_ok.jsonl"),
        ("DOWNLOAD_FAIL_LOG", "download_fail.jsonl"),
        ("UPLOAD_PENDING_LOG", "upload_pending.jsonl"),
    ):
        monkeypatch.setattr(d, name, str(tmp_path / filename))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "nope.jsonl"))
    monkeypatch.setattr(d, "clean_temp_directory", lambda: None)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", False)
    monkeypatch.setattr(d, "async_refetch_hook", None)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)
    monkeypatch.setattr(d, "MAX_WORKERS", workers)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", depth)
    monkeypatch.setattr(d, "HOST_LIMITS", _limits(max_concurrent=3, max_deferred=1000))

    source = Src(items, closed=False)
    # 模拟取流线程：开头先给全部片，随后收工。
    source.closed = True
    monkeypatch.setattr(d, "ListEntrySource", lambda entries: source)
    return source


def test_pipeline_never_exceeds_cap_and_finishes_every_film(tmp_path, monkeypatch):
    """🔒 端到端：8 个槽位、paleoak 上限 3。

    - paleoak 同时在下的片从不超过 3，且确实跑到了 3；
    - 排在 paleoak 后面的快片不被堵住（在最后一部 paleoak 开始前就都开始了）；
    - 每部片恰好处理一次，本轮正常收尾（让行的片没有被丢掉）。
    """
    items = [pal(f"p{i}") for i in range(12)] + [fast(f"f{i}") for i in range(12)]
    _streaming_env(tmp_path, monkeypatch, items)
    lock = threading.Lock()
    active = {"n": 0, "peak": 0}
    started = []
    rng = random.Random(7)

    def fake_process(entry, processed_ids):
        is_pal = PALEO in entry["urls"][0]["url"]
        with lock:
            started.append(entry["tmdbId"])
            if is_pal:
                active["n"] += 1
                active["peak"] = max(active["peak"], active["n"])
        time.sleep(rng.uniform(0.02, 0.06) * (3 if is_pal else 1))
        with lock:
            if is_pal:
                active["n"] -= 1
        return entry["tmdbId"], False, {"error": "stub", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    _run_pipeline_bounded()

    assert sorted(started) == sorted(ids(items))
    assert active["peak"] == 3
    last_pal_start = max(i for i, t in enumerate(started) if t.startswith("p"))
    last_fast_start = max(i for i, t in enumerate(started) if t.startswith("f"))
    assert last_fast_start < last_pal_start


def test_pipeline_interrupt_with_deferred_films_exits_cleanly(tmp_path, monkeypatch):
    """Ctrl+C 时让行队列里还有片：照常抛出中断、不挂起（让行的片在 results.jsonl
    里，下次是存量，不丢）。"""
    items = [pal(f"p{i}") for i in range(10)]
    source = _streaming_env(tmp_path, monkeypatch, items)
    real_poll = source.poll

    def poll_then_interrupt():
        if source.polls >= 10:
            raise KeyboardInterrupt
        return real_poll()

    source.poll = poll_then_interrupt
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, ids_: (time.sleep(0.05), (entry["tmdbId"], False,
                                                {"error": "stub", "retriable": False}))[1],
    )
    started = time.time()
    with pytest.raises(KeyboardInterrupt):
        _run_pipeline_bounded()
    assert time.time() - started < 10


# ------------------------------------------------------------ pipeline.py 的配套接口

def test_async_refetcher_is_pending_covers_inflight_and_unclaimed(monkeypatch, tmp_path):
    """is_pending 要同时覆盖「在途」与「结果已出、未被收走」，中间不能有空窗。"""
    gate_open = threading.Event()

    def fetch(tid, providers=None):
        gate_open.wait(timeout=5)
        return "ok", {"tmdbId": tid, "urls": [{"url": "new"}], "fetched_at": 1}

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", fetch)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop)
    r.start()
    try:
        r.dispatch([{"tmdbId": "7"}])
        assert r.is_pending("7")                 # 在途
        gate_open.set()
        deadline = time.time() + 5
        while time.time() < deadline and r.pending_count():
            time.sleep(0.01)
        assert r.pending_count() == 0
        assert r.is_pending("7")                 # 已出结果、还没被收走
        assert [e["tmdbId"] for e in r.collect()] == ["7"]
        assert not r.is_pending("7")
    finally:
        gate_open.set()
        stop.set()


def test_async_refetcher_is_pending_false_after_no_result(monkeypatch):
    monkeypatch.setattr(p.fetcher, "process_tmdb_id",
                        lambda tid, providers=None: ("dead", None))
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop)
    r.start()
    try:
        r.dispatch([{"tmdbId": "8"}])
        deadline = time.time() + 5
        while time.time() < deadline and r.is_pending("8"):
            time.sleep(0.01)
        assert not r.is_pending("8")
        assert r.collect() == []
    finally:
        stop.set()


def test_queue_source_reports_refetch_capability_and_buffer():
    class _R:
        def __init__(self):
            self.results = []

        def collect(self):
            out, self.results = self.results, []
            return out

        def pending_count(self):
            return 0

    import queue as _queue
    assert p.QueueEntrySource(_queue.Queue()).delivers_refetch_results is False
    r = _R()
    busy = {"1"}
    source = p.QueueEntrySource(_queue.Queue(), refetcher=r, is_busy=lambda k: k in busy)
    assert source.delivers_refetch_results is True
    r.results = [{"tmdbId": "1", "urls": ["new"]}]
    assert source.poll() == ("wait", None)       # 旧链接还在下：压在缓冲里
    assert source.has_buffered("1")
    busy.clear()
    assert source.poll()[1]["tmdbId"] == "1"
    assert not source.has_buffered("1")
