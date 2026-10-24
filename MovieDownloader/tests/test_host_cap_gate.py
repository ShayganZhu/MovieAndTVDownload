"""host_limits（按 CDN 主机的并发上限 + 让行队列，2026-10-24）的离线用例。

背景：sun.paleoak.top 回源过载时，慢片占满下载槽位、把快片饿死。HostCapGate
把这类片的在途数压在上限内，多出的整片让行；派发那一刻链接太旧就先重取；
让行太久且有 mp4 的片改走 mp4。

重点锁死几件错了会静默丢片 / 卡死 / 白烧配额的事：
  - 让行的片本轮内必须下完：来源取空后有让行就报 wait，不能 done；
  - 名额必须归还，否则闸门永久关死；
  - 派发前重取无果时必须沿用原链接，不能让片消失；
  - 排队超过告警线也照收不误、不阻塞（阻塞会把另一条通道一起堵死）。
分通道（专用槽位、分档放量）与节点降级的用例见 test_download_lanes.py。
"""

import random
import threading
import time
from concurrent.futures import Future

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
        "playlist_500_expired_after_minutes": 150,
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


class Probe:
    """mp4 兜底画质评估的替身：按 tmdbId 给结论，记下被评估了几次。

    verdicts[id] 为 "ok" / "lower" / "unknown"，或 None 表示评估还没跑完。
    """

    def __init__(self, default="ok"):
        self.default = default
        self.verdicts = {}
        self.calls = []
        self.pending = {}

    def __call__(self, entry):
        key = entry["tmdbId"]
        self.calls.append(key)
        future = Future()
        verdict = self.verdicts.get(key, self.default)
        if verdict is None:
            self.pending[key] = future
        else:
            future.set_result((verdict, f"stub {verdict}"))
        return future

    def finish(self, key, verdict):
        self.pending.pop(key).set_result((verdict, f"stub {verdict}"))


def make_gate(src, refetcher=None, extra=None, clock=None, probe=None, limits=None):
    gate = d.HostCapGate(
        limits or d.HOST_LIMITS, refetcher=refetcher,
        extra_active=(lambda host: extra.get(host, 0)) if extra is not None else None,
        clock=clock or Clock(),
        probe_submit=probe or Probe(),
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


def test_extra_occupancy_does_not_take_dedicated_slots():
    """节点循环中途切到 paleoak 的普通片占的是普通槽位，不占 paleoak 专用名额。

    2026-10-24 前它算进名额：zenoak /r2/ 全部 502 后回落到 paleoak 的片把名额
    占满（在途 0(+7)/6），paleoak 打头的片 235 部只下完 19 部。
    """
    extra = {PALEO: 5}
    src = Src([pal("p1"), pal("p2")])
    gate = make_gate(src, extra=extra)
    got, state = drain(gate)
    assert ids(got) == ["p1", "p2"] and state == "done"


def test_entry_whose_second_node_is_capped_is_not_held_back():
    """只看第一个节点：paleoak 只是备用节点的片不受上限约束。"""
    entry = fast("f1")
    entry["urls"].append({"url": f"https://{PALEO}/r6/f1/master.m3u8",
                          "provider": "videasy", "type": "m3u8"})
    src = Src([pal("p1"), pal("p2"), entry])
    got, _ = drain(make_gate(src))
    assert ids(got) == ["p1", "p2", "f1"]


def test_queue_beyond_warn_line_keeps_accepting_and_does_not_block(capsys):
    """🔴 排队超过 max_deferred 只告警、不阻塞：后面的快片照常派发，一部不丢。

    2026-10-24 前到上限就停止从来源取片，排在后面的普通片也一起被堵死；
    23 号机的排队按 40~65 部/小时增长，这次运行内必然撞上。
    """
    src = Src([pal(f"p{i}") for i in range(1, 7)] + [fast("f7")])
    gate = make_gate(src)          # 上限 2，告警线 3
    got, state = drain(gate)
    assert ids(got) == ["p1", "p2", "f7"] and state == "wait"
    assert src.items == []
    assert "排队已 4 部（告警线 3）" in capsys.readouterr().out

    for entry in got[:2]:
        gate.release(entry)
    got2, _ = drain(gate)
    assert ids(got2) == ["p3", "p4"]           # 先来先走，排队的片都还在


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


def test_partial_refetch_keeps_old_hls_node_in_its_original_position():
    """🔴 重取时 videasy 瞬时缺席（videasy-seed 403）：不能把 HLS 节点换丢。

    旧 paleoak 节点补回原位（仍排第一），新 mp4 节点替换旧的；补回的旧节点带着
    自己的取流时刻，条目时刻仍是新结果的；这次直接派发，不立刻再重取一遍。
    """
    r = Refetcher()
    src = Src([pal("p1", age_min=200, mp4=True)], closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r)
    assert drain(gate) == ([], "wait")
    fresh = fast("p1")                              # 新结果只有 vidlink
    fresh["urls"][0]["url"] = f"https://{HAKUNA}/p1-new.mp4"
    fresh["fetched_at"] = NOW
    src.items.append(fresh)
    got, _ = drain(gate)
    assert ids(got) == ["p1"]
    urls = [u["url"] for u in got[0]["urls"]]
    assert urls == [f"https://{PALEO}/r6/p1/master.m3u8", f"https://{HAKUNA}/p1-new.mp4"]
    assert got[0]["fetched_at"] == NOW
    assert got[0]["urls"][0]["fetched_at"] == NOW - 200 * 60       # 旧节点按旧时刻
    assert "fetched_at" not in got[0]["urls"][1]                    # 新节点按条目时刻
    assert d.node_link_age(got[0], d._normalize_url_entry(got[0]["urls"][0]), NOW) == 200 * 60
    assert r.dispatched == ["p1"]
    assert gate.stats["jit_partial"] == 1


def test_partial_refetch_that_has_to_wait_is_not_refetched_again():
    """🔴 补回旧节点后又要排队：轮到它时不再按旧节点年龄重取，直接派发。

    2026-10-24 23 号机：499580 的重取结果缺了 vidup，补回旧 paleoak 节点后因排队
    非空又进排队，轮到时按旧节点 120+ 分钟再重取——120/122/124 分钟连重 3 次，
    几分钟内把额度烧光，换回的还是同样残缺的结果。
    """
    r = Refetcher()
    src = Src([pal("p0"), pal("p1", age_min=200, mp4=True)], closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r, limits=_limits(max_concurrent=1))
    got, _ = drain(gate)
    assert ids(got) == ["p0"]                       # p1 排队
    gate.release(got[0])
    assert drain(gate) == ([], "wait")              # p1 轮到时太旧 → 去重取
    partial = fast("p1")
    partial["fetched_at"] = NOW
    src.items += [pal("p9"), partial]               # p9 先占住名额，p1 的残缺结果后到
    got, _ = drain(gate)
    assert ids(got) == ["p9"]                       # p1 补回旧节点后排队
    gate.release(got[0])
    got, _ = drain(gate)
    assert ids(got) == ["p1"]                       # 轮到时直接派发
    assert PALEO in got[0]["urls"][0]["url"]        # 补回的 HLS 节点仍排第一
    assert r.dispatched == ["p1"]
    assert gate.stats["jit_partial"] == 1


def test_skip_after_partial_refetch_applies_only_once():
    """「不再查年龄」只管补回后的那一次派发：之后下载失败再回来，照常按年龄重取。"""
    r = Refetcher()
    src = Src([pal("p1", age_min=200, mp4=True)], closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r)
    assert drain(gate) == ([], "wait")              # 太旧 → 去重取
    partial = fast("p1")
    partial["fetched_at"] = NOW
    src.items.append(partial)
    got, _ = drain(gate)
    assert ids(got) == ["p1"]                       # 补回旧节点，直接派发
    gate.release(got[0])
    src.items.append(got[0])                        # 下载失败，带着旧节点又回来
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["p1", "p1"]


def test_merge_missing_providers_only_when_something_is_missing():
    old = pal("x", age_min=200, mp4=True)
    full = pal("x", age_min=0, mp4=True)
    merged, changed = d._merge_missing_providers(full, old)
    assert merged is full and changed is False
    extra = pal("x", age_min=0)
    extra["urls"].append({"url": "https://moon.zenoak.top/x.m3u8",
                          "provider": "vidup", "type": "m3u8"})
    merged, changed = d._merge_missing_providers(extra, old)
    # 旧次序：videasy, vidlink；新结果多出的 vidup 接在最后。
    assert changed is True
    assert [u["provider"] for u in merged["urls"]] == ["videasy", "vidlink", "vidup"]


def test_no_refetch_for_films_whose_first_node_is_not_capped():
    """paleoak 只是备用节点的快片：不为它拦下重取，免得换丢好好的第一节点。"""
    r = Refetcher()
    entry = fast("f1")
    entry["urls"].append({"url": f"https://{PALEO}/r6/f1/master.m3u8",
                          "provider": "videasy", "type": "m3u8"})
    entry["fetched_at"] = NOW - 300 * 60
    src = Src([entry], refetcher=r)
    got, _ = drain(make_gate(src, refetcher=r))
    assert ids(got) == ["f1"] and r.dispatched == []
    assert d.capped_refetch_policy(entry) is None


def test_failed_film_returning_from_refetch_keeps_its_queue_position():
    """下载失败、重取回来的片按第一次进闸门的时刻排回原位，不去队尾重新排。"""
    clock = Clock()
    src = Src([pal("p1"), pal("p2"), pal("p3")], closed=False)
    gate = make_gate(src, clock=clock, limits=_limits(max_concurrent=1))
    got, _ = drain(gate)
    assert ids(got) == ["p1"]                       # p2 p3 让行
    clock.advance(10)
    gate.release(got[0])
    got, _ = drain(gate)
    assert ids(got) == ["p2"]
    clock.advance(10)
    src.items.append(pal("p4"))                     # 后来的 p4
    drain(gate)
    gate.release(got[0])                            # p2 下载失败
    got, _ = drain(gate)
    assert ids(got) == ["p3"]
    clock.advance(10)
    src.items.append(pal("p2"))                     # p2 的新链接回来
    drain(gate)
    gate.release(got[0])
    assert ids(drain(gate)[0]) == ["p2"]            # 排在 p4 前面


def test_budget_exhausted_is_counted_once_per_film():
    clock = Clock()
    r = Refetcher()
    src = Src([pal("p0"), pal("p1", age_min=200)], closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r, clock=clock, limits=_limits(max_concurrent=1))
    gate._jit_counts["p1"] = 2
    got, _ = drain(gate)
    gate.release(got[0])
    got, _ = drain(gate)                            # 额度用尽，原链接派出
    assert ids(got) == ["p1"]
    src.items.append(pal("p1", age_min=200))        # 失败后同一部又回来
    drain(gate)
    gate.release(got[0])
    drain(gate)
    assert gate.stats["jit_budget_out"] == 1


# ------------------------------------------------------------ mp4 兜底

def test_mp4_fallback_after_long_wait_moves_mp4_first_and_keeps_paleoak():
    clock = Clock()
    probe = Probe("ok")
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True), pal("p4")])
    gate = make_gate(src, clock=clock, probe=probe)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2"]

    clock.advance(119)
    assert drain(gate) == ([], "wait")
    assert probe.calls == []                       # 没到时间不评估
    clock.advance(2)
    assert drain(gate) == ([], "wait")             # 本次只投后台评估，不等结论
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


def test_mp4_fallback_waits_when_mp4_is_lower_quality():
    """🔴 画质门槛：mp4 比 HLS 低就不兜底，继续等 HLS 名额，且不反复评估。"""
    clock = Clock()
    probe = Probe("lower")
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True)])
    gate = make_gate(src, clock=clock, probe=probe)
    got, _ = drain(gate)
    clock.advance(121)
    assert drain(gate) == ([], "wait")
    clock.advance(600)
    assert drain(gate) == ([], "wait")
    assert probe.calls == ["p3"]
    assert gate.stats["fallback_lower"] == 1 and gate.stats["mp4_fallback"] == 0
    gate.release(got[0])
    got2, _ = drain(gate)
    assert ids(got2) == ["p3"] and PALEO in got2[0]["urls"][0]["url"]   # 走 HLS 原顺序


def test_mp4_fallback_unknown_quality_is_re_evaluated_later():
    """测不出就不兜底（宁可多等不掉画质），过一阵再评估。"""
    clock = Clock()
    probe = Probe("unknown")
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True)])
    gate = make_gate(src, clock=clock, probe=probe)
    drain(gate)
    clock.advance(121)
    assert drain(gate) == ([], "wait")             # 投评估
    assert drain(gate) == ([], "wait")             # 收到「测不出」
    clock.advance(d.FALLBACK_PROBE_RETRY_SECONDS / 60 - 1)
    assert drain(gate) == ([], "wait")
    assert probe.calls == ["p3"]
    probe.verdicts["p3"] = "ok"
    clock.advance(2)
    drain(gate)                                    # 到点重新评估（本次投递）
    got, _ = drain(gate)                           # 结论回来
    assert probe.calls == ["p3", "p3"]
    assert ids(got) == ["p3"] and HAKUNA in got[0]["urls"][0]["url"]


def test_mp4_fallback_does_not_block_main_loop_while_probing():
    """评估在后台跑：没出结论前 poll 照常返回，不等它。"""
    clock = Clock()
    probe = Probe()
    probe.verdicts["p3"] = None                    # 评估迟迟不出结论
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True), fast("f4")])
    gate = make_gate(src, clock=clock, probe=probe)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2", "f4"]
    clock.advance(121)
    assert drain(gate) == ([], "wait")
    assert drain(gate) == ([], "wait")
    assert probe.calls == ["p3"]                   # 在途期间不重复投
    probe.finish("p3", "ok")
    assert ids(drain(gate)[0]) == ["p3"]


def test_mp4_fallback_probe_concurrency_is_bounded():
    clock = Clock()
    probe = Probe()
    for i in range(10):
        probe.verdicts[f"m{i}"] = None
    src = Src([pal("p1"), pal("p2")] + [pal(f"m{i}", mp4=True) for i in range(10)])
    gate = make_gate(src, clock=clock, probe=probe,
                     limits=_limits(max_deferred=100))
    drain(gate)
    clock.advance(121)
    drain(gate)
    assert len(probe.calls) == d.FALLBACK_PROBE_MAX_INFLIGHT


def test_mp4_fallback_disabled_when_minutes_is_zero(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", _limits(mp4_fallback_after_minutes=0))
    clock = Clock()
    probe = Probe("ok")
    src = Src([pal("p1"), pal("p2"), pal("p3", mp4=True)])
    gate = make_gate(src, clock=clock, probe=probe)
    drain(gate)
    clock.advance(600)
    assert drain(gate) == ([], "wait")
    assert probe.calls == []


def test_mp4_fallback_refetches_stale_links_and_keeps_its_wait_time():
    """兜底前先查年龄（过期链接评估也只会 401/500）；换回新链接后不重新计时。"""
    r = Refetcher()
    clock = Clock()
    probe = Probe("ok")
    src = Src([pal("p1"), pal("p2"), pal("p3", age_min=0, mp4=True)],
              closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r, clock=clock, probe=probe,
                     limits=_limits(mp4_fallback_after_minutes=150))
    drain(gate)
    clock.advance(140)                             # 让行 140 < 150，还不到兜底
    assert drain(gate) == ([], "wait")
    clock.advance(50)                              # 让行 190：该兜底了，但链接 190 > 180
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["p3"] and probe.calls == []
    assert gate.stats["mp4_fallback"] == 0

    fresh = pal("p3", mp4=True)
    fresh["fetched_at"] = clock.t
    src.items.append(fresh)
    drain(gate)                                    # 回到队里，投评估
    got, _ = drain(gate)
    assert probe.calls == ["p3"]
    assert ids(got) == ["p3"] and HAKUNA in got[0]["urls"][0]["url"]


def _fake_variants(*resolutions):
    return [(res, f"https://{PALEO}/v/{i}.m3u8", 0.0) for i, res in enumerate(resolutions)]


@pytest.mark.parametrize("hls,mp4,verdict", [
    (("1920x1080", "1280x720"), "1920x800", "ok"),     # 同为 1920 宽：宽银幕不算低
    (("1920x1080",), "1280x720", "lower"),
    (("3840x2160", "1920x1080"), "1920x1080", "lower"),
    (("unknown",), "1920x1080", "unknown"),            # HLS 没声明分辨率
])
def test_compare_fallback_quality(hls, mp4, verdict, monkeypatch):
    monkeypatch.setattr(d, "parse_master_playlist", lambda *a, **k: _fake_variants(*hls))
    monkeypatch.setattr(d, "_mp4_preflight",
                        lambda node, label, runtime=None: {"probed": (mp4, 0, 2000, "h264")})
    assert d.compare_fallback_quality(pal("x", mp4=True))[0] == verdict


def test_compare_fallback_quality_unknown_when_either_side_fails(monkeypatch):
    monkeypatch.setattr(d, "_mp4_preflight", lambda *a, **k: None)
    monkeypatch.setattr(d, "parse_master_playlist", lambda *a, **k: _fake_variants("1920x1080"))
    assert d.compare_fallback_quality(pal("x", mp4=True))[0] == "unknown"

    def boom(*a, **k):
        raise RuntimeError("HTTP 502")

    monkeypatch.setattr(d, "parse_master_playlist", boom)
    assert d.compare_fallback_quality(pal("x", mp4=True))[0] == "unknown"


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
    # paleoak 只是备用节点：按普通的 12 小时，不按 paleoak 的阈值。
    backup = fast("3")
    backup["urls"].append(pal("3")["urls"][0])
    backup["fetched_at"] = NOW - 300 * 60
    assert not d.is_stale_entry(backup, now)


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
    assert shipped["max_concurrent"] == 40
    assert shipped["ramp"] == {
        "steps": [16, 24, 32, 40], "step_seconds": 900.0,
        "backoff_breaker_trips": 3, "backoff_segment_fail_ratio": 0.02,
        "min_segment_requests": 2000, "backoff_playlist_fail_ratio": 0.2,
        "min_playlist_requests": 20, "step_up_min_load": 0.9, "recover_after_windows": 4,
    }
    assert shipped["refetch_after_seconds"] == {"vidup": 7200, "videasy": 10800}
    assert shipped["mp4_fallback_after_seconds"] == 7200
    assert shipped["max_deferred"] == 1000
    assert shipped["playlist_500_expired_after_seconds"] == 9000
    assert d._CFG["max_workers"] == 50
    assert d._CFG["download_queue_depth"] == 51
    assert d._parse_general_slots(d._CFG.get("general_slots")) == 10
    assert d._parse_demote_patterns(d._CFG.get("demote_nodes")) == ["moon.zenoak.top/r2/"]
    # 槽位合计不超过线程数：两条通道才真正互不占用。
    assert shipped["max_concurrent"] + 10 <= d._CFG["max_workers"] < d._CFG["download_queue_depth"]


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


def _http_500_error(url, status=500):
    """拿到 request_with_retry 对某状态码抛出的原样异常。"""
    import download_movies as dm
    real = dm.get_session
    dm.get_session = lambda: _Session(status)
    try:
        dm.request_with_retry("GET", url, retries=1, as_text=True)
    except RuntimeError as exc:
        return exc
    finally:
        dm.get_session = real
    raise AssertionError("expected RuntimeError")


@pytest.mark.parametrize("age_min,status,url,marked", [
    (160, 500, f"https://{PALEO}/b3f8c21a/x/master.m3u8", True),
    (149, 500, f"https://{PALEO}/b3f8c21a/x/master.m3u8", False),   # 新链接的过载 500
    (None, 500, f"https://{PALEO}/b3f8c21a/x/master.m3u8", False),  # 年龄未知
    (160, 502, f"https://{PALEO}/b3f8c21a/x/master.m3u8", False),   # 别的 5xx
    (160, 500, "https://moon.zenoak.top/x/master.m3u8", False),     # 别的主机
])
def test_playlist_500_counts_as_expiry_only_when_link_is_old(age_min, status, url, marked):
    exc = _http_500_error(url, status)
    age = None if age_min is None else age_min * 60
    expired = d._playlist_500_as_expired(exc, url, age)
    assert (expired is not None) is marked
    if marked:
        assert d.needs_refetch(str(expired))
        assert not d._classify_failure(str(expired))


def test_playlist_500_rule_off_when_host_limits_disabled(monkeypatch):
    url = f"https://{PALEO}/b3f8c21a/x/master.m3u8"
    exc = _http_500_error(url)
    monkeypatch.setattr(d, "HOST_LIMITS", {})
    assert d._playlist_500_as_expired(exc, url, 9999 * 60) is None


@pytest.mark.parametrize("age_min,marked", [(160, True), (10, False)])
def test_master_playlist_500_marking_reaches_node_failures(
        age_min, marked, tmp_path, monkeypatch):
    """接线：process_one_entry 里按这部片的 fetched_at 判年龄。"""
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "get_session", lambda: _Session(500))
    monkeypatch.setattr(d, "PLAYLIST_RETRY_MAX", 1)
    entry = pal("v1", provider="vidup")
    entry["fetched_at"] = int(time.time()) - age_min * 60
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert d.needs_refetch(info["node_failures"][0]["error"]) is marked


def test_segment_500_on_capped_host_stays_transient(monkeypatch):
    """分片不走播放列表那条判断：过载时的分片 500 仍是瞬时错误、照常重试。"""
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
    # 本组用例只验 paleoak 上限；普通槽位不限（分通道的端到端见 test_download_lanes.py）。
    monkeypatch.setattr(d, "GENERAL_SLOTS", None)

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


# ------------------------------------------------------------ 审查修复（2026-10-24 第二轮）

def test_waiting_film_with_lower_verdict_does_not_burn_refetch_budget():
    """🔴 评估结论为「mp4 更低、继续等」的片：等待期间链接过期也不为它重取，
    额度留给真正派发的那一刻。"""
    r = Refetcher()
    clock = Clock()
    probe = Probe("lower")
    src = Src([pal("p1"), pal("p2"), pal("p3", provider="vidup", age_min=0, mp4=True)],
              closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r, clock=clock, probe=probe)
    got, _ = drain(gate)
    clock.advance(121)                      # 让行满 120 分钟，链接也 121 分钟（vidup 阈值 120）
    drain(gate)
    assert r.dispatched == ["p3"]           # 为评估换一次新链接（额度还能留一次给派发）
    fresh = pal("p3", provider="vidup", mp4=True)
    fresh["fetched_at"] = clock.t
    src.items.append(fresh)
    drain(gate)                             # 回到原位，投评估
    drain(gate)                             # 结论：更低
    clock.advance(600)                      # 又等了 10 小时，链接早过期
    drain(gate)
    drain(gate)
    assert r.dispatched == ["p3"], "等待期间不能再为它重取"
    gate.release(got[0])
    assert drain(gate) == ([], "wait")      # 轮到派发：链接 600 分钟 → 派发前重取
    assert r.dispatched == ["p3", "p3"]
    assert gate.stats["jit_budget_out"] == 0


def test_probe_refetch_leaves_one_refetch_for_dispatch():
    """额度只剩一次时不为评估重取（也不拿过期链接去评估），这一次留给派发。"""
    r = Refetcher()
    clock = Clock()
    probe = Probe("ok")
    src = Src([pal("p1"), pal("p2"), pal("p3", provider="vidup", age_min=0, mp4=True)],
              closed=False, refetcher=r)
    gate = make_gate(src, refetcher=r, clock=clock, probe=probe)
    gate._jit_counts["p3"] = 1              # 本例上限 2，只剩 1 次
    got, _ = drain(gate)
    clock.advance(121)
    drain(gate)
    drain(gate)
    assert r.dispatched == [] and probe.calls == []
    gate.release(got[0])
    drain(gate)
    assert r.dispatched == ["p3"]


def test_gate_keeps_asking_source_after_it_reported_done():
    """来源报过 done 之后还可能来片（下面的 NodeRetrySource 收到换节点重下）：
    闸门不能记住 done 就再也不问。"""
    r = Refetcher()
    src = Src([pal("p1", age_min=200)], closed=True, refetcher=r)
    gate = make_gate(src, refetcher=r)
    assert drain(gate) == ([], "wait")      # p1 去重取；来源已 done
    r.pending.discard("p1")                 # 重取无果
    src.items.append(fast("f9"))            # 之后又来了一部（换节点重下）
    got, state = drain(gate)
    assert set(ids(got)) == {"p1", "f9"}


def test_refetch_check_uses_the_first_nodes_own_age():
    """补回的旧节点排第一：即使条目时刻是新的，也按旧节点的年龄判要不要重取。"""
    r = Refetcher()
    entry = pal("p1", age_min=0)
    entry["urls"][0]["fetched_at"] = NOW - 200 * 60
    src = Src([entry], refetcher=r)
    assert drain(make_gate(src, refetcher=r)) == ([], "wait")
    assert r.dispatched == ["p1"]


def test_playlist_500_judged_by_the_nodes_own_age(tmp_path, monkeypatch):
    """新取回的节点不因条目里混着旧节点就被算老；旧节点也不因条目时刻新就被算年轻。"""
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "get_session", lambda: _Session(500))
    monkeypatch.setattr(d, "PLAYLIST_RETRY_MAX", 1)
    entry = pal("v1", provider="vidup")
    entry["fetched_at"] = int(time.time())                     # 条目是新的
    entry["urls"][0]["fetched_at"] = int(time.time()) - 160 * 60   # 这个节点是补回的旧节点
    _, ok, info = d.process_one_entry(entry, set())
    assert d.needs_refetch(info["node_failures"][0]["error"])
