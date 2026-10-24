"""分通道派发（paleoak 专用槽位 + 普通槽位）、分档放量、节点降级（2026-10-24）。

背景（23 号机 2h43m 实测）：paleoak 上限 6 时每小时只下完 30~40 部、需求 ~137 部；
zenoak /r2/ 节点全部 502 后回落到 paleoak 的片又把 6 个名额占满，paleoak 打头
的片 235 部只下完 19 部。改成：50 槽位 = paleoak 专用 40（16→24→32→40 分档放量）
+ 普通 10，互不外借；/r2/ 节点排到最后。

重点锁死：
  - 两条通道各自不超上限、互不堵塞、互不外借；两条都满才停止从来源取片；
  - 放量只在这一档真跑满、源站也吃得消时升档；熔断 / 出问题的分片 / 播放列表
    失败超标就退一档并冷却，冷却够了能升回去（源站临时出事不会压低几天）；
  - 降级只改顺序、不删节点，派发分类与下载尝试顺序一致；
  - 端到端：每部片恰好处理一次、本轮正常收尾。
"""

import random
import threading
import time

import pytest
import requests

import download_movies as d
from test_host_cap_gate import (
    HAKUNA, NOW, PALEO, Clock, Probe, Refetcher, Src, _limits, _run_pipeline_bounded,
    _streaming_env, drain, fast, ids, pal,
)

R2 = "moon.zenoak.top"


def r2(tid, then=("paleoak",)):
    """第一个节点是 zenoak /r2/ 的片；then 决定后面跟哪些节点。"""
    urls = [{"url": f"https://{R2}/r2/cdn1/{tid}/playlist.m3u8",
             "provider": "vidup", "type": "m3u8"}]
    for kind in then:
        if kind == "paleoak":
            urls.append({"url": f"https://{PALEO}/b3f8c21a/{tid}/master.m3u8",
                         "provider": "vidup", "type": "m3u8"})
        elif kind == "mp4":
            urls.append({"url": f"https://{HAKUNA}/{tid}.mp4",
                         "provider": "vidlink", "type": "mp4"})
    return {"tmdbId": tid, "urls": urls}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", _limits())
    monkeypatch.setattr(d, "DEMOTE_NODE_PATTERNS", [f"{R2}/r2/"])


def lane_gate(src, general=2, cap=2, clock=None, probe=None, ramp=None, stats=None, extra=None,
              refetcher=None, general_cfg=None):
    limits = _limits(max_concurrent=cap, max_deferred=100)
    if ramp is not None:
        limits[PALEO]["ramp"] = ramp
    gate = d.HostCapGate(
        limits, clock=clock or Clock(), probe_submit=probe or Probe(),
        general=d._parse_general_lane(dict({"slots": general}, **(general_cfg or {}))),
        segment_stats=stats, refetcher=refetcher,
        extra_active=(lambda host: extra.get(host, 0)) if extra is not None else None,
    )
    gate.set_source(src)
    return gate


# ------------------------------------------------------------ 两条通道

def test_each_lane_respects_its_own_cap():
    src = Src([pal("p1"), pal("p2"), pal("p3"), fast("f1"), fast("f2"), fast("f3")])
    gate = lane_gate(src)
    got, state = drain(gate)
    assert ids(got) == ["p1", "p2", "f1", "f2"] and state == "wait"
    assert gate.in_flight(PALEO) == 2 and gate.in_flight(d.GENERAL_LANE) == 2


def test_full_general_lane_does_not_block_paleoak_behind_it():
    """🔴 队首的普通片在等普通槽位，后面的 paleoak 片照样派（不再队头阻塞）。"""
    src = Src([fast("f1"), fast("f2"), fast("f3"), fast("f4"), pal("p1")])
    got, _ = drain(lane_gate(src))
    assert ids(got) == ["f1", "f2", "p1"]


def test_full_paleoak_lane_does_not_block_general_behind_it():
    src = Src([pal("p1"), pal("p2"), pal("p3"), pal("p4"), fast("f1")])
    got, _ = drain(lane_gate(src))
    assert ids(got) == ["p1", "p2", "f1"]


def test_dedicated_slots_are_not_lent_to_the_other_lane():
    """paleoak 槽位全空，普通片也只能用自己的 2 个；反过来也一样。"""
    gate = lane_gate(Src([fast(f"f{i}") for i in range(5)]))
    got, _ = drain(gate)
    assert ids(got) == ["f0", "f1"] and gate.in_flight(PALEO) == 0
    gate = lane_gate(Src([pal(f"p{i}") for i in range(5)]))
    got, _ = drain(gate)
    assert ids(got) == ["p0", "p1"] and gate.in_flight(d.GENERAL_LANE) == 0


def test_both_lanes_full_stops_pulling_from_source():
    """两条通道都满就不再从来源取片：反压照样还给取流侧。"""
    src = Src([pal("p1"), fast("f1"), pal("p2"), fast("f2"), fast("f3")])
    gate = lane_gate(src, general=1, cap=1)
    got, state = drain(gate)
    assert ids(got) == ["p1", "f1"] and state == "wait"
    assert ids(src.items) == ["p2", "f2", "f3"]


def test_release_returns_the_slot_to_its_own_lane():
    src = Src([pal("p1"), pal("p2"), fast("f1"), fast("f2")])
    gate = lane_gate(src, general=1, cap=1)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "f1"]
    gate.release(got[1])                      # 还的是普通槽位
    assert ids(drain(gate)[0]) == ["f2"]      # 只有普通片能补上
    gate.release(got[0])
    assert ids(drain(gate)[0]) == ["p2"]


def test_general_film_returning_keeps_its_queue_position():
    """下载失败 / 重取回来的普通片按第一次进闸门的时刻排回原位。"""
    clock = Clock()
    src = Src([fast("f1"), fast("f2"), fast("f3")], closed=False)
    gate = lane_gate(src, general=1, clock=clock)
    got, _ = drain(gate)
    assert ids(got) == ["f1"]
    clock.advance(10)
    gate.release(got[0])
    got, _ = drain(gate)
    assert ids(got) == ["f2"]
    clock.advance(10)
    src.items.append(fast("f4"))
    drain(gate)
    gate.release(got[0])                      # f2 下载失败
    got, _ = drain(gate)
    assert ids(got) == ["f3"]
    src.items.append(fast("f2"))              # f2 新链接回来
    drain(gate)
    gate.release(got[0])
    assert ids(drain(gate)[0]) == ["f2"]      # 排在后来的 f4 前面


def test_mp4_fallback_takes_a_general_slot_and_waits_when_general_is_full():
    """兜底下的是 mp4，占普通槽位；普通槽位满时不评估也不兜底。"""
    clock = Clock()
    probe = Probe("ok")
    src = Src([pal("p1"), pal("p2", mp4=True), fast("f1")])
    gate = lane_gate(src, general=1, cap=1, clock=clock, probe=probe)
    got, _ = drain(gate)
    assert ids(got) == ["p1", "f1"]           # p2 在 paleoak 通道排队
    clock.advance(121)
    assert drain(gate) == ([], "wait")
    assert probe.calls == []                  # 普通槽位满：不评估
    gate.release(got[1])
    assert drain(gate) == ([], "wait")        # 有普通空位了：投评估
    got2, _ = drain(gate)
    assert ids(got2) == ["p2"] and HAKUNA in got2[0]["urls"][0]["url"]
    assert gate.in_flight(d.GENERAL_LANE) == 1 and gate.in_flight(PALEO) == 1
    gate.release(got2[0])
    assert gate.in_flight(d.GENERAL_LANE) == 0


def test_extra_occupancy_is_shown_but_does_not_block(capsys):
    gate = lane_gate(Src([pal("p1"), pal("p2"), fast("f1")]), extra={PALEO: 9})
    got, _ = drain(gate)
    assert ids(got) == ["p1", "p2", "f1"]
    gate.print_status(force=True)
    out = capsys.readouterr().out
    assert f"[让行] {PALEO} 专用槽位：在途 2/2" in out
    assert "节点切到该主机的普通片 9 部" in out
    assert "[让行] 普通槽位：在途 1/2" in out


def test_without_general_slots_general_lane_is_unlimited():
    """general_lane.slots 不配：普通片不限（分通道之前的行为）。"""
    gate = lane_gate(Src([fast(f"f{i}") for i in range(30)]), general=None)
    assert len(drain(gate)[0]) == 30


# ------------------------------------------------------------ 分档放量

RAMP = d._parse_ramp({"steps": [16, 24, 32, 40], "step_minutes": 15}, 40)


class Stats:
    """hls_host_stats 的替身：测试里直接往累计数上加。"""

    def __init__(self):
        self.total = dict.fromkeys(d._HLS_STAT_FIELDS, 0)

    def add(self, **amounts):
        for key, value in amounts.items():
            self.total[key] += value

    def __call__(self, host):
        return dict(self.total)


def make_ramp(clock, stats, **override):
    return d.LaneRamp(PALEO, dict(RAMP, **override), clock, stats)


def window(ramp, clock, stats, load="full", minutes=15, **amounts):
    """跑一个窗口：整段在途 load 部（默认 = 当前上限），期间累计 amounts，到点判档。"""
    ramp.observe(ramp.cap if load == "full" else load, clock.t)
    stats.add(**amounts)
    clock.advance(minutes)
    return ramp.tick()


def test_ramp_steps_up_when_the_lane_is_full_until_the_top(capsys):
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    caps = [ramp.cap]
    for _ in range(5):
        ramp.observe(ramp.cap, clock.t)
        clock.advance(14)
        assert ramp.tick() is False           # 不到 15 分钟不判
        stats.add(segments=6000, ok=6000, bytes=10**9, playlists=40)
        clock.advance(1)
        ramp.tick()
        caps.append(ramp.cap)
    assert caps == [16, 24, 32, 40, 40, 40]
    out = capsys.readouterr().out
    assert "[放量] sun.paleoak.top 上限 16 部的这 15 分钟：平均在途 16.0，分片 6000 个" in out
    assert "正常 → 升到 24 部" in out and "正常 → 保持 40 部" in out


def test_ramp_holds_when_the_lane_was_not_filled(capsys):
    """🔴 这一档的负载根本没压上去（片不够）就不升：升了等于没验证就放量。"""
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    window(ramp, clock, stats, load=5)
    assert ramp.cap == 16
    assert "正常但没跑满（平均在途 5.0 < 90% × 16）" in capsys.readouterr().out
    window(ramp, clock, stats, load=15)      # 15/16 ≥ 90%
    assert ramp.cap == 24


def test_ramp_load_is_time_weighted():
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    ramp.observe(16, clock.t)
    clock.advance(7.5)
    ramp.observe(0, clock.t)                  # 后半段片子都下完了
    clock.advance(7.5)
    ramp.tick()
    assert ramp.cap == 16                     # 平均 8 部，没跑满


def test_ramp_backs_off_then_recovers_after_cooldown():
    """🔴 退档后不再永久停住：源站临时出事恢复后，连续 4 个好窗口就能再升。"""
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    window(ramp, clock, stats)
    assert ramp.cap == 24
    assert window(ramp, clock, stats, trips=3) is True
    assert ramp.cap == 16 and ramp.cooldown_left == 4
    for left in (3, 2, 1):
        window(ramp, clock, stats)
        assert ramp.cap == 16 and ramp.cooldown_left == left
    window(ramp, clock, stats)                # 第 4 个好窗口：冷却结束，跑满就升
    assert ramp.cap == 24 and ramp.cooldown_left == 0


def test_repeated_backoff_from_the_same_level_doubles_the_cooldown():
    """某一档本身扛不住时越往后越少去试：同一档第 n 次退下来，冷却 4 × 2^(n-1)，最多 8 倍。"""
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    window(ramp, clock, stats)
    cooldowns = []
    for _ in range(5):
        assert ramp.cap == 24
        window(ramp, clock, stats, trips=3)
        cooldowns.append(ramp.cooldown_left)
        for _ in range(ramp.cooldown_left):
            window(ramp, clock, stats)
    assert cooldowns == [4, 8, 16, 32, 32]


def test_ramp_keeps_backing_off_but_never_below_the_first_step():
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    for _ in range(3):
        window(ramp, clock, stats)
    assert ramp.cap == 40
    caps = []
    for _ in range(4):
        window(ramp, clock, stats, trips=5)
        caps.append(ramp.cap)
    assert caps == [32, 24, 16, 16]


def test_ramp_segment_ratio_uses_troubled_segments_with_enough_samples():
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    window(ramp, clock, stats, segments=1000, seg_troubled=50)    # 5%，但只有 1000 个：不判
    assert ramp.cap == 24
    window(ramp, clock, stats, segments=5700, seg_troubled=6)     # 0.1%：正常
    assert ramp.cap == 32
    window(ramp, clock, stats, segments=3000, seg_troubled=100)   # 3.3%：退
    assert ramp.cap == 24 and ramp.cooldown_left == 4


def test_ramp_backs_off_on_playlist_failures_even_if_segments_look_clean(capsys):
    """🔴 过载时片子先死在播放列表 502 上，走不到分片：只看分片会一直以为没事。"""
    clock, stats = Clock(), Stats()
    ramp = make_ramp(clock, stats)
    window(ramp, clock, stats, playlists=10, playlist_fail=5)     # 不足 20 次：不判
    assert ramp.cap == 24
    window(ramp, clock, stats, segments=3000, playlists=32, playlist_fail=9)
    assert ramp.cap == 16
    assert "播放列表失败 28%" in capsys.readouterr().out


def test_gate_dispatches_queued_films_as_soon_as_the_cap_rises():
    clock, stats = Clock(), Stats()
    ramp = dict(RAMP, steps=[1, 2])
    src = Src([pal("p1"), pal("p2"), pal("p3")], closed=False)
    gate = lane_gate(src, cap=2, clock=clock, ramp=ramp, stats=stats)
    got, _ = drain(gate)
    assert ids(got) == ["p1"] and gate.cap(PALEO) == 1
    clock.advance(15)                          # p1 占满整个窗口 → 升档
    assert ids(drain(gate)[0]) == ["p2"]
    assert gate.cap(PALEO) == 2


def test_gate_does_not_step_up_a_lane_that_never_filled():
    clock, stats = Clock(), Stats()
    ramp = dict(RAMP, steps=[4, 8])
    src = Src([pal("p1")], closed=False)
    gate = lane_gate(src, cap=8, clock=clock, ramp=ramp, stats=stats)
    drain(gate)                                # 只有 1 部 paleoak 片，上限 4
    clock.advance(15)
    drain(gate)
    assert gate.cap(PALEO) == 4


def test_lowering_the_cap_does_not_cancel_in_flight_films():
    clock, stats = Clock(), Stats()
    ramp = dict(RAMP, steps=[1, 2])
    src = Src([pal(f"p{i}") for i in range(4)], closed=False)
    gate = lane_gate(src, cap=2, clock=clock, ramp=ramp, stats=stats)
    first = drain(gate)[0]
    clock.advance(15)
    second = drain(gate)[0]
    assert ids(first + second) == ["p0", "p1"] and gate.in_flight(PALEO) == 2
    stats.add(trips=3)
    clock.advance(15)
    assert drain(gate) == ([], "wait")
    assert gate.cap(PALEO) == 1 and gate.in_flight(PALEO) == 2   # 在下的不打断
    gate.release(first[0])
    assert drain(gate) == ([], "wait")         # 1 部在途 = 新上限，还不能放
    gate.release(second[0])
    assert ids(drain(gate)[0]) == ["p2"]


def test_parse_ramp_keeps_increasing_steps_and_ends_at_cap():
    parsed = d._parse_ramp({"steps": [24, 16, "x", 50, 32]}, 40)
    assert parsed["steps"] == [24, 32, 40]
    assert parsed["step_seconds"] == 900.0
    assert (parsed["backoff_playlist_fail_ratio"], parsed["min_playlist_requests"],
            parsed["step_up_min_load"], parsed["recover_after_windows"]) == (0.2, 20, 0.9, 4)
    assert d._parse_ramp({"steps": []}, 40) is None
    assert d._parse_ramp(None, 40) is None
    assert d._parse_host_limits({PALEO: {"max_concurrent": 6}})[PALEO]["ramp"] is None


def test_fit_lane_caps_lowers_dedicated_slots_to_fit_workers(capsys):
    limits = d._parse_host_limits({PALEO: {
        "max_concurrent": 40, "ramp": {"steps": [16, 24, 32, 40]}}})
    assert d.fit_lane_caps(limits, 10, 50)[PALEO] is limits[PALEO]
    fitted = d.fit_lane_caps(limits, 10, 30)[PALEO]
    assert fitted["max_concurrent"] == 20
    assert fitted["ramp"]["steps"] == [16, 20]
    assert limits[PALEO]["max_concurrent"] == 40          # 原配置不改
    assert "压到 20 部" in capsys.readouterr().out


def test_parse_general_slots_and_demote_patterns():
    assert d._parse_general_slots(10) == 10
    assert d._parse_general_slots(0) is None
    assert d._parse_general_slots(None) is None
    assert d._parse_general_slots("x") is None
    assert d._parse_demote_patterns(
        ["https://Moon.Zenoak.top/r2/", "moon.zenoak.top/r2/", "", None]
    ) == ["moon.zenoak.top/r2/"]


# ------------------------------------------------------------ 分片 / 播放列表计数（放量判档的依据）

@pytest.fixture
def clean_stats(monkeypatch):
    monkeypatch.setattr(d, "_hls_seg_stats", {})
    monkeypatch.setattr(d, "_hls_host_5xx", {})
    monkeypatch.setattr(d, "_hls_host_429", {})
    monkeypatch.setattr(d, "_hls_host_tripped", {})
    monkeypatch.setattr(d, "validate_segment_content", lambda content, url: None)


def _scripted_request(monkeypatch, replies):
    def fake_request(*args, **kwargs):
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(d, "request_with_retry", fake_request)


def test_segment_is_counted_once_however_many_retries(clean_stats, monkeypatch):
    """🔴 一个分片重试 5 次才成功：只算 1 个出过问题的分片，不是 5 次失败。"""
    url = f"https://{PALEO}/b3f8c21a/x/seg-1.ts"
    _scripted_request(monkeypatch, [RuntimeError("Read timed out.")] * 5 + [b"\x47" * 1000])
    assert d.download_single_segment(url, 0, retry_max=10, delay=0) == b"\x47" * 1000
    stats = d.hls_host_stats(PALEO)
    assert (stats["segments"], stats["seg_troubled"], stats["ok"], stats["bytes"]) == (1, 1, 1, 1000)


def test_link_or_source_failures_are_not_counted_as_overload(clean_stats, monkeypatch):
    """404 / 403 / 返回的不是视频：链接或片源的问题，不算源站扛不住。"""
    url = f"https://{PALEO}/b3f8c21a/x/seg-2.ts"
    _scripted_request(monkeypatch, [
        RuntimeError(f"请求失败({d._HTTP_PERMANENT_MARKER} HTTP 404): {url}"),
        RuntimeError(f"服务器返回的不是视频分片: {url}"),
    ])
    for index in (0, 1):
        with pytest.raises(RuntimeError):
            d.download_single_segment(url, index, retry_max=3, delay=0)
    stats = d.hls_host_stats(PALEO)
    assert stats["segments"] == 2 and stats["seg_troubled"] == 0


def test_breaker_trips_are_counted(clean_stats):
    url = f"https://{PALEO}/b3f8c21a/x/seg-3.ts"
    for _ in range(d.HLS_HOST_CIRCUIT_THRESHOLD):
        d._hls_host_record_result(url, 502)
    assert d.hls_host_stats(PALEO)["trips"] == 1
    assert d.hls_host_stats("nobody.example") == dict.fromkeys(d._HLS_STAT_FIELDS, 0)


class _PlaylistSession:
    def __init__(self, statuses):
        self.statuses = list(statuses)

    def request(self, *args, **kwargs):
        return _PlaylistResp(self.statuses.pop(0))


class _PlaylistResp:
    def __init__(self, status):
        self.status_code = status
        self.encoding = "utf-8"
        self.text = "#EXTM3U\n"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"{self.status_code} Server Error")
            error.response = self
            raise error


def test_playlist_attempts_are_counted_per_attempt_excluding_500(clean_stats, monkeypatch):
    """每次播放列表请求都记一笔；502/超时算过载，500 不算（vidup 签名过期就是 500）。"""
    url = f"https://{PALEO}/b3f8c21a/x/master.m3u8"
    session = _PlaylistSession([502, 502, 200, 500])
    monkeypatch.setattr(d, "get_session", lambda: session)
    assert d.request_with_retry("GET", url, retries=3, backoff=0, as_text=True,
                                on_attempt=d._count_playlist_attempt) == "#EXTM3U\n"
    with pytest.raises(RuntimeError):
        d.request_with_retry("GET", url, retries=1, backoff=0, as_text=True,
                             on_attempt=d._count_playlist_attempt)
    stats = d.hls_host_stats(PALEO)
    assert (stats["playlists"], stats["playlist_fail"]) == (4, 2)


def test_playlist_parsers_report_every_attempt(monkeypatch):
    seen = []

    def spy(method, url, **kwargs):
        seen.append(kwargs.get("on_attempt"))
        return "#EXTM3U\n#EXTINF:4,\nseg-1.ts\n"

    monkeypatch.setattr(d, "request_with_retry", spy)
    d.parse_master_playlist(f"https://{PALEO}/b3f8c21a/x/master.m3u8")
    d.parse_media_playlist(f"https://{PALEO}/b3f8c21a/x/index.m3u8")
    assert seen == [d._count_playlist_attempt, d._count_playlist_attempt]


def test_on_attempt_errors_never_break_the_request(monkeypatch):
    monkeypatch.setattr(d, "get_session", lambda: _PlaylistSession([200]))

    def broken(url, exc):
        raise ValueError("stats bug")

    assert d.request_with_retry("GET", "https://x/a.m3u8", retries=1, as_text=True,
                                on_attempt=broken) == "#EXTM3U\n"


# ------------------------------------------------------------ 节点降级

def _urls(entry):
    return [u["url"] for u in entry["urls"]]


def test_demote_moves_matching_nodes_last_and_keeps_order():
    entry = r2("x", then=("paleoak", "mp4"))
    entry["urls"].append({"url": f"https://{R2}/r2/cdn2/x/playlist.m3u8",
                          "provider": "vidup", "type": "m3u8"})
    before = _urls(entry)
    moved, changed = d.demote_dead_nodes(entry)
    assert changed is True
    assert _urls(moved) == [before[1], before[2], before[0], before[3]]
    assert _urls(entry) == before, "原条目不能被改"
    assert d.demote_dead_nodes(moved) == (moved, False)


def test_demote_is_a_no_op_when_nothing_or_everything_matches():
    entry = r2("x", then=())
    assert d.demote_dead_nodes(entry) == (entry, False)
    plain = pal("y")
    assert d.demote_dead_nodes(plain) == (plain, False)
    zen_vd = {"tmdbId": "z", "urls": [{"url": f"https://{R2}/vd/z/index.m3u8",
                                       "provider": "vidup", "type": "m3u8"}, pal("z")["urls"][0]]}
    assert d.demote_dead_nodes(zen_vd)[1] is False      # /vd/ 不受影响


def test_lane_classification_uses_demoted_order():
    assert d.capped_host_of(r2("a", then=("paleoak", "mp4"))) == PALEO
    assert d.capped_host_of(r2("b", then=("mp4",))) is None
    assert d.capped_host_of(r2("c", then=())) is None


def test_gate_routes_r2_first_films_to_the_paleoak_lane():
    src = Src([r2("a"), r2("b"), r2("c", then=("mp4",))])
    gate = lane_gate(src, cap=1, general=2)
    got, state = drain(gate)
    assert ids(got) == ["a", "c"] and state == "wait"   # b 在 paleoak 通道排队
    assert PALEO in got[0]["urls"][0]["url"]
    assert HAKUNA in got[1]["urls"][0]["url"]
    assert gate.in_flight(PALEO) == 1
    assert gate.stats["demoted"] == 3


def test_node_loop_tries_the_demoted_node_last(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    tried = []

    def fake_master(url, retries=None, headers=None):
        tried.append(url)
        raise RuntimeError("boom")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    entry = r2("x")
    _, ok, _ = d.process_one_entry(entry, set())
    assert ok is False
    assert [PALEO in u for u in tried] == [True, False]


# ------------------------------------------------------------ 端到端

def test_pipeline_lanes_never_exceed_caps_and_finish_every_film(tmp_path, monkeypatch):
    """🔒 6 个线程 = paleoak 专用 4 + 普通 2，/r2/ 打头的片算 paleoak。

    - 两条通道的实际并发从不超过各自上限，且都确实跑满过；
    - 每部片恰好处理一次，本轮正常收尾。
    """
    items = ([pal(f"p{i}") for i in range(10)] + [r2(f"r{i}") for i in range(4)]
             + [fast(f"f{i}") for i in range(10)])
    random.Random(3).shuffle(items)
    _streaming_env(tmp_path, monkeypatch, items, workers=6, depth=7)
    monkeypatch.setattr(d, "HOST_LIMITS", _limits(max_concurrent=4, max_deferred=1000))
    monkeypatch.setattr(d, "GENERAL_LANE_CFG", d._parse_general_lane({"slots": 2}))
    lock = threading.Lock()
    active = {"pal": 0, "gen": 0}
    peak = {"pal": 0, "gen": 0, "total": 0}
    started = []
    rng = random.Random(5)

    def fake_process(entry, processed_ids):
        lane = "pal" if d.capped_host_of(entry) == PALEO else "gen"
        with lock:
            started.append(entry["tmdbId"])
            active[lane] += 1
            peak[lane] = max(peak[lane], active[lane])
            peak["total"] = max(peak["total"], active["pal"] + active["gen"])
        time.sleep(rng.uniform(0.01, 0.04))
        with lock:
            active[lane] -= 1
        return entry["tmdbId"], False, {"error": "stub", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    _run_pipeline_bounded()

    assert sorted(started) == sorted(ids(items))
    assert peak["pal"] == 4 and peak["gen"] == 2 and peak["total"] <= 6


# ------------------------------------------------------------ 普通通道的派发前重取与告警线

BCDN4 = "bcdn4.hakunaymatata.com"


def vl(tid, host=BCDN4, signed_hours=None, fetched_min=0):
    """vidlink mp4 片：signed_hours = 签名（url 里 t=）距今几小时，fetched_min = 取到距今几分钟。"""
    query = f"?sign=s&t={NOW - int(signed_hours * 3600)}" if signed_hours is not None else ""
    return {"tmdbId": tid, "fetched_at": NOW - fetched_min * 60,
            "urls": [{"url": f"https://{host}/resource/{tid}.mp4{query}",
                      "provider": "vidlink", "type": "mp4"}]}


SHIPPED_GENERAL = {"refetch_after_minutes": {"vidup": 120, "videasy": 180, "vidlink": 720},
                   "signed_link_max_age_hours": {BCDN4: 24}}


def general_gate(src, r, general=2, **override):
    return lane_gate(src, general=general, refetcher=r,
                     general_cfg=dict(SHIPPED_GENERAL, **override))


def test_general_film_with_old_link_is_refetched_before_dispatch():
    """🔴 普通片派发那一刻链接太旧（vidlink 按取到时刻 12 小时）：先换新，新链接回来再派。"""
    r = Refetcher()
    src = Src([vl("f1", signed_hours=1, fetched_min=13 * 60), vl("f2", signed_hours=1)],
              closed=False, refetcher=r)
    gate = general_gate(src, r)
    got, _ = drain(gate)
    assert ids(got) == ["f2"] and r.dispatched == ["f1"]
    assert gate.stats["general_jit_refetch"] == 1 and gate.stats["jit_refetch"] == 0
    fresh = vl("f1", signed_hours=0)
    fresh["urls"][0]["url"] += "&new=1"
    src.items.append(fresh)
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and got[0]["urls"][0]["url"].endswith("&new=1")


def test_general_age_is_checked_when_dispatched_not_when_queued():
    """在普通通道里排队排旧了的片，轮到派发时才查年龄、才去换。"""
    r = Refetcher()
    clock = Clock()
    src = Src([vl("f1", signed_hours=1), vl("f2", signed_hours=1, fetched_min=11 * 60)],
              closed=False, refetcher=r)
    gate = lane_gate(src, general=1, clock=clock, refetcher=r, general_cfg=SHIPPED_GENERAL)
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and r.dispatched == []          # f2 排队时只有 11 小时
    clock.advance(120)
    gate.release(got[0])
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["f2"]                             # 轮到它时 13 小时


@pytest.mark.parametrize("host,signed_hours,refetched", [
    (BCDN4, 23.5, True),      # 24 小时过期、提前 1 小时换
    (BCDN4, 22.5, False),
    (HAKUNA, 30, False),      # bcdn 不卡有效期，不在名单里
])
def test_signed_link_age_triggers_refetch(host, signed_hours, refetched):
    r = Refetcher()
    src = Src([vl("f1", host=host, signed_hours=signed_hours)], closed=False, refetcher=r)
    got, _ = drain(general_gate(src, r))
    assert (r.dispatched == ["f1"]) is refetched
    assert (ids(got) == ["f1"]) is not refetched


def test_same_link_coming_back_is_not_refetched_again():
    """🔴 vidlink 换回原样的过期链接（实测 371/394）：本次运行不再为它重取，照常派出去试。"""
    r = Refetcher()
    stale = vl("f1", signed_hours=30)
    src = Src([stale], closed=False, refetcher=r)
    gate = general_gate(src, r)
    assert drain(gate) == ([], "wait")
    src.items.append(vl("f1", signed_hours=30))               # 重取结果：同一条链接
    got, _ = drain(gate)
    assert ids(got) == ["f1"]
    assert gate.stats["jit_same_link"] == 1
    gate.release(got[0])
    src.items.append(vl("f1", signed_hours=30))               # 下载失败后又回来
    assert ids(drain(gate)[0]) == ["f1"]
    assert r.dispatched == ["f1"], "换回同一条链接后不能再空烧取流"


def test_general_refetch_respects_its_budget():
    r = Refetcher()
    src = Src([vl("f1", signed_hours=30)], closed=False, refetcher=r)
    gate = general_gate(src, r, jit_refetch_max_per_movie=1)
    gate._jit_counts["f1"] = 1
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and r.dispatched == []
    assert gate.stats["jit_budget_out"] == 1


def test_general_refetch_off_without_config():
    """general_lane 没配重取阈值：普通片一律不做派发前重取（分通道之前的行为）。"""
    r = Refetcher()
    src = Src([vl("f1", signed_hours=30, fetched_min=24 * 60)], closed=False, refetcher=r)
    got, _ = drain(lane_gate(src, refetcher=r))
    assert ids(got) == ["f1"] and r.dispatched == []
    assert not d.general_refetch_applies(vl("f1"), d._parse_general_lane(None))
    assert d.general_refetch_applies(vl("f1"), d._parse_general_lane(SHIPPED_GENERAL))


def test_general_lane_warn_line_is_configurable(capsys):
    src = Src([fast(f"f{i}") for i in range(4)])
    gate = lane_gate(src, general=1, general_cfg={"max_deferred": 2})
    drain(gate)
    assert "普通槽位排队已 3 部（告警线 2）" in capsys.readouterr().out


def test_parse_general_lane_defaults_and_values():
    default = d._parse_general_lane(None)
    assert default == {
        "slots": None, "max_deferred": 1000, "refetch_after_seconds": {},
        "signed_link_max_age_seconds": {}, "signed_link_margin_seconds": 3600.0,
        "jit_refetch_max": 2,
    }
    parsed = d._parse_general_lane({"slots": 12, "max_deferred": 500,
                                     "refetch_after_minutes": {"vidup": 120, "bad": "x"},
                                     "signed_link_max_age_hours": {"BCDN4.hakunaymatata.com": 24},
                                     "signed_link_margin_minutes": 30})
    assert parsed["slots"] == 12 and parsed["max_deferred"] == 500
    assert parsed["refetch_after_seconds"] == {"vidup": 7200.0}
    assert parsed["signed_link_max_age_seconds"] == {BCDN4: 86400.0}
    assert parsed["signed_link_margin_seconds"] == 1800.0
    assert d._signed_at(f"https://{BCDN4}/r/x.mp4?sign=a&t=1792698594") == 1792698594
    assert d._signed_at(f"https://{BCDN4}/r/x.mp4?sign=a") is None
