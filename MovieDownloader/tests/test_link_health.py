"""派发前筛查（link_health，2026-10-26）：不拿已知失效的链接去下，vidlink 等缓存刷新再重取。

背景（10-24 23 号机 / 10-25 06 号机实测）：
  - bcdn4 的 vidlink 直链 t= 签名起 24 小时必 403；vidlink 按片缓存链接，缓存约在
    t + 48 小时才刷新，期间重取 371/394 原样返回同一条死链接；过了刷新时刻重取
    8/8 换到新签名，bcdnxw 上的片 8/10 换到别的主机。
  - videasy / vidup 链接过了实测有效期必失效，但一取就是新签名。

重点锁死：
  - 已知失效的节点不派去下；剩下的节点照常派（剪掉首节点后按新的首节点分通道）；
  - 节点全失效、有一取就新的节点：照常派出去、下载侧当场按「需重新取流」失败，
    走既有的过期重取闭环（不改变派发前重取额度用完后的老路）；
  - 只剩 vidlink 死链接 / 长期故障主机：缓存刷新前不白重取，挂起到刷新再重取，挂起有
    次数上限，本轮等不到的记「需重新取流」留给下次运行；
  - 🔴 判死红线不变：剪掉的节点没给出画质结论，剩下的节点再差也不能把整片判死。
"""

import json
import threading
import time

import pytest

import download_movies as d
from test_host_cap_gate import (
    NOW, PALEO, Clock, Probe, Refetcher, Src, _limits, _run_pipeline_bounded, _streaming_env,
    drain, ids,
)

BCDN4 = "bcdn4.hakunaymatata.com"
HCDN3 = "hcdn3.hakunaymatata.com"
BCDNXW = "bcdnxw.hakunaymatata.com"

GENERAL = {"slots": 2, "refetch_after_minutes": {"vidup": 120, "videasy": 180, "vidlink": 720},
           "signed_link_max_age_hours": {BCDN4: 24}}
HEALTH = {"dead_after_minutes": {"videasy": 300, "vidup": 300}, "vidlink_cache_roll_hours": 49,
          "park_retry_hours": 6, "park_max_per_movie": 2, "park_end_wait_minutes": 30,
          "broken_vidlink_hosts": [BCDNXW]}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", _limits())
    monkeypatch.setattr(d, "DEMOTE_NODE_PATTERNS", [])
    monkeypatch.setattr(d, "_mp4_host_tripped", {})


def vlink(tid, host=BCDN4, signed_ago_h=None, now=NOW):
    """vidlink mp4 节点：signed_ago_h = 签名（url 里 t=）距 now 几小时。"""
    query = f"?sign=s&t={int(now - signed_ago_h * 3600)}" if signed_ago_h is not None else ""
    return {"url": f"https://{host}/resource/{tid}.mp4{query}", "provider": "vidlink", "type": "mp4"}


def hls(tid, provider="vidup"):
    return {"url": f"https://{PALEO}/r6/{tid}/master.m3u8", "provider": provider, "type": "m3u8"}


def film(tid, *nodes, fetched_ago_min=0, now=NOW):
    return {"tmdbId": tid, "fetched_at": now - fetched_ago_min * 60, "urls": list(nodes)}


class Skips:
    def __init__(self):
        self.calls = []

    def __call__(self, entry, message):
        self.calls.append((entry["tmdbId"], message))


def gate_with(src, refetcher=None, clock=None, health=HEALTH, skips=None):
    gate = d.HostCapGate(
        _limits(max_concurrent=2, max_deferred=100), refetcher=refetcher,
        clock=clock or Clock(), probe_submit=Probe(),
        general=d._parse_general_lane(GENERAL),
        health=d._parse_link_health(health), on_skip=skips,
    )
    gate.set_source(src)
    return gate


def _urls(entry):
    return [node["url"] for node in d._entry_nodes(entry)]


# ------------------------------------------------------------ 剪掉失效节点

def test_dead_backup_node_is_pruned_and_live_node_dispatched():
    r = Refetcher()
    entry = film("f1", vlink("f1", host=HCDN3, signed_ago_h=1), vlink("f1", signed_ago_h=30))
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    got, state = drain(gate)
    assert ids(got) == ["f1"] and state == "wait"
    assert _urls(got[0]) == [entry["urls"][0]["url"]]
    dead = got[0]["_dead_nodes"]
    assert [n["url"] for n in dead] == [entry["urls"][1]["url"]]
    assert "签名已 30.0 小时" in dead[0]["reason"]
    assert gate.stats["dead_nodes_pruned"] == 1 and r.dispatched == []
    assert len(entry["urls"]) == 2, "原条目不能被改（来源、台账还引用它）"


def test_pruning_the_first_node_moves_the_film_to_its_new_lane():
    """死掉的 vidlink 打头、后面是 paleoak 节点：剪掉后按 paleoak 占专用槽位，且不为
    那条缓存没刷新的死链接白重取。"""
    r = Refetcher()
    entry = film("f1", vlink("f1", signed_ago_h=30), hls("f1"))
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and _urls(got[0]) == [hls("f1")["url"]]
    assert gate.in_flight(PALEO) == 1 and gate.in_flight(d.GENERAL_LANE) == 0
    assert r.dispatched == []


def test_merged_old_node_is_judged_by_its_own_age():
    """派发前重取补回的旧节点按它自己的 fetched_at 判（条目时刻是新结果的）。"""
    old = dict(hls("f1", provider="videasy"), fetched_at=NOW - 301 * 60)
    entry = film("f1", vlink("f1", host=HCDN3, signed_ago_h=1), old)
    got, _ = drain(gate_with(Src([entry])))
    assert _urls(got[0]) == [entry["urls"][0]["url"]]
    assert "videasy 链接已 301 分钟" in got[0]["_dead_nodes"][0]["reason"]


def test_refetch_still_comes_first_for_old_vidup_links():
    """额度还在时照旧先派发前重取（阈值 120 分钟 < 判失效的 300 分钟），不剪不跳过。"""
    r = Refetcher()
    entry = film("p1", hls("p1"), fetched_ago_min=400)
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["p1"] and gate.stats["dead_nodes_pruned"] == 0


def test_all_dead_vidup_film_fails_fast_into_the_refetch_loop(monkeypatch):
    """🔴 派发前重取额度用完、节点全失效：不下载，当场按「需重新取流」失败，
    走既有的过期重取闭环（不能在闸门里悄悄跳过，断掉 auto_refetch）。"""
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    r = Refetcher()
    entry = film("p1", hls("p1"), fetched_ago_min=400)
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    gate._jit_counts["p1"] = 2
    got, _ = drain(gate)
    assert ids(got) == ["p1"] and got[0]["urls"] == []
    assert gate.stats["dead_link_failed"] == 1 and r.dispatched == []

    _, ok, info = d.process_one_entry(got[0], set())
    assert ok is False and info["retriable"] is False and info["needs_refetch"] is True
    assert d._NEEDS_REFETCH_MARKER in info["error"]
    [failure] = info["node_failures"]
    assert failure["provider"] == "vidup" and failure["skipped_before_dispatch"] is True
    assert failure["reason_class"] == d._REFETCH_REASON_LABEL
    assert d.plan_retry_buckets(info["retriable"], info["error"], info["needs_refetch"]) == (False, True)
    assert d.is_quality_dead(info["retriable"], info["error"], info["node_failures"]) is False


def test_fully_pruned_film_that_already_succeeded_is_skipped_not_failed():
    """🔴 已成功（或正在处理）的片哪怕节点全被剪光，也照旧按「已处理」跳过：
    不能再记一条失败、再触发重取。"""
    entry, _dead = d.prune_dead_nodes(
        film("p1", hls("p1"), fetched_ago_min=400), NOW, {}, {"vidup": 300 * 60},
    )
    assert entry["urls"] == []
    _, ok, info = d.process_one_entry(entry, {"p1"})
    assert ok is False and info == {"error": "already processed successfully"}
    assert "p1" not in d.processing_ids


def test_link_health_off_keeps_every_node():
    r = Refetcher()
    entry = film("f1", vlink("f1", host=HCDN3, signed_ago_h=1), vlink("f1", signed_ago_h=30))
    got, _ = drain(gate_with(Src([entry], closed=False, refetcher=r), r, health=None))
    assert _urls(got[0]) == _urls(entry) and "_dead_nodes" not in got[0]


# ------------------------------------------------------------ vidlink 挂起到缓存刷新

def test_stale_vidlink_film_is_parked_until_cache_roll_then_refetched():
    r = Refetcher()
    clock = Clock()
    src = Src([film("f1", vlink("f1", signed_ago_h=30))], closed=False, refetcher=r)
    gate = gate_with(src, r, clock)
    assert drain(gate) == ([], "wait")
    assert r.dispatched == [] and gate.stats["parked"] == 1, "缓存刷新前重取只会换回同一条"

    clock.advance(19 * 60 - 1)
    assert drain(gate) == ([], "wait") and r.dispatched == []
    clock.advance(1)                                       # t + 49 小时
    assert drain(gate) == ([], "wait")
    assert r.dispatched == ["f1"] and gate.stats["park_refetch"] == 1

    fresh = film("f1", vlink("f1", host=HCDN3, signed_ago_h=0.5, now=clock.t), now=clock.t)
    src.items.append(fresh)
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and _urls(got[0]) == _urls(fresh)
    assert "_dead_nodes" not in got[0]
    assert r.dispatched == ["f1"], "到点重取本身就是派发前重取，新链接回来不再重取"


def test_same_dead_link_after_roll_is_parked_once_more_then_left_for_next_run():
    r = Refetcher()
    clock = Clock()
    skips = Skips()
    src = Src([film("f1", vlink("f1", signed_ago_h=30))], closed=False, refetcher=r)
    gate = gate_with(src, r, clock, skips=skips)
    drain(gate)
    clock.advance(19 * 60)
    drain(gate)
    assert r.dispatched == ["f1"]

    src.items.append(film("f1", vlink("f1", signed_ago_h=30)))   # 缓存没按时刷新
    assert drain(gate) == ([], "wait")
    assert gate.stats["parked"] == 2 and gate._parked[0][0] == clock.t + 6 * 3600
    assert r.dispatched == ["f1"]

    clock.advance(6 * 60)
    drain(gate)
    assert r.dispatched == ["f1", "f1"]
    src.items.append(film("f1", vlink("f1", signed_ago_h=30)))
    assert drain(gate) == ([], "wait")
    assert gate.stats["dead_link_skipped"] == 1 and not gate._parked
    [(key, message)] = skips.calls
    assert key == "f1" and d._NEEDS_REFETCH_MARKER in message
    assert r.dispatched == ["f1", "f1"]


def test_parked_refetch_without_result_parks_again_with_its_reason(capsys):
    r = Refetcher()
    clock = Clock()
    src = Src([film("f1", vlink("f1", signed_ago_h=30))], closed=False, refetcher=r)
    gate = gate_with(src, r, clock)
    drain(gate)
    clock.advance(19 * 60)
    drain(gate)
    capsys.readouterr()
    r.pending.discard("f1")                                # 重取无果
    assert drain(gate) == ([], "wait")
    assert gate.stats["parked"] == 2 and gate.stats["jit_no_result"] == 1
    assert "签名已" in capsys.readouterr().out
    assert gate._parked[0][0] == clock.t + 6 * 3600


def test_film_only_on_broken_vidlink_host_is_parked():
    r = Refetcher()
    only = film("f1", vlink("f1", host=BCDNXW, signed_ago_h=1))
    mixed = film("f2", vlink("f2", host=BCDNXW, signed_ago_h=1), vlink("f2", host=HCDN3, signed_ago_h=1))
    gate = gate_with(Src([only, mixed], closed=False, refetcher=r), r)
    got, _ = drain(gate)
    assert ids(got) == ["f2"] and _urls(got[0]) == _urls(mixed), "故障主机上的节点不剪，照常作一试"
    assert gate.stats["parked"] == 1 and gate.stats["dead_nodes_pruned"] == 0
    assert gate._parked[0][0] == NOW + 48 * 3600 and r.dispatched == []


def test_momentary_mp4_breaker_trip_does_not_park():
    """🔴 mp4 主机熔断 15 秒就复位（给分钟级抖动用）：熔断那一刻到闸门的片不能因此
    挂起几十个小时等 vidlink 缓存刷新——只等这次冷却（熔断等冷却，2026-10-27），
    到点照常派。"""
    clock = Clock()
    d._mp4_host_tripped[HCDN3] = time.monotonic() + 15
    entry = film("f1", vlink("f1", host=HCDN3, signed_ago_h=1))
    r = Refetcher()
    gate = gate_with(Src([entry], closed=False, refetcher=r), r, clock=clock)
    assert drain(gate) == ([], "wait")
    assert gate.stats["parked"] == 0 and gate.stats["circuit_held"] == 1
    assert gate._cooling[0][0] <= NOW + 15, "只等这一次冷却，不是 vidlink 缓存刷新"
    clock.advance(0.25)
    del d._mp4_host_tripped[HCDN3]           # 冷却结束
    got, _ = drain(gate)
    assert ids(got) == ["f1"] and gate.stats["parked"] == 0 and r.dispatched == []


def _variant(node, quality):
    """同一部片 vidlink 的另一个清晰度版本：同主机、同签名时刻，url 不同。"""
    return dict(node, url=node["url"] + f"&q={quality}")


def test_stuck_bcdnxw_film_is_parked_without_a_futile_refetch(capsys):
    """🔴 只剩 bcdnxw、缓存还没刷新：派发前重取只会换回同一条，直接挂起到刷新时刻。
    （10-26 24 号机：4498 次派发前重取里 882 次是这种。）说明里同一个原因只写一次。"""
    r = Refetcher()
    node = vlink("f1", host=BCDNXW, signed_ago_h=10)
    entry = film("f1", _variant(node, 720), _variant(node, 480), fetched_ago_min=13 * 60)
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    assert drain(gate) == ([], "wait")
    assert r.dispatched == [] and gate.stats["general_jit_refetch"] == 0
    assert gate.stats["parked"] == 1 and gate._parked[0][0] == NOW + 39 * 3600
    park_line = next(l for l in capsys.readouterr().out.splitlines() if "[挂起待重取]" in l)
    assert park_line.count("长期故障") == 1


def test_stuck_film_waits_for_its_earliest_cache_roll():
    """bcdnxw + 签名已过期的 bcdn4：两条都卡住，按更早的那次刷新挂起，期间不重取。"""
    r = Refetcher()
    entry = film("f1", vlink("f1", host=BCDNXW, signed_ago_h=10), vlink("f1", signed_ago_h=30),
                 fetched_ago_min=13 * 60)
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    assert drain(gate) == ([], "wait")
    assert r.dispatched == [] and gate._parked[0][0] == NOW + 19 * 3600


@pytest.mark.parametrize("nodes,why", [
    ([vlink("f1", host=BCDNXW, signed_ago_h=50)], "缓存已经刷新，重取能换到新主机"),
    ([vlink("f1", host=BCDNXW, signed_ago_h=10), vlink("f1", host=HCDN3, signed_ago_h=10)],
     "还有能下的节点，按 vidlink 12 小时的老规则换新"),
    ([vlink("f1", host=BCDNXW)], "没有签名时刻，算不出刷新时间"),
])
def test_refetch_still_happens_when_it_can_help(nodes, why):
    r = Refetcher()
    entry = film("f1", *nodes, fetched_ago_min=13 * 60)
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    drain(gate)
    assert r.dispatched == ["f1"], why


def test_film_with_a_pruned_vidup_node_is_still_refetched():
    """之前剪掉过 vidup 节点（一取就新）的片，剩下只有 bcdnxw 也要照常重取。"""
    r = Refetcher()
    entry = film("f1", vlink("f1", host=BCDNXW, signed_ago_h=10), fetched_ago_min=13 * 60)
    # 带上 t=：免得「算不出刷新时刻」那条顺带把它放过去，测不到这里要锁的判断。
    entry["_dead_nodes"] = [{"provider": "vidup", "type": "m3u8",
                             "url": hls("f1")["url"] + f"?t={NOW - 10 * 3600}",
                             "reason": "vidup 链接已 400 分钟（300 分钟即失效）"}]
    gate = gate_with(Src([entry], closed=False, refetcher=r), r)
    drain(gate)
    assert r.dispatched == ["f1"]


@pytest.mark.parametrize("health,delivers", [
    (dict(HEALTH, park_max_per_movie=0), True),
    (HEALTH, False),                       # 本轮来源收不回重取结果
])
def test_vidlink_film_that_cannot_be_parked_is_left_for_next_run(health, delivers):
    r = Refetcher()
    skips = Skips()
    src = Src([film("f1", vlink("f1", signed_ago_h=30))], closed=False, delivers=delivers, refetcher=r)
    gate = gate_with(src, r, health=health, skips=skips)
    assert drain(gate) == ([], "wait")
    assert gate.stats["dead_link_skipped"] == 1 and not gate._parked and r.dispatched == []
    assert "没有可用的节点（签名已 30.0 小时" in skips.calls[0][1]


def test_run_end_waits_for_a_park_due_soon_and_leaves_far_ones_for_next_run():
    r = Refetcher()
    clock = Clock()
    skips = Skips()
    soon = film("s1", vlink("s1", signed_ago_h=48.8))      # 缓存 12 分钟后刷新
    far = film("f1", vlink("f1", signed_ago_h=30))         # 19 小时后
    src = Src([soon, far], closed=True, refetcher=r)
    gate = gate_with(src, r, clock, skips=skips)
    assert drain(gate) == ([], "wait")
    assert gate.stats["parked"] == 2 and skips.calls == []

    clock.advance(12)
    assert drain(gate) == ([], "wait") and r.dispatched == ["s1"]
    fresh = film("s1", vlink("s1", host=HCDN3, signed_ago_h=0, now=clock.t), now=clock.t)
    src.items.append(fresh)
    got, state = drain(gate)
    assert ids(got) == ["s1"] and state == "done"
    [(key, message)] = skips.calls
    assert key == "f1" and "本轮等不到" in message and d._NEEDS_REFETCH_MARKER in message
    assert gate.stats["park_flushed"] == 1 and gate.stats["dead_link_skipped"] == 0
    assert drain(gate) == ([], "done")


def test_dispatch_skip_record_is_picked_up_for_refetch(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    entry, _dead = d.prune_dead_nodes(
        film("f1", vlink("f1", signed_ago_h=30)), NOW, {BCDN4: 24 * 3600}, {},
    )
    d.record_dispatch_skip(entry, f"没有可用的节点，本次不下载，{d._NEEDS_REFETCH_MARKER}")
    with open(d.FAILED_LOG, encoding="utf-8") as f:
        [record] = [json.loads(line) for line in f]
    assert record["stage"] == "dispatch" and record["retriable"] is False
    assert record["node_failures"][0]["reason_class"] == d._REFETCH_REASON_LABEL
    assert d._still_needs_refetch(record, done_ids=set(), dead_ids=set())
    assert not d.is_quality_dead(record["retriable"], record["error"], record["node_failures"])


def test_pipeline_end_to_end_with_link_health(tmp_path, monkeypatch):
    """🔒 端到端（真实主循环）：失效节点不派、全失效的 vidup 片当场按需重新取流失败、
    只剩 vidlink 死链接又收不到重取结果的片记 dispatch 跳过，本轮正常收尾。"""
    now = int(time.time())                     # 取流侧写的 fetched_at 是整数秒
    items = [
        film("ok1", vlink("ok1", host=HCDN3, signed_ago_h=1, now=now), now=now),
        film("pruned", vlink("pruned", host=HCDN3, signed_ago_h=1, now=now),
             vlink("pruned", signed_ago_h=30, now=now), now=now),
        film("deadvidup", hls("deadvidup"), fetched_ago_min=400, now=now),
        film("deadvidlink", vlink("deadvidlink", signed_ago_h=30, now=now), now=now),
    ]
    _streaming_env(tmp_path, monkeypatch, items)
    monkeypatch.setattr(d, "GENERAL_LANE_CFG", d._parse_general_lane(
        {"signed_link_max_age_hours": {BCDN4: 24}}))
    monkeypatch.setattr(d, "LINK_HEALTH_CFG", d._parse_link_health(HEALTH))
    real_process = d.process_one_entry
    lock = threading.Lock()
    dispatched = {}

    def fake_process(entry, processed_ids):
        with lock:
            dispatched[entry["tmdbId"]] = _urls(entry)
        if not entry["urls"]:
            return real_process(entry, processed_ids)
        return entry["tmdbId"], False, {"error": "stub", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    _run_pipeline_bounded()

    assert sorted(dispatched) == ["deadvidup", "ok1", "pruned"]
    assert dispatched["pruned"] == [items[1]["urls"][0]["url"]]
    assert dispatched["deadvidup"] == []
    with open(d.FAILED_LOG, encoding="utf-8") as f:
        records = {r["tmdbId"]: r for r in map(json.loads, f)}
    assert records["deadvidup"]["stage"] == "download"
    assert d._NEEDS_REFETCH_MARKER in records["deadvidup"]["error"]
    assert records["deadvidlink"]["stage"] == "dispatch"
    assert d._NEEDS_REFETCH_MARKER in records["deadvidlink"]["error"]


# ------------------------------------------------------------ 🔴 判死红线

def test_pruned_nodes_keep_a_bad_live_node_from_killing_the_film(tmp_path, monkeypatch):
    """剪掉的节点没给出画质结论：唯一派出去的节点画质不达标，也不能把整片判死。"""
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_mp4_preflight", lambda *a, **k: None)

    def fake_mp4(node, output_path, label, runtime_minutes=None, preflight=None):
        raise d.QualityRejectedError("声明分辨率 480p 低于红线 1080")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_mp4)
    entry, _dead = d.prune_dead_nodes(
        film("55", vlink("55", host=HCDN3, signed_ago_h=1), vlink("55", signed_ago_h=30)),
        NOW, {BCDN4: 24 * 3600}, {},
    )
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert info["needs_refetch"] is True and d._NEEDS_REFETCH_MARKER in info["error"]
    assert "判定整片画质不达标" not in info["error"]
    assert len(info["node_failures"]) == 2
    assert d.is_quality_dead(info["retriable"], info["error"], info["node_failures"]) is False
    assert d.plan_retry_buckets(info["retriable"], info["error"], info["needs_refetch"])[1] is True


# ------------------------------------------------------------ 判失效规则与配置

def test_node_dead_reason_rules():
    signed, dead_after = {BCDN4: 24 * 3600}, {"vidup": 300 * 60}

    def reason(entry):
        return d.node_dead_reason(entry, d._entry_nodes(entry)[0], NOW, signed, dead_after)

    assert reason(film("x", hls("x"), fetched_ago_min=299)) is None
    assert "vidup 链接已 300 分钟" in reason(film("x", hls("x"), fetched_ago_min=300))
    assert reason({"tmdbId": "x", "urls": [hls("x")]}) is None          # 没有 fetched_at 不判
    assert reason(film("x", vlink("x", signed_ago_h=23.9))) is None
    assert "签名已 24.0 小时" in reason(film("x", vlink("x", signed_ago_h=24)))
    assert reason(film("x", vlink("x"), fetched_ago_min=10_000)) is None   # bcdn4 没 t= 不判
    assert reason(film("x", vlink("x", host=HCDN3, signed_ago_h=100))) is None
    assert reason(film("x", hls("x", provider="videasy"), fetched_ago_min=10_000)) is None


def test_parse_link_health_defaults_and_shipped_config():
    off = d._parse_link_health(None)
    assert off["enabled"] is False and off["dead_after_seconds"] == {}
    assert off["broken_vidlink_hosts"] == set()
    assert d._parse_link_health({"broken_vidlink_hosts": [" BCDNXW.hakunaymatata.com ", ""]})[
        "broken_vidlink_hosts"] == {BCDNXW}
    assert d._parse_link_health({"enabled": False, "dead_after_minutes": {"vidup": 1}})["enabled"] is False
    bad = d._parse_link_health({"dead_after_minutes": {"vidup": "x", "videasy": 0},
                                "park_max_per_movie": "x"})
    assert bad["enabled"] is True and bad["dead_after_seconds"] == {} and bad["park_max_per_movie"] == 2

    shipped = d._parse_link_health(d._CFG.get("link_health"))
    assert shipped == {
        "enabled": True, "dead_after_seconds": {"videasy": 18000.0, "vidup": 18000.0},
        "vidlink_cache_roll_seconds": 49 * 3600.0, "park_retry_seconds": 6 * 3600.0,
        "park_max_per_movie": 2, "park_end_wait_seconds": 1800.0,
        "broken_vidlink_hosts": {BCDNXW},
    }
    # 判失效的年龄必须晚于派发前重取的阈值：额度还在时总是先换新，而不是先剪掉。
    general = d._parse_general_lane(d._CFG.get("general_lane"))
    hosts = d._parse_host_limits(d._CFG.get("host_limits"))
    for provider, dead_after in shipped["dead_after_seconds"].items():
        for ages in [general["refetch_after_seconds"]] + [h["refetch_after_seconds"] for h in hosts.values()]:
            assert ages.get(provider, 0) < dead_after
