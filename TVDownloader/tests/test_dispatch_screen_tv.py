"""派发闸门 DispatchScreen（熔断等冷却 + vidlink 缓存挂起，电影侧 bb5382a / 1573d28 / 3120958
移植，2026-10-30）的离线用例。"""

import time

import pytest

import download_tv as d


FINAL = "sun.finalsignal.top"
NOW = 1_793_300_000


def hls(tid, ep=1, host=FINAL):
    return {"url": f"https://{host}/r6/{tid}-{ep}/master.m3u8", "provider": "vidup", "type": "m3u8"}


def vid(tid, ep=1, host="bcdn4.hakunaymatata.com", signed=NOW - 25 * 3600):
    return {"url": f"https://{host}/resource/{tid}e{ep}.mp4?sign=x&t={signed}",
            "provider": "vidlink", "type": "mp4"}


def ep(tid, episode, urls):
    return {"tmdbId": str(tid), "season": 1, "episode": episode, "urls": urls}


class Src:
    def __init__(self, items=(), closed=True):
        self.items = list(items)
        self.closed = closed

    def poll(self):
        if self.items:
            return "item", self.items.pop(0)
        return ("done", None) if self.closed else ("wait", None)


class _Clock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture(autouse=True)
def _clean_circuits(monkeypatch):
    for name in ("_hls_host_tripped", "_mp4_host_tripped"):
        getattr(d, name).clear()
    monkeypatch.setattr(d, "HOST_CIRCUIT_HOLD_MAX_PER_EPISODE", 2)
    monkeypatch.setattr(d, "SIGNED_LINK_MAX_AGE_SECONDS", {"bcdn4.hakunaymatata.com": 24 * 3600})
    monkeypatch.setattr(d, "BROKEN_VIDLINK_HOSTS", {"bcdnxw.hakunaymatata.com"})
    monkeypatch.setattr(d, "VIDLINK_CACHE_ROLL_SECONDS", 49 * 3600)
    yield
    for name in ("_hls_host_tripped", "_mp4_host_tripped"):
        getattr(d, name).clear()


def _trip(host, seconds):
    d._hls_host_tripped[host] = time.monotonic() + seconds


# ------------------------------------------------------------ 熔断等冷却

def test_host_circuit_remaining_reads_the_right_breaker():
    _trip(FINAL, 10)
    assert 0 < d.host_circuit_remaining(d._normalize_url_entry(hls("1"))) <= 10
    assert d.host_circuit_remaining(d._normalize_url_entry(hls("1", host="moon.goldenkite.top"))) is None
    d._mp4_host_tripped["bcdn.hakunaymatata.com"] = time.monotonic() - 1      # 已到点
    assert d.host_circuit_remaining(d._normalize_url_entry(vid("1", host="bcdn.hakunaymatata.com"))) is None


def test_episode_with_every_node_on_a_tripped_host_waits_for_cooldown(capsys):
    """🔴 节点全在熔断主机上：先挂起、不派，冷却到点再派；来源取空期间报 wait 不收尾。"""
    _trip(FINAL, 5)
    clock = _Clock()
    screen = d.DispatchScreen(clock=clock, wall=lambda: NOW)
    screen.set_source(Src([ep(1, 1, [hls("1")]), ep(1, 2, [hls("1", 2), hls("1", 2, "moon.goldenkite.top")])]))

    state, entry = screen.poll()
    assert (state, entry["episode"]) == ("item", 2), "有一个节点不在熔断主机上就照常派"
    assert screen.poll() == ("wait", None), "还有冷却中的集，本轮不能收尾"
    assert "[熔断等冷却] 1_S01E01" in capsys.readouterr().out

    d._hls_host_tripped.clear()
    clock.t += 6
    state, entry = screen.poll()
    assert (state, entry["episode"]) == ("item", 1)
    assert screen.poll() == ("done", None)


def test_hold_is_capped_per_episode():
    _trip(FINAL, 1000)
    clock = _Clock()
    screen = d.DispatchScreen(clock=clock, wall=lambda: NOW)
    screen.set_source(Src([ep(2, 1, [hls("2")])]))
    for _ in range(2):
        assert screen.poll() == ("wait", None)
        clock.t += 1001
    state, entry = screen.poll()
    assert state == "item" and entry["episode"] == 1, "挂起次数用完照常派出去"
    assert screen.held == 2


def test_hold_disabled_dispatches_immediately(monkeypatch):
    monkeypatch.setattr(d, "HOST_CIRCUIT_HOLD_MAX_PER_EPISODE", 0)
    _trip(FINAL, 1000)
    screen = d.DispatchScreen(wall=lambda: NOW)
    screen.set_source(Src([ep(3, 1, [hls("3")])]))
    assert screen.poll()[0] == "item"


# ------------------------------------------------------------ vidlink 缓存挂起

def test_vidlink_refresh_due_only_for_vidlink_only_bad_links():
    signed = NOW - 25 * 3600
    due, why = d.vidlink_refresh_due(ep(4, 1, [vid("4", signed=signed)]), NOW)
    assert due == signed + 49 * 3600 and "签名已 25.0 小时" in why
    broken = ep(4, 2, [vid("4", 2, host="bcdnxw.hakunaymatata.com", signed=NOW - 3600)])
    assert "bcdnxw.hakunaymatata.com 长期故障" in d.vidlink_refresh_due(broken, NOW)[1]
    # 还有一个没坏的节点 / 有 HLS 节点：照常派
    assert d.vidlink_refresh_due(ep(4, 3, [vid("4", 3, signed=NOW - 3600)]), NOW) is None
    assert d.vidlink_refresh_due(ep(4, 4, [vid("4", 4), hls("4", 4)]), NOW) is None
    assert d.vidlink_refresh_due(ep(4, 5, [vid("4", 5, host="bcdn.hakunaymatata.com",
                                                signed=NOW - 99 * 3600)]), NOW) is None, \
        "bcdn 不卡有效期，不判坏"


def test_vidlink_only_bad_episode_is_skipped_until_the_cache_rolls():
    skipped = []
    screen = d.DispatchScreen(on_skip=lambda e, why: skipped.append((e["episode"], why)),
                              wall=lambda: NOW)
    fresh_roll = ep(5, 2, [vid("5", 2, signed=NOW - 50 * 3600)])     # 缓存应已刷新：照常派
    screen.set_source(Src([ep(5, 1, [vid("5")]), fresh_roll]))
    state, entry = screen.poll()
    assert (state, entry["episode"]) == ("item", 2)
    assert [e for e, _ in skipped] == [1]
    assert "本轮等不到，本次不下载" in skipped[0][1]
    assert d._NEEDS_REFETCH_MARKER not in skipped[0][1], "不能带需重取标记，免得重取闭环白跑"


def test_preflight_does_not_refetch_vidlink_episodes_before_the_cache_rolls(monkeypatch, capsys):
    monkeypatch.setattr(d, "is_stale_entry", lambda entry, now=None: True)
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: set())
    monkeypatch.setattr(d.time, "time", lambda: NOW)
    entries = [ep(6, 1, [vid("6")]), ep(6, 2, [hls("6", 2)])]
    stale, _ = d._select_stale_entries(entries)
    assert [e["episode"] for e in stale] == [2]
    assert "跳过 1 集只剩 vidlink 坏链接" in capsys.readouterr().out


def test_shipped_config():
    assert d.HOST_CIRCUIT_HOLD_MAX_PER_EPISODE >= 0
    import yaml, os

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]
    assert cfg["host_circuit_hold_max_per_episode"] == 20
    assert cfg["link_health"]["vidlink_cache_roll_hours"] == 49
    assert cfg["link_health"]["broken_vidlink_hosts"] == ["bcdnxw.hakunaymatata.com"]
    assert cfg["link_health"]["signed_link_max_age_hours"] == {"bcdn4.hakunaymatata.com": 24}


def test_tripped_host_does_not_drain_the_whole_source(capsys):
    """🔴 熔断时闸门不能把来源整条拖进冷却堆：单独跑时会在一次 poll 里空转几万集、
    pipeline 模式下会抽空取流侧的有界队列、反压失效。冷却堆满了就报 wait。"""
    _trip(FINAL, 1000)
    src = Src([ep(7, i, [hls("7", i)]) for i in range(1, 501)], closed=False)
    screen = d.DispatchScreen(wall=lambda: NOW, max_held=8)
    screen.set_source(src)
    assert screen.poll() == ("wait", None)
    assert len(src.items) == 500 - 8, "冷却堆满了就不再从来源往外拿"
    assert screen.poll() == ("wait", None)
    assert len(src.items) == 500 - 8
