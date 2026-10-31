"""按 CDN 主机限流（2026-10-30，23 号机实测）的离线用例：sun.finalsignal.top 按客户端 IP 限流，
直连只开不出 502 的量、超出的经代理；同时在下它的集数封顶，满了的集排队、不占下载槽位。"""

import threading
import time

import pytest

import download_tv as d

FINAL = "sun.finalsignal.top"
LIMITS = {FINAL: {"max_episodes": 2, "direct": 2, "proxy": 1, "proxy_spec": "env:TEST_PROXY"}}


def hls(tid, ep=1, host=FINAL):
    return {"url": f"https://{host}/b3f8c21a/{tid}-{ep}/master.m3u8", "provider": "vidup", "type": "m3u8"}


def mp4(tid, ep=1):
    return {"url": f"https://bcdn.hakunaymatata.com/{tid}e{ep}.mp4", "provider": "vidlink", "type": "mp4"}


def ep(tid, episode, urls):
    return {"tmdbId": str(tid), "season": 1, "episode": episode, "urls": urls}


class Src:
    def __init__(self, items=(), closed=True):
        self.items, self.closed, self.polled = list(items), closed, 0

    def poll(self):
        self.polled += 1
        if self.items:
            return "item", self.items.pop(0)
        return ("done", None) if self.closed else ("wait", None)


# ------------------------------------------------------------ 连接名额：先直连、再代理、都满就等

def test_direct_first_then_proxy_then_wait():
    conns = d.HostConnections(limits=LIMITS, proxy_url=lambda spec: "http://u:p@proxy:1")
    seen, release, entered = [], threading.Event(), threading.Barrier(4)

    def take():
        with conns.connection(f"https://{FINAL}/seg.ts") as proxies:
            seen.append(proxies and proxies["https"])
            if len(seen) <= 3:
                entered.wait(5)
                release.wait(5)

    threads = [threading.Thread(target=take) for _ in range(4)]
    for t in threads:
        t.start()
    entered.wait(5)                          # 前 3 个已经拿到名额
    time.sleep(0.2)
    assert sorted(map(str, seen)) == ["None", "None", "http://u:p@proxy:1"], "直连 2 条用满才走代理"
    assert len(seen) == 3, "第 4 个请求要等名额"
    release.set()
    for t in threads:
        t.join(5)
    assert len(seen) == 4 and conns.served == {"direct": 3, "proxy": 1}


def test_unlimited_hosts_and_missing_proxy():
    conns = d.HostConnections(limits=LIMITS, proxy_url=lambda spec: None)
    with conns.connection("https://bcdn.hakunaymatata.com/x.mp4") as proxies:
        assert proxies is None
    with conns.connection(f"https://{FINAL}/a.ts") as a, conns.connection(f"https://{FINAL}/b.ts") as b:
        assert (a, b) == (None, None)
    assert conns.summary() == "限流主机分片请求：直连 2 次、经代理 0 次"


def test_waiting_for_a_slot_gives_up_on_interrupt(monkeypatch):
    conns = d.HostConnections(limits={FINAL: {**LIMITS[FINAL], "direct": 1, "proxy": 0}},
                              proxy_url=lambda spec: None)
    with conns.connection(f"https://{FINAL}/a.ts"):
        d.interrupted.set()
        try:
            with pytest.raises(d.DownloadInterrupted):
                with conns.connection(f"https://{FINAL}/b.ts"):
                    pass
        finally:
            d.interrupted.clear()


def test_segment_request_goes_through_the_proxy_it_was_given(monkeypatch):
    monkeypatch.setattr(d, "HOST_CONNECTIONS", d.HostConnections(
        limits={FINAL: {**LIMITS[FINAL], "direct": 1}}, proxy_url=lambda spec: "http://u:p@proxy:1"))
    calls = []

    def fake_request(method, url, **kw):
        calls.append(kw.get("proxies"))
        return b"\x47" * 188 * 4

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    monkeypatch.setattr(d, "validate_segment_content", lambda content, url: None)
    with d.HOST_CONNECTIONS.connection(f"https://{FINAL}/hold.ts"):     # 直连名额先占掉
        d.download_single_segment(f"https://{FINAL}/s1.ts", 0, 1, 0)
    d.download_single_segment(f"https://{FINAL}/s2.ts", 1, 1, 0)
    assert calls == [{"http": "http://u:p@proxy:1", "https": "http://u:p@proxy:1"}, None]


def test_download_proxy_url_specs(monkeypatch):
    monkeypatch.setenv("TEST_PROXY", "http://alice:pw@v6.example:3128")
    assert d._download_proxy_url("env:TEST_PROXY") == "http://alice:pw@v6.example:3128"
    monkeypatch.delenv("TEST_PROXY")
    assert d._download_proxy_url("env:TEST_PROXY") is None
    monkeypatch.setenv("PROXY_USER", "u")
    monkeypatch.setenv("PROXY_PASSWORD", "p")
    assert d._download_proxy_url("residential").startswith("http://u:p@4x3h6x5m-as.dataedpt.com:")


# ------------------------------------------------------------ 集数上限：满了排队、不占槽位

def test_lane_full_episodes_queue_while_other_hosts_pass(capsys):
    screen = d.DispatchScreen(limits=LIMITS)
    a, b, c, v = ep(1, 1, [hls(1, 1)]), ep(1, 2, [hls(1, 2)]), ep(1, 3, [hls(1, 3)]), ep(2, 1, [mp4(2)])
    screen.set_source(Src([a, b, c, v]))
    got = [screen.poll()[1] for _ in range(3)]
    assert got == [a, b, v], "第 3 集排队，vidlink 的集照常派"
    assert screen.poll() == ("wait", None), "还有排队的集，本轮不能收尾"
    assert "[主机限流排队] 1_S01E03" in capsys.readouterr().out
    screen.release(a)
    assert screen.poll() == ("item", c), "空出名额先派排队的"
    screen.release(b)
    screen.release(c)
    assert screen.poll() == ("done", None)


def test_lane_counts_by_first_node_only():
    screen = d.DispatchScreen(limits={FINAL: {**LIMITS[FINAL], "max_episodes": 1}})
    first_final = ep(3, 1, [hls(3, 1), mp4(3, 1)])
    first_mp4 = ep(3, 2, [mp4(3, 2), hls(3, 2)])
    screen.set_source(Src([first_final, first_mp4, ep(3, 3, [hls(3, 3)])]))
    assert [screen.poll()[1] for _ in range(2)] == [first_final, first_mp4]
    assert screen.poll() == ("wait", None)


def test_parked_queue_is_bounded_and_keeps_backpressure():
    src = Src([ep(4, i, [hls(4, i)]) for i in range(1, 50)], closed=False)
    screen = d.DispatchScreen(limits=LIMITS, max_parked=5)
    screen.set_source(src)
    assert [screen.poll()[0] for _ in range(2)] == ["item", "item"]
    assert screen.poll() == ("wait", None)
    assert len(src.items) == 49 - 2 - 5, "排队满了就不再从来源往外拿"


def test_shipped_host_limits():
    import os

    import yaml

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]
    assert cfg["host_limits"] == {"finalsignal.top": {"max_episodes": 48, "direct_connections": 200,
                                                      "proxy_connections": 600, "proxy": "residential"}}
    # 集数上限必须给别的主机留出槽位，否则 finalsignal 的慢集占满全部槽位、vidlink 排不上。
    assert cfg["max_workers"] - cfg["host_limits"]["finalsignal.top"]["max_episodes"] >= 16
    assert cfg["download_queue_depth"] > cfg["max_workers"]
    assert cfg["max_parked_episodes"] == 20000


# ------------------------------------------------------------ 键写父域：换子域名不会绕过限流（2026-10-31）

PARENT = {"finalsignal.top": {"max_episodes": 2, "direct": 2, "proxy": 0, "proxy_spec": "residential"}}


@pytest.mark.parametrize("host, table, expected", [
    ("sun1.finalsignal.top", PARENT, "finalsignal.top"),
    ("a.b.finalsignal.top", PARENT, "finalsignal.top"),
    ("finalsignal.top", PARENT, "finalsignal.top"),
    ("sun.finalsignal.top", {**PARENT, FINAL: {}}, FINAL),       # 精确匹配优先
    ("notfinalsignal.top", PARENT, None),                        # 只认完整的域名标签
    ("sun1.finalsignal.top.evil.com", PARENT, None),
    ("top", PARENT, None),
    ("", PARENT, None),
])
def test_host_rule_key(host, table, expected):
    assert d._host_rule_key(host, table) == expected


def test_subdomains_share_one_connection_budget():
    conns = d.HostConnections(limits=PARENT, proxy_url=lambda spec: None)
    entered = threading.Event()
    done = []

    def third():
        entered.set()
        with conns.connection("https://sun.finalsignal.top/c.ts"):
            done.append(True)

    with conns.connection("https://sun1.finalsignal.top/a.ts"), conns.connection("https://sun2.finalsignal.top/b.ts"):
        t = threading.Thread(target=third)
        t.start()
        entered.wait(5)
        time.sleep(0.2)
        assert not done, "sun / sun1 / sun2 共用 2 条直连名额，第 3 条要等"
    t.join(5)
    assert done == [True]
    assert conns.served == {"direct": 3}


def test_subdomains_share_one_episode_lane():
    screen = d.DispatchScreen(limits=PARENT)
    a = ep(5, 1, [hls(5, 1, "sun.finalsignal.top")])
    b = ep(5, 2, [hls(5, 2, "sun1.finalsignal.top")])
    c = ep(5, 3, [hls(5, 3, "sun2.finalsignal.top")])
    screen.set_source(Src([a, b, c]))
    assert [screen.poll()[1] for _ in range(2)] == [a, b]
    assert screen.poll() == ("wait", None), "三个子域共用 2 集的上限"
    screen.release(b)
    assert screen.poll() == ("item", c)


# ------------------------------------------------------------ 主循环端到端：名额按集归还，排队的集当轮下完

def test_main_loop_releases_lane_slots_and_finishes_parked_episodes(tmp_path, monkeypatch):
    """🔴 每集下载结束都要还名额，否则排队的集永远派不出去、本轮收不了尾。"""
    from test_overlay_tv import _pipeline_env, hls as fhls, mp4 as fmp4

    monkeypatch.setattr(d, "HOST_LIMITS", {FINAL: {**LIMITS[FINAL], "max_episodes": 1}})
    entries = [{"tmdbId": "95", "season": 1, "episode": i, "title": "x", "urls": [fhls("95", i)]}
               for i in range(1, 5)]
    entries.append({"tmdbId": "96", "season": 1, "episode": 1, "title": "y", "urls": [fmp4("96", 1)]})
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert sorted(uploads) == ["95_S01E01", "95_S01E02", "95_S01E03", "95_S01E04", "96_S01E01"]
