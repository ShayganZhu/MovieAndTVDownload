"""按 CDN 主机限流（2026-10-30，23 号机实测）的离线用例：sun.finalsignal.top 按客户端 IP 限流，
直连只开不出 502 的量、超出的经代理；同时在下它的集数封顶，满了的集排队、不占下载槽位。"""

import threading
import time

import pytest
import requests

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
        with conns.connection(f"https://{FINAL}/seg.ts") as route:
            seen.append(route.get("proxies", {}).get("https"))
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
    with conns.connection("https://bcdn.hakunaymatata.com/x.mp4") as route:
        assert route == {}
    with conns.connection(f"https://{FINAL}/a.ts") as a, conns.connection(f"https://{FINAL}/b.ts") as b:
        assert (a, b) == ({}, {})
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


# ------------------------------------------------------------ 代理池按连接分 session（每条连接一个出口 IP）

SESSIONS = {FINAL: {**LIMITS[FINAL], "direct": 1, "proxy": 3, "proxy_session_per_connection": True}}
POOL = "http://alice%40corp:p%3Aw@v6.example:8000"


def _http_error(status):
    resp = requests.Response()
    resp.status_code = status
    cause = requests.HTTPError(f"{status} Client Error", response=resp)
    try:
        raise RuntimeError(f"请求失败: {status}") from cause
    except RuntimeError as exc:
        return exc


def _session_of(route):
    url = route["proxies"]["https"]
    assert route["proxies"]["http"] == url
    return url.split("-session-", 1)[1].split(":", 1)[0]


def test_proxy_url_with_session_appends_to_the_username():
    assert (d._proxy_url_with_session(POOL, "ab12")
            == "http://alice%40corp-session-ab12:p%3Aw@v6.example:8000")
    assert d._proxy_url_with_session("http://bob@p.example:1", "x") == "http://bob-session-x@p.example:1"
    assert d._proxy_url_with_session("http://p.example:1", "x") == "http://p.example:1", "没账号就不拼"


def test_each_concurrent_proxy_connection_gets_its_own_session():
    conns = d.HostConnections(limits=SESSIONS, proxy_url=lambda spec: POOL)
    routes, release, entered = [], threading.Event(), threading.Barrier(5)

    def take():
        with conns.connection(f"https://{FINAL}/seg.ts") as route:
            routes.append(route)
            if len(routes) <= 4:
                entered.wait(5)
                release.wait(5)

    threads = [threading.Thread(target=take) for _ in range(5)]
    for t in threads:
        t.start()
    entered.wait(5)                          # 直连 1 条 + 代理 3 条已经拿到名额
    time.sleep(0.2)
    assert len(routes) == 4, "第 5 个请求要等名额"
    proxied = [r for r in routes if "proxies" in r]
    assert len(proxied) == 3 and len({_session_of(r) for r in proxied}) == 3, "同时在用的代理连接各一个 session"
    assert all(r["proxies"]["https"].startswith("http://alice%40corp-session-") for r in proxied)
    release.set()
    for t in threads:
        t.join(5)
    assert len(routes) == 5 and sum(conns.served.values()) == 5 and conns.served["proxy"] >= 3


def test_a_thread_gets_its_own_session_back_to_reuse_the_tunnel():
    conns = d.HostConnections(limits=SESSIONS, proxy_url=lambda spec: POOL)
    with conns.connection(f"https://{FINAL}/hold.ts"):              # 直连名额先占掉
        with conns.connection(f"https://{FINAL}/a.ts") as a:
            first = _session_of(a)
        with conns.connection(f"https://{FINAL}/b.ts") as b:
            assert _session_of(b) == first, "同一线程接着拿同一个编号、同一个出口"
        other = []
        th = threading.Thread(target=lambda: other.append(_session_of(conns.connection(f"https://{FINAL}/c.ts").__enter__())))
        with conns.connection(f"https://{FINAL}/d.ts") as dd:      # 本线程占着自己的编号时别的线程拿别的
            th.start()
            th.join(5)
            assert other and other[0] != _session_of(dd)


def test_429_replaces_only_that_connections_session():
    conns = d.HostConnections(limits=SESSIONS, proxy_url=lambda spec: POOL)
    with conns.connection(f"https://{FINAL}/hold.ts"):
        with conns.connection(f"https://{FINAL}/a.ts") as a:
            before = _session_of(a)
        with pytest.raises(RuntimeError):
            with conns.connection(f"https://{FINAL}/b.ts") as b:
                assert _session_of(b) == before
                raise _http_error(503)
        with conns.connection(f"https://{FINAL}/c.ts") as c:
            assert _session_of(c) == before, "非 429 不换 IP"
        with pytest.raises(RuntimeError):
            with conns.connection(f"https://{FINAL}/e.ts") as e:
                raise _http_error(429)
        with conns.connection(f"https://{FINAL}/f.ts") as f:
            assert _session_of(f) != before, "这个出口被限了，换一个"
    assert conns.rotated == {FINAL: 1}
    assert conns.summary().endswith("（回 429 换出口 IP 1 次）")
    # 名额都还回去了：3 个代理编号全空
    assert conns._session_busy[FINAL] == set() and conns._in_use[(FINAL, "proxy")] == 0


def test_without_session_flag_the_proxy_url_is_used_as_is():
    conns = d.HostConnections(limits={FINAL: {**SESSIONS[FINAL], "proxy_session_per_connection": False}},
                              proxy_url=lambda spec: POOL)
    with conns.connection(f"https://{FINAL}/hold.ts"):
        with conns.connection(f"https://{FINAL}/a.ts") as a:
            assert a == {"proxies": {"http": POOL, "https": POOL}}
        with pytest.raises(RuntimeError):
            with conns.connection(f"https://{FINAL}/b.ts"):
                raise _http_error(429)
    assert conns.rotated == {}


def test_session_flag_with_missing_proxy_falls_back_to_direct_only(capsys):
    conns = d.HostConnections(limits=SESSIONS, proxy_url=lambda spec: None)
    with conns.connection(f"https://{FINAL}/hold.ts") as a:
        assert a == {}
        got = []
        th = threading.Thread(target=lambda: got.append(conns.connection(f"https://{FINAL}/b.ts").__enter__()))
        th.start()
        th.join(0.5)
        assert not got, "代理没配：只有直连名额，满了就等"
    th.join(5)
    assert got == [{}]
    assert "代理分流没配好" in capsys.readouterr().out


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
    assert cfg["host_limits"] == {"finalsignal.top": {"max_episodes": 16, "direct_connections": 16,
                                                      "proxy_connections": 256, "proxy": "env:TV_PROXY_V6_URL",
                                                      "proxy_session_per_connection": True,
                                                      "path_max_episodes": {"/fine/": 8}},
                                  "armadilloscp.workers.dev": {"max_episodes": 48, "source_ips": "env:TV_SOURCE_IPS",
                                                               "backfill_max_parked": 200},
                                  "hakunaymatata.com": {"max_episodes": 64, "source_ips": "env:TV_VIDLINK_SOURCE_IPS",
                                                        "direct_connections": "env:TV_VIDLINK_MAX_CONNECTIONS"}}
    # 🔴 belfast 不能限连接数：总速度随连接数线性增长，限 200 条时 Orion 掉了约 80%（2026-11-02）。
    orion = d._parse_host_limits(cfg["host_limits"])["armadilloscp.workers.dev"]
    assert (orion["direct"], orion["proxy"]) == (0, 0)
    final = d._parse_host_limits(cfg["host_limits"])["finalsignal.top"]
    assert final["path_max_episodes"] == {"/fine/": 8}
    # 11-04 起 finalsignal 走按连接分 session 的 IPv6 代理池：集数封顶时的全部分片连接（每集 segment_concurrency 条）
    # 直连 + 代理名额要装得下，不然分片排队等名额；直连只留少量，免得主 IPv4 吃 429。
    assert final["proxy_session_per_connection"] and final["proxy_spec"] == "env:TV_PROXY_V6_URL"
    assert final["direct"] + final["proxy"] >= final["max_episodes"] * cfg["segment_concurrency"]
    assert final["direct"] <= 32
    # 集数上限必须给别的主机留出槽位，否则 finalsignal 的慢集占满全部槽位、vidlink 排不上。
    assert cfg["max_workers"] - cfg["host_limits"]["finalsignal.top"]["max_episodes"] >= 16
    assert cfg["max_workers"] - cfg["host_limits"]["armadilloscp.workers.dev"]["max_episodes"] >= 16
    # 两台慢主机同时排满时也要留出 16 个：11-03 Orion 48 + finalsignal 16 正好占满 64 个槽位。
    # vidrock 停用（dropped_providers）时 belfast 不占槽位，槽位改回了 64；恢复 vidrock 要把 max_workers 一起调回 128。
    slow_hosts = ["finalsignal.top"]
    if "vidrock" not in (cfg["link_health"].get("dropped_providers") or []):
        slow_hosts.append("armadilloscp.workers.dev")
    slow = sum(cfg["host_limits"][h]["max_episodes"] for h in slow_hosts)
    assert cfg["max_workers"] - slow >= 16
    # mp4 连到 hakunaymatata 的连接不超过 1024 条（09-15 实测 ~1400 条时它全局 429）。
    assert cfg["host_limits"]["hakunaymatata.com"]["max_episodes"] * cfg["mp4_concurrency"] <= 1024
    assert d._host_rule_key("bcdn4.hakunaymatata.com", d._parse_host_limits(cfg["host_limits"])) == "hakunaymatata.com"
    assert cfg["download_queue_depth"] > cfg["max_workers"]
    assert cfg["max_parked_episodes"] == 20000


def test_orion_connections_wait_for_a_slot_and_never_use_the_proxy():
    """belfast 没配代理名额：超出的请求等名额、绝不走代理；子域共用一份名额。"""
    orion = d._parse_host_limits({"armadilloscp.workers.dev": {"direct_connections": 1}})
    conns = d.HostConnections(limits=orion, proxy_url=lambda spec: pytest.fail("不该走代理"))
    got_second = threading.Event()

    def second():
        with conns.connection("https://other.armadilloscp.workers.dev/b.ts") as route:
            assert route == {}
            got_second.set()

    with conns.connection("https://belfast.armadilloscp.workers.dev/a.ts") as route:
        assert route == {}
        thread = threading.Thread(target=second)
        thread.start()
        assert not got_second.wait(0.2), "子域共用名额，满了要等"
    assert got_second.wait(5)
    thread.join(5)


# ------------------------------------------------------------ belfast 集数上限（2026-11-02）

BELFAST = "belfast.armadilloscp.workers.dev"
ORION = d._parse_host_limits({"armadilloscp.workers.dev": {"direct_connections": 200, "max_episodes": 2,
                                                           "backfill_max_parked": 3}})


def orion(tid, ep_no=1, fetched_at=None):
    entry = ep(tid, ep_no, [{"url": f"https://{BELFAST}/{tid}-{ep_no}/master.m3u8",
                             "provider": "vidrock", "type": "m3u8", "link_checked": True}])
    return {**entry, "fetched_at": fetched_at or int(time.time())}


@pytest.fixture
def belfast_down(monkeypatch):
    down = {"on": True}
    monkeypatch.setattr(d, "host_circuit_remaining",
                        lambda node: 15.0 if down["on"] and BELFAST in node["url"] else None)
    return down


def test_orion_cap_leaves_slots_for_other_sources():
    a, b, c = orion(1), orion(2), orion(3)
    other = ep(9, 1, [mp4(9)])
    screen = d.DispatchScreen(limits=ORION)
    screen.set_source(Src([a, b, c, other]))
    assert [screen.poll()[1] for _ in range(3)] == [a, b, other], "第 3 集 Orion 排队，其他源照常派"
    screen.release(a)
    assert screen.poll() == ("item", c), "空出名额先派排队的"


def test_belfast_outage_does_not_block_other_sources(belfast_down):
    """🔴 belfast 全挂时它的集不能堵住来源：冷却堆满了会停止取集，别的源的集也跟着出不来。"""
    other = ep(9, 1, [mp4(9)])
    screen = d.DispatchScreen(limits=ORION, max_held=5)
    screen.set_source(Src([orion(i) for i in range(50)] + [other]))
    assert screen.poll() == ("item", other)
    assert screen.parked_on("armadilloscp.workers.dev") == 50
    assert screen._cooling == []


def test_parked_orion_stays_parked_until_breaker_clears(belfast_down):
    screen = d.DispatchScreen(limits=ORION)
    first = orion(1)
    screen.set_source(Src([first]))
    assert screen.poll() == ("wait", None), "熔断中不派，也不能报 done 把这一集丢掉"
    belfast_down["on"] = False
    assert screen.poll() == ("item", first)
    assert screen.parked_on("armadilloscp.workers.dev") == 0


def test_expired_orion_link_skips_the_queue(monkeypatch):
    """超过可用时长的链接派出去当场转重取：不占名额、不排队，也不算进回扫反压。"""
    monkeypatch.setattr(d, "LINK_MAX_AGE_SECONDS", {"vidrock": 180 * 60})
    old = orion(5, fetched_at=int(time.time()) - 4 * 3600)
    a, b = orion(1), orion(2)
    screen = d.DispatchScreen(limits=ORION)
    screen.set_source(Src([a, b, old]))
    assert [screen.poll()[1] for _ in range(3)] == [a, b, old]
    assert screen.parked_on("armadilloscp.workers.dev") == 0
    screen.release(old)
    assert screen._lane_busy["armadilloscp.workers.dev"] == 2, "它没占名额，还回来也不能多减"


def test_backfill_waits_only_when_its_own_host_queue_is_long(monkeypatch):
    monkeypatch.setattr(d, "HOST_LIMITS", ORION)
    screen = d.DispatchScreen(limits=ORION)
    monkeypatch.setattr(d, "active_dispatch_screen", screen)
    screen.set_source(Src([orion(i) for i in range(5)]))
    assert d.backfill_should_wait(orion(99)) is False
    assert [screen.poll()[0] for _ in range(2)] == ["item", "item"]
    screen.poll()                                   # 其余 3 集进队，正好到上限
    assert screen.parked_on("armadilloscp.workers.dev") == 3
    assert d.backfill_should_wait(orion(99)) is True
    assert d.backfill_should_wait(ep(9, 1, [mp4(9)])) is False, "别的主机不受影响"
    monkeypatch.setattr(d, "active_dispatch_screen", None)
    assert d.backfill_should_wait(orion(99)) is False, "没有闸门（单独跑取流）不等"


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


# ------------------------------------------------------------ 路径单独封顶：/fine/ 占不满名额（2026-10-31）

CAPPED = {"finalsignal.top": {"max_episodes": 3, "direct": 2, "proxy": 0, "proxy_spec": "residential",
                              "path_max_episodes": {"/fine/": 1}}}


def fine(tid, episode, host="sun1.finalsignal.top"):
    return ep(tid, episode, [{"url": f"https://{host}/fine/s/{tid}-{episode}/master.m3u8",
                              "provider": "vidfast", "type": "m3u8"}])


def good(tid, episode, host="sun1.finalsignal.top"):
    return ep(tid, episode, [hls(tid, episode, host)])


def test_capped_path_queues_without_blocking_other_paths():
    screen = d.DispatchScreen(limits=CAPPED)
    a, b, c = fine(6, 1), fine(6, 2), good(6, 3)
    screen.set_source(Src([a, b, c]))
    assert [screen.poll()[1] for _ in range(2)] == [a, c], "第 2 集 /fine/ 排队，别的路径照常派"
    assert screen.poll() == ("wait", None)
    screen.release(c)
    assert screen.poll() == ("wait", None), "空出的名额不是 /fine/ 的"
    screen.release(a)
    assert screen.poll() == ("item", b)
    screen.release(b)
    assert screen.poll() == ("done", None)


def test_capped_path_shares_the_host_budget_and_is_served_first():
    screen = d.DispatchScreen(limits=CAPPED)
    x, y, z, w, f = good(7, 1), good(7, 2), good(7, 3), good(7, 4), fine(7, 5)
    screen.set_source(Src([x, y, z, w, f]))
    assert [screen.poll()[1] for _ in range(3)] == [x, y, z]
    assert screen.poll() == ("wait", None), "主机上限 3 集用满，/fine/ 有余量也要等"
    screen.release(x)
    assert screen.poll() == ("item", f), "/fine/ 先派，否则会被别的路径一直挡在外面"
    screen.release(y)
    assert screen.poll() == ("item", w)


def test_path_cap_matches_on_the_first_node_and_ignores_other_paths():
    screen = d.DispatchScreen(limits=CAPPED)
    assert screen._lane_key(fine(8, 1)) == ("finalsignal.top", "/fine/")
    assert screen._lane_key(good(8, 2)) == ("finalsignal.top", None)
    assert screen._lane_key(ep(8, 3, [hls(8, 3), fine(8, 3)["urls"][0]])) == ("finalsignal.top", None)
    assert screen._lane_key(ep(8, 4, [{"url": "https://sun1.finalsignal.top/x/fine/m.m3u8",
                                       "provider": "vidup", "type": "m3u8"}])) == ("finalsignal.top", None)
    assert screen._lane_key(ep(8, 5, [mp4(8, 5)])) is None


def test_parse_host_limits_drops_non_positive_path_caps():
    parsed = d._parse_host_limits({"A.Top": {"max_episodes": 4, "direct_connections": 2,
                                             "path_max_episodes": {"/fine/": 16, "/off/": 0}}})
    assert parsed["a.top"]["path_max_episodes"] == {"/fine/": 16}
    assert d._parse_host_limits({"a.top": None})["a.top"]["path_max_episodes"] == {}
    assert d._parse_host_limits(None) == {}


@pytest.mark.parametrize("url, expected", [
    ("https://a.top/fine/s/x.m3u8?t=1", "/fine/s/x.m3u8"),
    ("https://a.top/", "/"),
    ("https://a.top", ""),
    ("not a url", ""),
    (None, ""),
])
def test_path_of(url, expected):
    assert d._path_of(url) == expected


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


def test_main_loop_finishes_episodes_queued_behind_a_path_cap(tmp_path, monkeypatch):
    """🔴 /fine/ 的名额同样要按集归还，否则封顶之后排队的 /fine/ 永远派不出去。"""
    from test_overlay_tv import _pipeline_env

    monkeypatch.setattr(d, "HOST_LIMITS", CAPPED)
    entries = [{**fine(97, i), "title": "x"} for i in range(1, 5)]
    entries += [{**good(98, i), "title": "y"} for i in range(1, 4)]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert sorted(uploads) == [f"97_S01E0{i}" for i in range(1, 5)] + [f"98_S01E0{i}" for i in range(1, 4)]


# ------------------------------------------------------------ 多出口 IP（2026-11-03，belfast 按客户端 IP 封顶）

TWO_IPS = {"armadilloscp.workers.dev": {"max_episodes": 96, "direct": 0, "proxy": 0, "proxy_spec": "",
                                        "source_ips": ["10.0.0.1", "10.0.0.2"]}}


def test_source_ips_from_env_skip_unbindable(monkeypatch, capsys):
    monkeypatch.setenv("TEST_SOURCE_IPS", "127.0.0.1, 192.0.2.1 127.0.0.1")
    assert d._source_ips("env:TEST_SOURCE_IPS") == ["127.0.0.1"]
    assert "出口 IP 192.0.2.1 本机绑不上" in capsys.readouterr().out
    monkeypatch.delenv("TEST_SOURCE_IPS")
    assert d._source_ips("env:TEST_SOURCE_IPS") == []
    assert d._source_ips(None) == []


def test_source_ips_scale_episodes_per_ip(monkeypatch):
    monkeypatch.setattr(d, "_can_bind", lambda ip: ip != "10.0.0.9")
    limits = d._parse_host_limits({
        "armadilloscp.workers.dev": {"max_episodes": 48, "source_ips": ["10.0.0.1", "10.0.0.9", "10.0.0.2"]},
        "hakunaymatata.com": {"max_episodes": 64},
    })
    orion = limits["armadilloscp.workers.dev"]
    assert (orion["source_ips"], orion["max_episodes"], orion["extra_episodes"]) == (["10.0.0.1", "10.0.0.2"], 96, 48)
    assert (limits["hakunaymatata.com"]["max_episodes"], limits["hakunaymatata.com"]["extra_episodes"]) == (64, 0)
    monkeypatch.setattr(d, "_can_bind", lambda ip: False)
    orion = d._parse_host_limits({"armadilloscp.workers.dev": {"max_episodes": 48, "source_ips": ["10.0.0.1"]}})
    assert (orion["armadilloscp.workers.dev"]["max_episodes"], orion["armadilloscp.workers.dev"]["extra_episodes"]) == (48, 0), \
        "IP 都绑不上（重启后第二个 IP 没了）就照单 IP 跑"


def test_shipped_config_with_two_source_ips_keeps_other_hosts_slots(monkeypatch):
    import os

    import yaml

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]
    monkeypatch.setenv("TV_SOURCE_IPS", "172.96.172.17,103.195.100.154")
    monkeypatch.setattr(d, "_can_bind", lambda ip: True)
    limits = d._parse_host_limits(cfg["host_limits"])
    slots = cfg["max_workers"] + sum(limit["extra_episodes"] for limit in limits.values())
    slow = limits["armadilloscp.workers.dev"]["max_episodes"] + limits["finalsignal.top"]["max_episodes"]
    assert (limits["armadilloscp.workers.dev"]["max_episodes"], slots) == (96, cfg["max_workers"] + 48)
    assert slots - slow == cfg["max_workers"] - 48 - 16, "别的主机分到的槽位与单 IP 时一样"
    assert cfg["download_queue_depth"] > slots


def test_source_ips_pick_the_least_busy_ip():
    conns = d.HostConnections(limits=TWO_IPS, proxy_url=lambda spec: pytest.fail("不该走代理"))
    url = f"https://{BELFAST}/seg.ts"
    with conns.connection(url) as a, conns.connection(url) as b, conns.connection(url) as c:
        assert [a, b, c] == [{"source_ip": "10.0.0.1"}, {"source_ip": "10.0.0.2"}, {"source_ip": "10.0.0.1"}]
        with conns.connection(url) as e:
            assert e == {"source_ip": "10.0.0.2"}, "没设连接上限，不用等"
    with conns.connection(url) as a:
        with conns.connection(url) as b:
            assert (a, b) == ({"source_ip": "10.0.0.1"}, {"source_ip": "10.0.0.2"})
    assert conns.served_ips == {"10.0.0.1": 3, "10.0.0.2": 3}
    assert conns.summary() == "限流主机分片请求：直连 6 次（10.0.0.1 3 次、10.0.0.2 3 次）、经代理 0 次"


def test_direct_cap_counts_per_source_ip():
    limits = {FINAL: {**LIMITS[FINAL], "direct": 1, "proxy": 0, "source_ips": ["10.0.0.1", "10.0.0.2"]}}
    conns = d.HostConnections(limits=limits, proxy_url=lambda spec: None)
    got_third = threading.Event()

    def third():
        with conns.connection(f"https://{FINAL}/c.ts"):
            got_third.set()

    with conns.connection(f"https://{FINAL}/a.ts") as a, conns.connection(f"https://{FINAL}/b.ts") as b:
        assert (a, b) == ({"source_ip": "10.0.0.1"}, {"source_ip": "10.0.0.2"})
        thread = threading.Thread(target=third)
        thread.start()
        assert not got_third.wait(0.2), "每个 IP 1 条，两条都占着时第 3 条要等"
    assert got_third.wait(5)
    thread.join(5)


def test_segment_request_goes_out_from_the_source_ip(monkeypatch):
    monkeypatch.setattr(d, "HOST_CONNECTIONS", d.HostConnections(limits=TWO_IPS))
    calls = []

    def fake_request(method, url, **kw):
        calls.append(kw.get("source_ip"))
        return b"\x47" * 188 * 4

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    monkeypatch.setattr(d, "validate_segment_content", lambda content, url: None)
    with d.HOST_CONNECTIONS.connection(f"https://{BELFAST}/hold.ts"):     # 10.0.0.1 先占一条
        d.download_single_segment(f"https://{BELFAST}/s1.ts", 0, 1, 0)
    d.download_single_segment("https://bcdn.hakunaymatata.com/s2.ts", 1, 1, 0)
    assert calls == ["10.0.0.2", None]


def test_bound_session_really_sends_from_the_source_ip():
    import http.server

    clients = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            clients.append(self.client_address[0])
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/seg.ts"
        assert d.request_with_retry("GET", url, retries=1, source_ip="127.0.0.1", total_timeout=5) == b"ok"
    finally:
        server.shutdown()
    assert clients == ["127.0.0.1"]
    adapter = d.get_session("127.0.0.1").get_adapter("https://example.com")
    assert adapter.poolmanager.connection_pool_kw["source_address"] == ("127.0.0.1", 0)
    assert d.get_session("127.0.0.1") is not d.get_session(), "默认出口的会话不受影响"
    assert "source_address" not in d.get_session().get_adapter("https://example.com").poolmanager.connection_pool_kw


# ------------------------------------------------------------ vidlink mp4 也按 host_limits 挑出口 IP、受连接上限约束

VID = "bcdn.hakunaymatata.com"


class _Resp:
    def __init__(self, status, body=b""):
        self.status_code, self.content, self.headers = status, body, {"Content-Range": f"bytes 0-0/{len(body) or 1000}"}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def iter_content(self, chunk_size=None):
        yield self.content


def _fake_sessions(monkeypatch, calls, body=b"\x00" * 16):
    class _S:
        def __init__(self, ip):
            self.ip = ip

        def request(self, method, url, **kw):
            calls.append((self.ip, "proxies" in kw))
            return _Resp(206, body)

        def get(self, url, **kw):
            calls.append((self.ip, "proxies" in kw))
            return _Resp(206, body)

    def get_session(source_ip=None):
        return _S(source_ip)

    monkeypatch.setattr(d, "get_session", get_session)


def test_int_setting_reads_env_and_defaults_to_unlimited(monkeypatch):
    monkeypatch.setenv("TEST_CAP", "64")
    assert d._int_setting("env:TEST_CAP") == 64
    monkeypatch.delenv("TEST_CAP")
    assert d._int_setting("env:TEST_CAP") == 0
    assert (d._int_setting(16), d._int_setting(None), d._int_setting("x"), d._int_setting(-3)) == (16, 0, 0, 0)


def test_shipped_vidlink_limits_follow_env(monkeypatch):
    import os

    import yaml

    raw = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]["host_limits"]
    monkeypatch.delenv("TV_VIDLINK_SOURCE_IPS", raising=False)
    monkeypatch.delenv("TV_VIDLINK_MAX_CONNECTIONS", raising=False)
    plain = d._parse_host_limits(raw)["hakunaymatata.com"]
    assert (plain["source_ips"], plain["direct"], plain["max_episodes"]) == ([], 0, 64), "没配的机器与原来一样"
    monkeypatch.setenv("TV_VIDLINK_SOURCE_IPS", "127.0.0.1")
    monkeypatch.setenv("TV_VIDLINK_MAX_CONNECTIONS", "64")
    on19 = d._parse_host_limits(raw)["hakunaymatata.com"]
    assert (on19["source_ips"], on19["direct"], on19["max_episodes"], on19["extra_episodes"]) == (["127.0.0.1"], 64, 64, 0)
    assert on19["proxy"] == 0, "vidlink 不走代理"


def test_mp4_chunk_probe_and_caption_go_out_from_the_source_ip(monkeypatch):
    limits = {"hakunaymatata.com": {"max_episodes": 64, "direct": 2, "proxy": 0, "proxy_spec": "",
                                    "source_ips": ["10.0.0.9"]}}
    monkeypatch.setattr(d, "HOST_CONNECTIONS", d.HostConnections(limits=limits))
    monkeypatch.setattr(d, "validate_segment_content", lambda content, url: None)
    calls = []
    _fake_sessions(monkeypatch, calls, body=b"\x00" * 16)
    assert d._download_mp4_chunk(f"https://{VID}/a.mp4", {}, 0, 15, 0) == b"\x00" * 16
    d._mp4_probe_total_size(f"https://{VID}/a.mp4", {}, None)
    d._fetch_caption_text({"url": f"https://{VID}/a.vtt"})
    d._download_mp4_chunk("https://other.example/a.mp4", {}, 0, 15, 0)
    assert calls == [("10.0.0.9", False)] * 3 + [(None, False)], "限流主机走配好的出口 IP，别的主机照旧"
    assert d.HOST_CONNECTIONS._in_use[("hakunaymatata.com", "direct", "10.0.0.9")] == 0, "名额都还了"


def test_mp4_chunks_wait_for_the_connection_cap():
    limits = {"hakunaymatata.com": {"max_episodes": 64, "direct": 1, "proxy": 0, "proxy_spec": "",
                                    "source_ips": ["10.0.0.9"]}}
    conns = d.HostConnections(limits=limits, proxy_url=lambda spec: pytest.fail("vidlink 不该走代理"))
    got = []
    with conns.connection(f"https://{VID}/a.mp4") as first:
        assert first == {"source_ip": "10.0.0.9"}
        th = threading.Thread(target=lambda: got.append(conns.connection(f"https://{VID}/b.mp4").__enter__()))
        th.start()
        th.join(0.5)
        assert not got, "上限 1 条：第二块等着"
    th.join(5)
    assert got == [{"source_ip": "10.0.0.9"}]
