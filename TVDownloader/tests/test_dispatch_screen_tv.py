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


def test_shipped_due_refetch_and_fake_link_config():
    import yaml, os

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]["link_health"]
    assert cfg["due_refetch"] == {"enabled": True, "workers": 16, "max_per_minute": 60,
                                  "max_per_episode": 4, "retry_minutes": 360, "max_wait_hours": 60}
    assert cfg["fake_link_hosts"] == ["workers.dev"]
    assert d.FAKE_LINK_HOSTS == ("workers.dev",)


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


# ------------------------------------------------------------ 到期重取

BX = "bcdnxw.hakunaymatata.com"


class _Hook:
    """到期重取线程池的替身：记录投递，失败的集从 missed 交还。"""

    def __init__(self):
        self.sent = []
        self.missed = []
        self.pending = 0
        self.accept = True

    def dispatch(self, entries):
        if not self.accept:
            return 0
        self.sent.extend(entries)
        return len(entries)

    def collect_missed(self):
        out, self.missed = self.missed, []
        return out

    def pending_count(self):
        return self.pending


class _Wall:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def due_env(monkeypatch):
    hook = _Hook()
    monkeypatch.setattr(d, "due_refetch_hook", hook)
    monkeypatch.setattr(d, "DUE_REFETCH_WORKERS", 4)
    monkeypatch.setattr(d, "DUE_REFETCH_PER_MINUTE", 60.0)
    monkeypatch.setattr(d, "DUE_REFETCH_MAX_PER_EPISODE", 3)
    monkeypatch.setattr(d, "DUE_REFETCH_RETRY_SECONDS", 3600.0)
    monkeypatch.setattr(d, "DUE_REFETCH_MAX_WAIT_SECONDS", 60 * 3600.0)
    return hook


def _screen(items=(), closed=True, skipped=None):
    wall = _Wall()
    clock = _Clock()
    screen = d.DispatchScreen(on_skip=lambda e, why: (skipped if skipped is not None else []).append(why),
                              clock=clock, wall=wall)
    src = Src(items, closed=closed)
    screen.set_source(src)
    return screen, src, wall, clock


def test_bad_vidlink_episode_waits_for_the_cache_then_goes_to_refetch_not_download(due_env):
    """🔴 到点前不派也不问；到点只交给到期重取，不拿死链接去下。失败行只记一次。"""
    skipped = []
    signed = NOW - 3600
    screen, _src, wall, clock = _screen([ep(10, 1, [vid("10", host=BX, signed=signed)])], skipped=skipped)
    assert screen.poll() == ("wait", None), "还在等刷新，本轮不能收尾"
    assert due_env.sent == []
    assert len(skipped) == 1 and "到点自动重取" in skipped[0]
    assert d._NEEDS_REFETCH_MARKER not in skipped[0]

    wall.t = signed + 49 * 3600
    clock.t += 1
    screen.poll()     # 真来源在重取回来之前报 wait，这个替身来源直接报 done
    assert [e["episode"] for e in due_env.sent] == [1]
    assert len(skipped) == 1


def test_episode_already_past_the_roll_is_refetched_right_away(due_env):
    screen, _src, _wall_, _clock_ = _screen([ep(11, 1, [vid("11", host=BX, signed=NOW - 50 * 3600)])], closed=False)
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 1


def test_new_good_link_from_refetch_is_dispatched(due_env):
    screen, src, _wall_, _clock_ = _screen([ep(12, 1, [vid("12", host=BX, signed=NOW - 50 * 3600)])], closed=False)
    assert screen.poll() == ("wait", None)
    src.items.append(ep(12, 1, [vid("12", host="hcdn3.hakunaymatata.com", signed=NOW)]))
    state, entry = screen.poll()
    assert state == "item" and "hcdn3" in entry["urls"][0]["url"]


def test_refetch_landing_on_bad_host_again_waits_for_the_new_roll(due_env):
    """换回来还在 bcdnxw、但签名是新的：按新的刷新时刻再挂，不立刻再问。"""
    screen, src, wall, clock = _screen([ep(13, 1, [vid("13", host=BX, signed=NOW - 50 * 3600)])], closed=False)
    assert screen.poll() == ("wait", None)
    src.items.append(ep(13, 1, [vid("13", host=BX, signed=NOW)]))
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 1
    wall.t = NOW + 49 * 3600
    clock.t += 1
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 2


def test_same_link_back_is_asked_again_only_after_the_retry_interval(due_env):
    """🔴 缓存没刷新、拿回同一条：刷新时刻已过，不隔开就会原地空转、几秒烧光次数。"""
    bad = [vid("14", host=BX, signed=NOW - 50 * 3600)]
    screen, src, wall, clock = _screen([ep(14, 1, bad)], closed=False)
    assert screen.poll() == ("wait", None)
    src.items.append(ep(14, 1, bad))
    assert screen.poll() == ("wait", None)
    clock.t += 100
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 1, "同一条链接不能马上再问"
    wall.t = NOW + 3601
    clock.t += 100
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 2


def test_refetch_budget_per_episode_then_left_for_next_run(due_env):
    skipped = []
    bad = [vid("15", host=BX, signed=NOW - 50 * 3600)]
    screen, src, wall, clock = _screen([ep(15, 1, bad)], closed=False, skipped=skipped)
    for _ in range(3):
        assert screen.poll() == ("wait", None)
        src.items.append(ep(15, 1, bad))
        wall.t += 3601
        clock.t += 100
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 3
    assert "已重取 3 次仍只有坏链接" in skipped[-1]
    assert screen.due_gave_up == 1


def test_missed_refetch_is_retried_later(due_env):
    entry = ep(16, 1, [vid("16", host=BX, signed=NOW - 50 * 3600)])
    screen, _src, wall, clock = _screen([entry], closed=False)
    assert screen.poll() == ("wait", None)
    due_env.missed.append(due_env.sent[0])
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 1, "没问到的集隔一段时间再问"
    wall.t = NOW + 3601
    clock.t += 100
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 2


def test_refetch_is_rate_limited(due_env):
    """每分钟 60 集：突发最多 2 倍线程数（8），之后每秒放一集。"""
    items = [ep(17, i, [vid("17", i, host=BX, signed=NOW - 50 * 3600)]) for i in range(1, 21)]
    screen, _src, _wall_, clock = _screen(items, closed=False)
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 8
    due_env.sent.clear()
    clock.t += 3
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 3


def test_refetch_backlog_in_the_pool_is_bounded(due_env):
    items = [ep(18, i, [vid("18", i, host=BX, signed=NOW - 50 * 3600)]) for i in range(1, 6)]
    screen, _src, _wall_, _clock_ = _screen(items, closed=False)
    due_env.pending = 8
    assert screen.poll() == ("wait", None)
    assert due_env.sent == [], "线程池积压到上限就先不投"
    due_env.pending = 0
    assert screen.poll() == ("wait", None)
    assert len(due_env.sent) == 5


def test_rejected_dispatch_leaves_episode_for_next_run_without_spinning(due_env):
    due_env.accept = False
    screen, _src, _wall_, _clock_ = _screen([ep(19, 1, [vid("19", host=BX, signed=NOW - 50 * 3600)])])
    assert screen.poll() == ("done", None), "取流侧已终止：不投也不空转"
    assert screen.due_sent == 0


def test_source_done_waits_for_due_episodes_only_up_to_the_limit(due_env, capsys):
    """🔴 其余的集都完了：等到期的集，但有上限，不能让进程无限挂着。"""
    screen, _src, wall, clock = _screen([ep(20, 1, [vid("20", host=BX, signed=NOW)])])
    assert screen.poll() == ("wait", None)
    assert "最多再等 60 小时" in capsys.readouterr().out
    wall.t = NOW + 48 * 3600
    assert screen.poll() == ("wait", None)
    wall.t = NOW + 60 * 3600 + 1
    # 到了 49 小时的刷新点会先投出去，这里造一集排在更后面的看上限
    assert screen.poll() in (("wait", None), ("done", None))
    screen2, _s, wall2, _c = _screen([ep(21, 1, [vid("21", host=BX, signed=NOW + 100 * 3600)])])
    assert screen2.poll() == ("wait", None)
    wall2.t = NOW + 60 * 3600 + 1
    assert screen2.poll() == ("done", None)
    assert screen2.due_gave_up == 1
    assert "留给下次运行" in capsys.readouterr().out


def test_preflight_leaves_due_episodes_to_due_refetch(due_env, monkeypatch, capsys):
    """🔴 装了到期重取：到点的 vidlink 集不再占启动预检的名额（每次启动才 500 集）。"""
    monkeypatch.setattr(d, "is_stale_entry", lambda entry, now=None: True)
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: set())
    monkeypatch.setattr(d.time, "time", lambda: NOW)
    entries = [ep(22, 1, [vid("22", host=BX, signed=NOW - 50 * 3600)]), ep(22, 2, [hls("22", 2)])]
    stale, _ = d._select_stale_entries(entries)
    assert [e["episode"] for e in stale] == [2]


def test_without_hook_due_episode_goes_to_preflight_and_download_as_before(monkeypatch):
    monkeypatch.setattr(d, "due_refetch_hook", None)
    monkeypatch.setattr(d, "is_stale_entry", lambda entry, now=None: True)
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: set())
    monkeypatch.setattr(d.time, "time", lambda: NOW)
    entry = ep(23, 1, [vid("23", host=BX, signed=NOW - 50 * 3600)])
    assert d._select_stale_entries([entry])[0] == [entry]
    screen = d.DispatchScreen(wall=lambda: NOW)
    screen.set_source(Src([entry]))
    assert screen.poll()[0] == "item"


# ------------------------------------------------------------ 假链接节点

FAKE = "https://white-tooth-83b1.foo.workers.dev/video.m3u8?q=abc"


def test_drop_fake_nodes_only_touches_unchecked_nodes_on_fake_hosts(monkeypatch):
    monkeypatch.setattr(d, "FAKE_LINK_HOSTS", ("workers.dev",))
    fake = {"url": FAKE, "provider": "vidup", "type": "m3u8"}
    checked = {**fake, "link_checked": True}
    real = vid("30")
    entry = ep(30, 1, [fake, real])
    stripped, removed = d.drop_fake_nodes(entry)
    assert removed == 1 and stripped["urls"] == [real]
    assert len(entry["urls"]) == 2, "不改原条目"
    same = ep(30, 2, [checked, real, "https://old.example/plain.m3u8"])
    assert d.drop_fake_nodes(same) == (same, 0), "验证过的、别的主机、老格式字符串都不动"


def test_episode_with_fake_node_no_longer_escapes_the_vidlink_hold(monkeypatch):
    """🔴 假节点算"别的节点"的话，bcdnxw + workers.dev 的集会绕过挂起、到点也换不到链接。"""
    monkeypatch.setattr(d, "FAKE_LINK_HOSTS", ("workers.dev",))
    entry = ep(31, 1, [{"url": FAKE, "provider": "vidup", "type": "m3u8"},
                       vid("31", host=BX, signed=NOW - 3600)])
    assert d.vidlink_refresh_due(entry, NOW) is None, "读入前仍带假节点"
    stripped, _ = d.drop_fake_nodes(entry)
    assert "长期故障" in d.vidlink_refresh_due(stripped, NOW)[1]
