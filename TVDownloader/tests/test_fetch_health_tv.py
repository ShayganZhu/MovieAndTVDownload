"""取流源熔断 + 推迟名单 + enc-dec / vidlink 直连（电影侧 7631adb 移植，2026-10-30）的离线用例。

锁死几件错了会误判死 / 空转 / 丢集的事：
  - 熔断中没问到的源不算答过：只要有源被跳过就绝不判死，也不写 fail.txt；
  - 推迟的集只试一次、不重试、不走判死二次确认；
  - 源恢复后推迟的集放回下一轮，跑完仍推迟的写 provider_down_file；
  - 直连只承载 enc-dec / vidlink，被拒立刻改走代理；判死二次确认那一跳全走代理。
"""

import json

import pytest

import tv_ids_to_links as m
from test_tv_ids_to_links import PAGE, _install_fake_session


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _use_breaker(monkeypatch, trip=3, interval=600, clock=None):
    b = m.ProviderBreaker(trip_after=trip, probe_interval=interval, enabled=True,
                          clock=clock or _Clock())
    monkeypatch.setattr(m, "PROVIDER_BREAKER", b)
    return b


# ------------------------------------------------------------ 熔断器本身（与电影侧同一份实现）

def test_breaker_trips_only_after_consecutive_outages():
    b = m.ProviderBreaker(trip_after=3, enabled=True, clock=_Clock())
    b.record("vidup", "down")
    b.record("vidup", "down")
    assert b.allow("vidup") == "call"
    b.record("vidup", "down")
    assert b.is_down("vidup") and b.allow("vidup") == "skip"


def test_breaker_streak_resets_on_answer_and_ignores_neutral():
    b = m.ProviderBreaker(trip_after=3, enabled=True, clock=_Clock())
    for outcome in ("down", "down", None, "up", "down", "down"):
        b.record("v", outcome)
    assert not b.is_down("v")
    b.record("v", None)
    b.record("v", "down")
    assert b.is_down("v")


def test_breaker_probes_one_call_per_interval_and_recovers():
    clock = _Clock()
    b = m.ProviderBreaker(trip_after=1, probe_interval=600, enabled=True, clock=clock)
    b.record("v", "down")
    assert b.allow("v") == "skip"
    clock.t += 600
    assert b.allow("v") == "probe"
    assert b.allow("v") == "skip", "同一时刻只放一集去试探"
    b.record("v", "down", probe=True)
    assert b.is_down("v")
    clock.t += 600
    assert b.allow("v") == "probe"
    b.record("v", None, probe=True)
    assert b.allow("v") == "probe", "中性结局后下一个调用马上再试探"
    b.record("v", "up", probe=True)
    assert not b.is_down("v") and b.allow("v") == "call"


def test_breaker_disabled_never_skips():
    b = m.ProviderBreaker(trip_after=1, enabled=False, clock=_Clock())
    for _ in range(5):
        b.record("v", "down")
    assert b.allow("v") == "call" and not b.is_down("v")


@pytest.mark.parametrize("name,exc,expected", [
    ("vidup", None, "up"),
    ("vidup", m.NoSource("none"), "up"),
    ("vidup", m.HttpStatusError(403, "page"), "down"),
    ("vidfast", m.HttpStatusError(502, "page"), "down"),
    ("vidup", m.HttpStatusError(429, "page"), None),                 # 单个出口限流
    ("vidup", m.HttpStatusError(500, "enc-vidup(stage1)"), None),    # enc-dec 不算源的账
    ("vidup", m.HttpStatusError(403, "stage1"), None),               # 不是第一跳
    ("vidlink", m.HttpStatusError(403, "vidlink-api"), "down"),
    ("vidlink", m.HttpStatusError(500, "enc-vidlink"), None),
    ("vidup", Exception("curl: (28) Operation timed out"), None),
])
def test_breaker_outcome_classification(name, exc, expected):
    assert m._breaker_outcome(name, exc) == expected


# ------------------------------------------------------------ process_episode 的 deferred

def test_blocked_page_trips_breaker_and_vidup_is_no_longer_called(monkeypatch):
    """vidup 页面一律 502：熔断前照旧按瞬时重试，熔断后不再请求它，这一集推迟而非判死。"""
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=502)
    b = _use_breaker(monkeypatch, trip=3)

    assert m.process_episode("1", 1, 1, providers=["vidup", "vidlink"]) == ("retry", None)
    assert b.is_down("vidup")
    calls = len(seen["page_urls"])

    status, result = m.process_episode("2", 1, 1, providers=["vidup", "vidlink"])
    assert status == "deferred"
    assert result == {"tmdbId": "2", "season": 1, "episode": 1, "providers": ["vidup"]}
    assert len(seen["page_urls"]) == calls, "熔断后不该再请求 vidup 页面"


def test_deferred_takes_one_attempt_and_is_never_dead(monkeypatch):
    seen = _install_fake_session(monkeypatch, PAGE, [])
    _use_breaker(monkeypatch, trip=1).record("vidup", "down")
    sleeps = []
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    status, result = m.process_episode("42", 1, 3, providers=["vidup", "vidlink", "vidfast"])

    assert status == "deferred" and result["providers"] == ["vidup"]
    assert seen["order"].count("vidlink") == 1, "不重试，也不走判死二次确认"
    assert sleeps == []


def test_other_provider_url_is_ok_while_one_is_down(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [], vidfast_page_status=200,
                          vidfast_streams=[{"url": "v1"}])
    _use_breaker(monkeypatch, trip=1).record("vidup", "down")
    status, result = m.process_episode("42", 1, 1, providers=["vidup", "vidfast"])
    assert status == "ok" and [u["provider"] for u in result["urls"]] == ["vidfast"]


def test_all_providers_answering_nosource_is_still_dead(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    _use_breaker(monkeypatch, trip=3)
    assert m.process_episode("42", 1, 1, providers=["vidup", "vidlink"]) == ("dead", None)


# ------------------------------------------------------------ run_batch / 多轮循环

def test_run_batch_records_deferred_outside_retry_and_fail(tmp_path, monkeypatch):
    fail = tmp_path / "fail.txt"
    monkeypatch.setattr(m, "process_episode", lambda tid, s, e, **kw: (
        "deferred", {"tmdbId": str(tid), "season": s, "episode": e, "providers": ["vidup"]}))
    deferred = {}
    retry = m.run_batch([("7", 1, 2)], tmp_path / "r.jsonl", fail, 1, deferred_out=deferred)
    assert retry == []
    assert deferred == {("7", 1, 2): ["vidup"]}
    assert not fail.exists() or fail.read_text(encoding="utf-8") == "", "推迟的集绝不能写进 fail.txt"


def test_run_batch_without_a_deferred_dict_treats_deferred_as_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "process_episode", lambda tid, s, e, **kw: (
        "deferred", {"tmdbId": str(tid), "season": s, "episode": e, "providers": ["vidup"]}))
    assert m.run_batch([("7", 1, 2)], tmp_path / "r.jsonl", tmp_path / "f.txt", 1) == [("7", 1, 2)]


@pytest.fixture
def main_env(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "results.jsonl"),
        "fail_file": str(tmp_path / "fail.txt"), "seasons_cache": str(tmp_path / "c.jsonl"),
        "provider_down_file": str(tmp_path / "provider_down.txt"),
        "max_workers": 1, "max_rounds": 4,
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {
        "1": {"tmdbId": "1", "name": "S", "year": 2000,
              "seasons": [{"season": 1, "episodes": [1, 2]}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})
    monkeypatch.setattr(m, "_TMDB_NAMES", {})
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    return tmp_path


def test_main_keeps_deferred_out_of_later_rounds_and_lists_them(main_env, monkeypatch):
    """熔断一直没恢复：推迟的集不进后续轮次空转，跑完写进 provider_down_file。"""
    _use_breaker(monkeypatch, trip=1).record("vidup", "down")
    rounds = []

    def fake_batch(pending, rf, ff, mw, write_dead=True, deferred_out=None, **kw):
        rounds.append(list(pending))
        if len(rounds) == 1:
            deferred_out[("1", 1, 1)] = ["vidup"]
            return [("1", 1, 2)]          # 另一集瞬时失败，下一轮重跑
        return []

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main()
    assert rounds == [[("1", 1, 1), ("1", 1, 2)], [("1", 1, 2)]]
    assert (main_env / "provider_down.txt").read_text(encoding="utf-8") == "1\t1\t1\tvidup\n"
    assert "1\t1\t1" not in (main_env / "fail.txt").read_text(encoding="utf-8") \
        if (main_env / "fail.txt").exists() else True


def test_main_requeues_deferred_episodes_once_the_provider_recovers(main_env, monkeypatch):
    b = _use_breaker(monkeypatch, trip=1)
    b.record("vidup", "down")
    rounds = []

    def fake_batch(pending, rf, ff, mw, write_dead=True, deferred_out=None, **kw):
        rounds.append(list(pending))
        if len(rounds) == 1:
            deferred_out[("1", 1, 1)] = ["vidup"]
            b.record("vidup", "up")       # 本轮内源恢复了
        return []

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main()
    assert rounds[1] == [("1", 1, 1)], "源恢复后推迟的集要放回下一轮重查"
    assert not (main_env / "provider_down.txt").exists(), "没有推迟的集时不凭空建空文件"


def test_main_clears_a_stale_provider_down_list(main_env, monkeypatch):
    """上次留下的名单要清空：这次没有推迟的集时把已存在的文件覆盖成空。"""
    (main_env / "provider_down.txt").write_text("9\t1\t1\tvidup\n", encoding="utf-8")
    monkeypatch.setattr(m, "run_batch", lambda *a, **kw: [])
    m.main()
    assert (main_env / "provider_down.txt").read_text(encoding="utf-8") == ""


def test_write_provider_down_overwrites_and_clears(tmp_path):
    path = tmp_path / "provider_down.txt"
    m.write_provider_down(path, {("2", 1, 3): ["vidup", "vidfast"], ("1", 0, 1): ["vidup"]})
    assert path.read_text(encoding="utf-8") == "1\t0\t1\tvidup\n2\t1\t3\tvidup,vidfast\n"
    m.write_provider_down(path, {})
    assert path.read_text(encoding="utf-8") == ""


# ------------------------------------------------------------ 直连分流

class _Resp:
    def __init__(self, status=200, text=""):
        self.status_code = status
        self.text = text


class _Line:
    def __init__(self, name, seen, reply):
        self.name, self.seen, self.reply = name, seen, reply

    def get(self, url, **kw):
        self.seen.append((self.name, url))
        return self.reply(url)

    post = get


def _enable_direct(monkeypatch, encdec=True, vidlink=True, clock=None):
    clock = clock or _Clock()
    monkeypatch.setattr(m, "ENCDEC_ROUTE", m._DirectRoute("enc-dec", encdec, cooldown=600, clock=clock))
    monkeypatch.setattr(m, "VIDLINK_ROUTE", m._DirectRoute("vidlink", vidlink, cooldown=600, clock=clock))
    return clock


def test_encdec_and_vidlink_go_direct_while_sites_stay_on_proxy(monkeypatch):
    _enable_direct(monkeypatch)
    seen = []
    net = m._Net(_Line("proxy", seen, lambda u: _Resp()), _Line("direct", seen, lambda u: _Resp()))
    net.get(f"{m.API}/enc-vidup?text=x&stage=1")
    net.get("https://vidlink.pro/api/b/tv/abc/1/1")
    net.get("https://vidup.to/tv/1/1/1/")
    assert [line for line, _ in seen] == ["direct", "direct", "proxy"]


def test_rejected_direct_request_is_resent_via_proxy_then_cools_down(monkeypatch):
    clock = _enable_direct(monkeypatch)
    seen = []
    net = m._Net(_Line("proxy", seen, lambda u: _Resp()),
                 _Line("direct", seen, lambda u: _Resp(429, "Too Many Requests")))
    net.get(f"{m.API}/dec-vidup")
    assert [line for line, _ in seen] == ["direct", "proxy"]
    seen.clear()
    net.get(f"{m.API}/dec-vidup")
    assert [line for line, _ in seen] == ["proxy"], "冷却期内整类请求都走代理"
    clock.t += 601
    seen.clear()
    net.get(f"{m.API}/dec-vidup")
    assert seen[0][0] == "direct"


def test_dead_confirm_attempt_runs_entirely_through_proxy(monkeypatch):
    _enable_direct(monkeypatch)
    seen = []
    net = m._Net(_Line("proxy", seen, lambda u: _Resp()), _Line("direct", seen, lambda u: _Resp()),
                 allow_direct=False)
    net.get(f"{m.API}/enc-vidlink?text=1")
    assert [line for line, _ in seen] == ["proxy"]


@pytest.mark.parametrize("status,text,encdec,rejected", [
    (403, "", False, True), (429, "", True, True),
    (200, "rate limit exceeded", True, True),      # enc-dec 的限流可能以 200 + 文案出现
    (200, "rate limit exceeded", False, False),    # vidlink 200 就是正常作答
    (500, "too many requests", False, True),
    (404, "not found", False, False),
])
def test_direct_rejection_detection(monkeypatch, status, text, encdec, rejected):
    _enable_direct(monkeypatch)
    route = m.ENCDEC_ROUTE if encdec else m.VIDLINK_ROUTE
    assert m._rejects_direct(_Resp(status, text), route) is rejected


def test_shipped_config_enables_breaker_and_direct_routes():
    import os

    import yaml

    path = os.path.join(os.path.dirname(m.__file__), "config.yaml")
    cfg = yaml.safe_load(open(path, encoding="utf-8"))["tv_ids_to_links"]
    assert cfg["provider_breaker"]["enabled"] is True and cfg["provider_breaker"]["trip_after"] == 30
    assert cfg["direct"]["encdec"] is True and cfg["direct"]["vidlink"] is True
    assert cfg["provider_down_file"] == "provider_down.txt"


# ------------------------------------------------------------ videasy（可选源）

class _VResp:
    def __init__(self, status=200, text="", payload=None):
        self.status_code = status
        self.text = text
        self._payload = payload

    def json(self):
        return self._payload


class _VSession:
    """videasy 的假会话：seed → /{server}/sources → dec-videasy。replies: server -> (status, text)。"""

    def __init__(self, replies, decoded):
        self.replies = replies
        self.decoded = decoded
        self.urls = []

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        assert headers["Origin"] == "https://vidy.st"
        if "/seed?" in url:
            return _VResp(payload={"seed": "SEED"})
        server = url.split("/")[3]
        status, text = self.replies[server]
        return _VResp(status, text)

    def post(self, url, json=None, timeout=None):
        assert url.endswith("/dec-videasy") and json["seed"] == "SEED"
        return _VResp(payload={"status": 200, "result": self.decoded})


def test_videasy_returns_hls_urls_with_tv_params(monkeypatch):
    monkeypatch.setattr(m, "_TMDB_NAMES", {"1396": "Breaking Bad"})
    monkeypatch.setattr(m, "_SERIES_META", {"1396": {"year": 2008, "imdb_id": "tt0903747"}})
    monkeypatch.setattr(m, "VIDEASY_SERVERS", ("phoenix", "miami"))
    session = _VSession({"phoenix": (200, "ENC"), "miami": (500, '{"message":"No streams available"}')},
                        {"sources": [{"file": "https://sun.finalsignal.top/r6/x/master.m3u8"}],
                         "subtitles": [{"language": "English", "url": "https://s/en.vtt"}]})
    urls, title, captions = m._fetch_videasy(session, "1396", 2, 3)
    assert [(u["url"], u["provider"], u["type"]) for u in urls] == [
        ("https://sun.finalsignal.top/r6/x/master.m3u8", "videasy", "m3u8")]
    assert [c["language"] for c in captions] == ["en"]
    phoenix = next(u for u in session.urls if "/phoenix/" in u)
    assert "mediaType=tv" in phoenix and "seasonId=2" in phoenix and "episodeId=3" in phoenix
    assert "title=Breaking%2520Bad" in phoenix and "imdbId=tt0903747" in phoenix


def test_videasy_all_servers_empty_is_nosource(monkeypatch):
    monkeypatch.setattr(m, "_TMDB_NAMES", {"7": "Show"})
    monkeypatch.setattr(m, "VIDEASY_SERVERS", ("phoenix", "miami"))
    session = _VSession({"phoenix": (404, ""), "miami": (500, "phoenix api 404")}, {})
    with pytest.raises(m.NoSource):
        m._fetch_videasy(session, "7", 1, 1)


def test_videasy_without_a_title_asks_only_the_first_server(monkeypatch):
    monkeypatch.setattr(m, "_TMDB_NAMES", {})
    monkeypatch.setattr(m, "_SERIES_META", {})
    monkeypatch.setattr(m, "VIDEASY_SERVERS", ("phoenix", "miami"))
    session = _VSession({"phoenix": (404, "")}, {})
    with pytest.raises(m.NoSource):
        m._fetch_videasy(session, "8", 1, 1)
    assert not any("/miami/" in u for u in session.urls)


def test_shipped_providers_and_dead_confirm():
    import os

    import yaml

    path = os.path.join(os.path.dirname(m.__file__), "config.yaml")
    cfg = yaml.safe_load(open(path, encoding="utf-8"))["tv_ids_to_links"]
    assert "videasy" in m.PROVIDERS, "代码保留，要恢复就加回 providers"
    assert cfg["providers"] == ["vidup", "vidfast", "vidlink"], \
        "2026-10-30 起移出 videasy：23 号机首轮没有一集只有它有、也没有一集用它下成"
    assert cfg["dead_confirm"] is False and m.DEAD_CONFIRM is False, \
        "TV 全量关闭判死二次确认（用户 2026-10-30 定）"


# ------------------------------------------------------------ 哪些源熔断才阻止判死（2026-10-30）

def test_videasy_outage_does_not_block_a_dead_verdict(monkeypatch):
    """🔴 videasy 熔断、其余源都说无此集：照常判死（走二次确认），不推迟。
    TV 绝大多数集是真无源，不能让一个没有独占的源的宕机把它们全部推迟。"""
    monkeypatch.setattr(m, "DEAD_CONFIRM", True)   # 配置已关闭，这里专测打开时的行为
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    _use_breaker(monkeypatch, trip=1).record("videasy", "down")
    status, result = m.process_episode("42", 1, 1, providers=["vidup", "vidfast", "videasy", "vidlink"])
    assert (status, result) == ("dead", None)
    assert seen["order"].count("vidup") == 2, "照常走判死二次确认"
    assert "videasy" not in seen["order"], "熔断中的 videasy 不该被调用"


def test_core_provider_outage_still_defers(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [])
    b = _use_breaker(monkeypatch, trip=1)
    b.record("videasy", "down")
    b.record("vidup", "down")
    status, result = m.process_episode("42", 1, 1, providers=["vidup", "vidfast", "videasy", "vidlink"])
    assert status == "deferred" and result["providers"] == ["vidup", "videasy"]


def test_nobody_answered_is_never_dead_even_for_videasy(monkeypatch):
    """只问了 videasy 而它在熔断：没有任何一家实际答过，绝不能凭空判死。"""
    _install_fake_session(monkeypatch, PAGE, [])
    _use_breaker(monkeypatch, trip=1).record("videasy", "down")
    status, result = m.process_episode("42", 1, 1, providers=["videasy"])
    assert status == "deferred" and result["providers"] == ["videasy"]


def test_shipped_config_lists_the_dead_blocking_providers():
    import os

    import yaml

    path = os.path.join(os.path.dirname(m.__file__), "config.yaml")
    cfg = yaml.safe_load(open(path, encoding="utf-8"))["tv_ids_to_links"]["provider_breaker"]
    assert cfg["blocks_dead"] == ["vidup", "vidfast", "vidlink"]
    assert "videasy" not in m.BREAKER_BLOCKS_DEAD


# ------------------------------------------------------------ 慢 server 与 videasy 出错（2026-10-30，23 号机首轮）

def test_stream_requests_wait_for_slow_servers(monkeypatch):
    """🔴 vidup 的 Eclipse、vidfast 的 Bravo / Horizon 没片时要 13~18 秒才回 404：向 server 要流
    必须用更长的 stream_timeout，其余请求照旧用 timeout。"""
    monkeypatch.setattr(m, "STREAM_TIMEOUT", 30.0)
    monkeypatch.setattr(m, "TIMEOUT", 12)
    _install_fake_session(monkeypatch, PAGE, [{"url": "u1"}])
    fake = m.requests.Session
    seen = []
    orig = fake.post

    def post(self, url, headers=None, json=None, timeout=None):
        seen.append((url.rsplit("/", 2)[-2] if "/stream/" in url else url.rsplit("/", 1)[-1], timeout))
        return orig(self, url, headers=headers, json=json, timeout=timeout)

    monkeypatch.setattr(fake, "post", post)
    status, _ = m.process_episode("42", 1, 1, providers=["vidup"])
    assert status == "ok"
    assert ("stream", 30.0) in seen
    assert all(t == 12 for name, t in seen if name != "stream"), seen


def test_shipped_stream_timeout_covers_the_slow_servers():
    import os

    import yaml

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(m.__file__), "config.yaml"),
                              encoding="utf-8"))["tv_ids_to_links"]
    assert cfg["stream_timeout"] == 30 and cfg["timeout"] == 12
    assert m.STREAM_TIMEOUT == 30


def _videasy_raises(monkeypatch, exc):
    def broken(net, tid, season, episode):
        raise exc
    monkeypatch.setitem(m.PROVIDERS, "videasy", broken)


ALL4 = ["vidup", "vidfast", "videasy", "vidlink"]


def test_videasy_error_does_not_block_a_dead_verdict(monkeypatch):
    """🔴 23 号机首轮：videasy 入口正常、server 全部超时 / 401 / 500，旧规则下每一集都被它拖成
    瞬时错误。现在它出错等同没问到：其余源都说无此集就照常判死（走二次确认）。"""
    _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    _videasy_raises(monkeypatch, Exception("Operation timed out after 30001 milliseconds"))
    status, result = m.process_episode("42", 1, 1, providers=ALL4)
    assert (status, result) == ("dead", None)


def test_core_provider_error_still_retries(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [], page_status=503)
    status, _ = m.process_episode("42", 1, 1, providers=ALL4)
    assert status == "retry", "vidup 出错仍是瞬时错误，不能判死"


def test_only_videasy_and_it_errors_is_never_dead(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [])
    _videasy_raises(monkeypatch, m.HttpStatusError(401, "videasy-miami"))
    status, _ = m.process_episode("42", 1, 1, providers=["videasy"])
    assert status == "retry", "谁也没答过，不能凭空判死"


def test_videasy_errors_trip_its_breaker_so_it_is_skipped(monkeypatch):
    """不阻止判死的源出任何错都算一次不可用：连续出错就熔断跳过，不再每集白等。"""
    b = _use_breaker(monkeypatch, trip=3)
    for exc in (Exception("timed out"), m.HttpStatusError(401, "videasy-miami"),
                m.HttpStatusError(500, "videasy-phoenix")):
        assert m._breaker_outcome("videasy", exc) == "down"
        b.record("videasy", m._breaker_outcome("videasy", exc))
    assert b.is_down("videasy")
    assert m._breaker_outcome("vidup", Exception("timed out")) is None, "核心源的超时仍是中性"
    assert m._breaker_outcome("videasy", m.NoSource("x")) == "up"


# ------------------------------------------------------------ 超时当场重发 / server 挨个问（2026-10-30）

from test_tv_ids_to_links import _FakeResp  # noqa: E402


class _Flaky:
    """前 n 次请求抛 exc，之后正常返回。"""

    def __init__(self, n, exc):
        self.n, self.exc, self.calls = n, exc, 0

    def get(self, url, **kw):
        self.calls += 1
        if self.calls <= self.n:
            raise self.exc
        return _FakeResp(text="ok")


def test_timed_out_request_is_resent_once(monkeypatch):
    """🔴 19 号机实测：经代理的取流页 8%~10% 超时，一集十几个请求任何一个超时整次尝试作废。
    超时的请求当场再发一次。"""
    monkeypatch.setattr(m, "RETRY_STATS", m._RetryStats())
    timeout = m.requests.exceptions.Timeout("Operation timed out after 12001 milliseconds")
    proxied = _Flaky(1, timeout)
    assert m._Net(proxied).get("https://vidup.to/tv/1/1/1/").text == "ok"
    assert proxied.calls == 2
    assert m.RETRY_STATS.summary() == "超时重发 1 次、1 次成功"

    twice = _Flaky(2, Exception("Connection timed out after 12000 milliseconds"))
    with pytest.raises(Exception, match="timed out"):
        m._Net(twice).get("https://vidup.to/tv/1/1/1/")
    assert twice.calls == 2, "只重发一次"


def test_other_errors_are_not_resent():
    refused = _Flaky(1, Exception("Connection refused"))
    with pytest.raises(Exception, match="refused"):
        m._Net(refused).get("https://vidup.to/tv/1/1/1/")
    assert refused.calls == 1


class _ParallelSite:
    """vidup 同构链路的最小假会话：每个 server 要流都要等 delay 秒，replies: data -> url / 404。"""

    def __init__(self, replies, delay):
        self.replies, self.delay = replies, delay
        self.servers = [{"name": f"s{i}", "data": d} for i, d in enumerate(replies)]

    def get(self, url, headers=None, timeout=None):
        if "vidup.to/tv/" in url:
            return _FakeResp(text=PAGE)
        if url.endswith("&stage=1"):
            return _FakeResp(payload={"status": 200, "result": {"stage1": "https://x/stage1", "token": "t"}})
        if url.endswith("&stage=2"):
            return _FakeResp(payload={"status": 200, "result": {
                "servers": "https://x/servers", "stream": "https://x/stream", "token": "t"}})
        raise AssertionError(url)

    def post(self, url, headers=None, json=None, timeout=None):
        if url == "https://x/stage1":
            return _FakeResp(text="S1TEXT")
        if url == "https://x/servers":
            return _FakeResp(text="enc-servers")
        if url.endswith("/dec-vidup"):
            if json["text"] == "enc-servers":
                return _FakeResp(payload={"status": 200, "result": self.servers})
            return _FakeResp(payload={"status": 200, "result": {"url": self.replies[json["text"]]}})
        if url.startswith("https://x/stream/"):
            import time as _t
            _t.sleep(self.delay)
            data = url.rsplit("/", 1)[1]
            return _FakeResp(text=data, status=404 if self.replies[data] == 404 else 200)
        raise AssertionError(url)


def test_servers_are_asked_one_by_one_in_order():
    """🔴 必须挨个问：19 号机实测同一集的各 server 同时问，stream 接口一律回 HTTP 500。"""
    import threading as _th

    site = _ParallelSite({"d0": "u0", "d1": 404, "d2": "u2", "d3": "u0", "d4": "u4"}, delay=0.05)
    inflight, peak, lock = [0], [0], _th.Lock()
    orig = site.post

    def post(url, **kw):
        if not url.startswith("https://x/stream/"):
            return orig(url, **kw)
        with lock:
            inflight[0] += 1
            peak[0] = max(peak[0], inflight[0])
        try:
            return orig(url, **kw)
        finally:
            with lock:
                inflight[0] -= 1

    site.post = post
    urls, _, _ = m._fetch_vidup(site, "1", 1, 1)
    assert peak[0] == 1, "同一集同时只能有一个 stream 请求"
    assert [u["url"] for u in urls] == ["u0", "u2", "u4"]


def test_concurrent_servers_all_404_is_still_nosource():
    site = _ParallelSite({f"d{i}": 404 for i in range(5)}, delay=0.05)
    with pytest.raises(m.NoSource, match="all 5 vidup servers returned 404"):
        m._fetch_vidup(site, "1", 1, 1)
