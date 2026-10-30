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


def test_videasy_is_enabled_before_vidlink():
    import os

    import yaml

    path = os.path.join(os.path.dirname(m.__file__), "config.yaml")
    cfg = yaml.safe_load(open(path, encoding="utf-8"))["tv_ids_to_links"]
    assert "videasy" in m.PROVIDERS
    assert cfg["providers"] == ["vidup", "vidfast", "videasy", "vidlink"], \
        "videasy 长期启用、排在 vidlink 前（画质优先）"
