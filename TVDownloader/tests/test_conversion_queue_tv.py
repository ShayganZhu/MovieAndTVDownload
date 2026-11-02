"""转封装队列（2026-11-01）：重启接着转已下好的 ts、停机不等排队的集、队列封顶、检测放独立进程。"""

import json
import os
import signal
import threading
import time
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool

import pytest

import download_tv as d
from test_overlay_tv import _pipeline_env, _scan, hls


def _ts_job(temp, key="90_S01E01", size=10, write=True, recorded=None):
    tid, rest = key.split("_S")
    season, episode = rest.split("E")
    final_ts = str(temp / f"temp_{key}.ts")
    if write:
        with open(final_ts, "wb") as fh:
            fh.write(b"x" * size)
    node = hls(tid, int(episode))
    return {
        "tmdbId": tid, "season": int(season), "episode": int(episode), "normalized_id": key,
        "title": "T", "year": 2010, "url": node["url"], "node": node,
        "provider": node["provider"], "node_type": node["type"],
        "entry": {"tmdbId": tid, "season": int(season), "episode": int(episode), "title": "T",
                  "urls": [node]},
        "final_ts": final_ts, "temp_mp4": str(temp / f"temp_{key}.mp4"),
        "cleanup_paths": [final_ts], "bitrate_kbps": 3000, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
        "ts_size": size if recorded is None else recorded,
    }


@pytest.fixture
def temp(tmp_path, monkeypatch):
    folder = tmp_path / "temp"
    folder.mkdir()
    monkeypatch.setattr(d, "TEMP_DIR", str(folder))
    return folder


def _write_journal(jobs):
    with open(d.CONVERSION_JOURNAL, "w", encoding="utf-8") as fh:
        for job in jobs:
            fh.write((job if isinstance(job, str) else json.dumps(job)) + "\n")


# ------------------------------------------------------------ 记录与恢复

def test_journal_records_the_size_of_the_finished_ts(temp):
    job = _ts_job(temp, size=123)
    job.pop("ts_size")
    d.journal_conversion_job(job)
    assert json.loads(open(d.CONVERSION_JOURNAL, encoding="utf-8").read())["ts_size"] == 123


@pytest.mark.parametrize("job", [{"normalized_id": "1_S01E01"}, {"final_ts": None}, {"final_ts": "/none"}])
def test_journal_ignores_jobs_without_a_ts(temp, job):
    d.journal_conversion_job(job)
    assert not os.path.exists(d.CONVERSION_JOURNAL)


def test_only_intact_ts_files_are_resumable(temp, tmp_path):
    intact = _ts_job(temp, "1_S01E01")
    gone = _ts_job(temp, "2_S01E01", write=False)
    partial = _ts_job(temp, "3_S01E01", size=5, recorded=10)      # 后来又重下到一半
    elsewhere = _ts_job(temp, "4_S01E01")
    other = tmp_path / "other.ts"
    other.write_bytes(b"x" * 10)
    elsewhere["final_ts"] = str(other)
    no_entry = _ts_job(temp, "5_S01E01")
    no_entry["entry"] = None
    _write_journal([intact, gone, partial, elsewhere, no_entry, "{坏行", "[]"])

    assert [job["normalized_id"] for job in d.load_resumable_conversions()] == ["1_S01E01"]


def test_last_journal_row_of_an_episode_wins(temp):
    old = _ts_job(temp, size=10, recorded=99)
    new = _ts_job(temp, size=10)
    _write_journal([new, old])
    assert d.load_resumable_conversions() == []
    _write_journal([old, new])
    assert len(d.load_resumable_conversions()) == 1


def test_no_journal_means_nothing_to_resume(temp):
    assert d.load_resumable_conversions() == []


def test_clean_temp_directory_spares_resumable_ts(temp):
    job = _ts_job(temp, "1_S01E01")
    stale = temp / "temp_2_S01E01.ts"
    stale.write_bytes(b"x")
    half = temp / "temp_1_S01E01.mp4"
    half.write_bytes(b"x")
    d.clean_temp_directory(keep=[job["final_ts"]])
    assert os.listdir(temp) == ["temp_1_S01E01.ts"]


def test_rewrite_keeps_only_given_jobs(temp):
    job = _ts_job(temp, "1_S01E01")
    _write_journal([job, _ts_job(temp, "2_S01E01")])
    d.rewrite_conversion_journal([job])
    assert [json.loads(x)["normalized_id"] for x in open(d.CONVERSION_JOURNAL, encoding="utf-8")] == ["1_S01E01"]


# ------------------------------------------------------------ 中断：排队的集原样留着

def test_queued_conversion_leaves_ts_for_next_run_on_interrupt(temp, monkeypatch):
    job = _ts_job(temp)
    monkeypatch.setattr(d, "processing_ids", {job["normalized_id"]})
    monkeypatch.setattr(d, "convert_ts_to_mp4", lambda a, b: pytest.fail("中断后不该再开转封装"))
    d.interrupted.set()
    try:
        key, ok, info = d.finalize_one_entry(job, set())
    finally:
        d.interrupted.clear()
    assert (key, ok, info["error"]) == (job["normalized_id"], False, d._INTERRUPTED_ERROR)
    assert os.path.exists(job["final_ts"]), "ts 要留给下次启动接着转"
    assert d.processing_ids == set()


# ------------------------------------------------------------ 端到端：重启接着转

def test_run_resumes_conversion_instead_of_downloading_again(tmp_path, monkeypatch, capsys):
    entries = [
        {"tmdbId": "90", "season": 1, "episode": 1, "title": "x", "urls": [hls("90", 1)]},
        {"tmdbId": "90", "season": 1, "episode": 2, "title": "x", "urls": [hls("90", 2)]},
    ]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan(), []))
    job = _ts_job(tmp_path / "temp", "90_S01E01")
    stale = tmp_path / "temp" / "temp_90_S09E09.ts"
    stale.write_bytes(b"x")
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    _write_journal([job])

    d._run_pipeline()

    assert [key for key, _ in downloaded] == ["90_S01E02"], "下好的那集不重下"
    assert sorted(uploads) == ["90_S01E01", "90_S01E02"]
    assert not os.path.exists(job["final_ts"]) and not stale.exists()
    assert "发现 1 集已下完、还没转封装" in capsys.readouterr().out


def test_run_drops_resumable_ts_of_episode_that_already_has_a_verdict(tmp_path, monkeypatch):
    entries = [{"tmdbId": "91", "season": 1, "episode": 2, "title": "x", "urls": [hls("91", 2)]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan(), []))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"91_S01E01"})
    job = _ts_job(tmp_path / "temp", "91_S01E01")
    _write_journal([job])

    d._run_pipeline()

    assert "91_S01E01" not in uploads and not os.path.exists(job["final_ts"])
    assert d.load_resumable_conversions() == []


# ------------------------------------------------------------ 假链接与到期重取（端到端）

def test_run_drops_fake_nodes_and_skips_fake_only_rows(tmp_path, monkeypatch, capsys):
    fake = "https://a.b.workers.dev/video.m3u8?q=1"
    fresh = int(time.time())
    monkeypatch.setattr(d, "FAKE_LINK_HOSTS", ("workers.dev",))
    entries = [
        {"tmdbId": "95", "season": 1, "episode": 1, "title": "x",
         "urls": [{"url": fake, "provider": "vidup", "type": "m3u8"}, hls("95", 1)]},
        {"tmdbId": "95", "season": 1, "episode": 2, "title": "x",
         "urls": [{"url": fake, "provider": "vidup", "type": "m3u8"}]},
        # 同集：较新的一行只有假链接，不能顶掉较早的真链接
        {"tmdbId": "95", "season": 1, "episode": 3, "title": "x", "fetched_at": fresh, "urls": [hls("95", 3)]},
        {"tmdbId": "95", "season": 1, "episode": 3, "title": "x", "fetched_at": fresh + 1,
         "urls": [{"url": fake, "provider": "vidup", "type": "m3u8"}]},
    ]
    downloaded, _uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan(), []))

    d._run_pipeline()

    assert sorted(key for key, _ in downloaded) == ["95_S01E01", "95_S01E03"]
    assert all("workers.dev" not in url for _, url in downloaded)
    assert "2 行只有假链接" in capsys.readouterr().out


def test_run_resumes_assets_only_pending_at_startup(tmp_path, monkeypatch):
    downloaded, _uploads = _pipeline_env(tmp_path, monkeypatch, [], lambda url: ())
    monkeypatch.setattr(d, "S3_ENABLED", True)
    called = []
    monkeypatch.setattr(d, "resume_assets_only_pending", lambda: called.append(True))
    d._run_pipeline()
    assert called == [True] and downloaded == []


def test_run_survives_assets_only_resume_failure(tmp_path, monkeypatch, capsys):
    _pipeline_env(tmp_path, monkeypatch, [], lambda url: ())
    monkeypatch.setattr(d, "S3_ENABLED", True)

    def boom():
        raise RuntimeError("坏了")

    monkeypatch.setattr(d, "resume_assets_only_pending", boom)
    d._run_pipeline()
    assert "启动补传旁车资产失败" in capsys.readouterr().out


def test_run_refetches_due_episode_with_vidlink_and_downloads_new_link(tmp_path, monkeypatch):
    """🔴 端到端：bcdnxw 的集过了刷新点，当次运行只问 vidlink 换链接，再用新链接下载。"""
    import queue

    import pipeline as p

    signed = int(time.time()) - 50 * 3600
    bad = {"url": f"https://bcdnxw.hakunaymatata.com/r/a.mp4?sign=x&t={signed}", "provider": "vidlink", "type": "mp4"}
    good = {"url": f"https://hcdn3.hakunaymatata.com/r/a.mp4?sign=y&t={int(time.time())}",
            "provider": "vidlink", "type": "mp4"}
    entries = [{"tmdbId": "96", "season": 1, "episode": 1, "title": "x", "urls": [bad]},
               {"tmdbId": "96", "season": 1, "episode": 2, "title": "x", "urls": [bad]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda p_: (_scan(), []))
    monkeypatch.setattr(d, "BROKEN_VIDLINK_HOSTS", {"bcdnxw.hakunaymatata.com"})
    monkeypatch.setattr(d, "VIDLINK_CACHE_ROLL_SECONDS", 49 * 3600)
    monkeypatch.setattr(d, "DUE_REFETCH_RETRY_SECONDS", 3600.0)
    monkeypatch.setattr(d, "DUE_REFETCH_MAX_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    asked = []

    class Fetcher:
        append_result = staticmethod(lambda path, result: None)

        @staticmethod
        def process_episode(tid, season, episode, providers=None, stop_event=None):
            asked.append((episode, providers))
            if episode == 1:
                return "ok", {"tmdbId": tid, "season": season, "episode": episode,
                              "urls": [good], "fetched_at": int(time.time())}
            return "dead", None

    monkeypatch.setattr(p, "fetcher", Fetcher)
    stop = threading.Event()
    pool = p.AsyncRefetcher(2, stop, providers=["vidlink"], report_misses=True)
    pool.start()
    monkeypatch.setattr(d, "due_refetch_hook", pool)
    q = queue.Queue()
    q.put(p._SENTINEL)
    monkeypatch.setattr(d, "ListEntrySource", lambda items: p.QueueEntrySource(
        q, backlog=items, refetcher=p._RefetchPools(pool),
        is_busy=lambda key: key in d.processing_ids))

    try:
        d._run_pipeline()
    finally:
        stop.set()

    assert sorted(asked) == [(1, ["vidlink"]), (2, ["vidlink"])], "只问 vidlink"
    assert downloaded == [("96_S01E01", good["url"])], "没换到链接的集不拿死链接去下"
    assert "96_S01E01" in uploads and "96_S01E02" not in uploads


# ------------------------------------------------------------ 队列封顶

def test_downloads_pause_while_conversion_queue_is_full_and_everything_still_finishes(
        tmp_path, monkeypatch, capsys):
    entries = [{"tmdbId": "92", "season": 1, "episode": i, "title": "x", "urls": [hls("92", i)]}
               for i in range(1, 9)]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan(), []))
    monkeypatch.setattr(d, "MAX_PENDING_CONVERSIONS", 2)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 2)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)
    gate = threading.Event()
    real_finalize = d.finalize_one_entry

    def slow_finalize(job, processed):
        gate.wait(20)
        return real_finalize(job, processed)

    monkeypatch.setattr(d, "finalize_one_entry", slow_finalize)
    runner = threading.Thread(target=d._run_pipeline, daemon=True)
    runner.start()
    time.sleep(1)
    assert len(downloaded) <= 2 + 2, "队列满了不再派新下载（最多再加已投递的那批）"
    gate.set()
    runner.join(30)

    assert not runner.is_alive()
    assert sorted(uploads) == [f"92_S01E0{i}" for i in range(1, 9)]
    out = capsys.readouterr().out
    assert "[转封装队列] 已有 2 集，暂停派发新下载" in out and "恢复派发新下载" in out


def test_shipped_conversion_settings():
    import yaml

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]
    assert cfg["max_pending_conversions"] == 200
    assert cfg["conversion_journal"] == "conversion_jobs.jsonl"
    assert cfg["hardsub_check"]["ocr_processes"] == cfg["convert_workers"]


# ------------------------------------------------------------ 检测放独立进程

class _FakePool:
    def __init__(self, raise_on_submit=None, result_exc=None):
        self.raise_on_submit, self.result_exc = raise_on_submit, result_exc

    def submit(self, fn, *args):
        if self.raise_on_submit:
            raise self.raise_on_submit
        future = Future()
        if self.result_exc:
            future.set_exception(self.result_exc)
        else:
            future.set_result(("pool", args))
        return future


@pytest.fixture
def pool_on(monkeypatch):
    monkeypatch.setattr(d, "HARDSUB_OCR_PROCESSES", 2)
    monkeypatch.setattr(d, "_overlay_pool_instance", None)


def test_detection_runs_inline_when_processes_are_off(monkeypatch):
    monkeypatch.setattr(d, "detect_overlays", lambda p: ("inline", p))
    assert d._detect_overlays_isolated("x.mp4") == ("inline", "x.mp4")


def test_detection_goes_through_the_pool(pool_on, monkeypatch):
    monkeypatch.setattr(d, "_overlay_pool_instance", _FakePool())
    assert d._detect_overlays_isolated("x.mp4") == ("pool", ("x.mp4",))


@pytest.mark.parametrize("pool", [
    _FakePool(raise_on_submit=BrokenProcessPool("死了")),
    _FakePool(result_exc=BrokenProcessPool("死了")),
])
def test_broken_pool_falls_back_inline_and_is_rebuilt_next_time(pool_on, monkeypatch, pool, capsys):
    monkeypatch.setattr(d, "_overlay_pool_instance", pool)
    monkeypatch.setattr(d, "detect_overlays", lambda p: ("inline", p))
    assert d._detect_overlays_isolated("x.mp4") == ("inline", "x.mp4")
    assert d._overlay_pool_instance is None
    assert "检测子进程异常退出" in capsys.readouterr().out


def test_detection_error_in_worker_is_released_as_unknown(pool_on, monkeypatch, tmp_path):
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "_overlay_pool_instance", _FakePool(result_exc=ValueError("读不了")))
    reasons, scan = d.inspect_overlays(
        {"tmdbId": "1", "season": 1, "episode": 1}, "T", str(tmp_path / "x.mp4"))
    assert reasons == [] and scan["verdict"] == "unknown" and "读不了" in scan["error"]


@pytest.mark.parametrize("keyframe, expected", [
    (True, ["-threads", "1", "-skip_frame", "nokey", "-noaccurate_seek", "-ss", "12.500"]),
    (False, ["-threads", "1", "-ss", "12.500"]),
])
def test_grab_frame_seek_arguments(monkeypatch, keyframe, expected):
    pytest.importorskip("cv2")
    monkeypatch.setattr(d, "HARDSUB_KEYFRAME_SEEK", keyframe)
    seen = []

    def fake_run(command, **kwargs):
        seen.append(command)
        return d.subprocess.CompletedProcess(command, 1, b"", b"")

    monkeypatch.setattr(d.subprocess, "run", fake_run)
    assert d._grab_frame("x.mp4", 12.5) is None
    assert seen[0][3:3 + len(expected)] == expected


def test_real_worker_process_starts_with_sigint_ignored(pool_on):
    pool = d._overlay_pool()
    try:
        assert pool.submit(os.getpid).result(timeout=120) != os.getpid()
        assert pool.submit(signal.getsignal, signal.SIGINT).result(timeout=30) == signal.SIG_IGN
        assert d._overlay_pool() is pool, "进程池只建一个"
    finally:
        pool.shutdown(wait=True)
        d._overlay_pool_instance = None
