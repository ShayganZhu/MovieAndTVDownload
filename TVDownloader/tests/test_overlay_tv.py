"""硬字幕/水印检测接入 TV 下载流程（电影侧 2026-10-22～24 移植，2026-10-30）的离线用例。

业务口径（用户 2026-10-30 定）：**每一集都检测、检出后只处理这一集**——
带硬字幕或水印的版本不要；这一集还有没试过的节点就换节点重下，所有版本都检出
才整集判死；同剧别的集不受影响（误报不扩散）。

重点锁死几件错了会丢集 / 白下 / 误判死 / 误伤别的集的事：
  - 还有别的版本时绝不能写判死台账，也不能登记已完成（否则换节点重下被拦）；
  - 已检出的版本跨运行、跨重取都不能再下这一集，但同剧别的集照下；
  - 所有版本都检出才判死，判死记录带 season/episode，--retry-dead 永不放回；
  - 最后几集检出后的重下仍在本轮之内（来源取空也要等转封装跑完）。
"""

import json
import os

import pytest

import download_tv as d


FINAL = "sun.finalsignal.top"
HAKUNA = "bcdn.hakunaymatata.com"


def mp4(tid, ep=1, name="a", host=HAKUNA):
    return {"url": f"https://{host}/resource/{tid}e{ep}{name}.mp4?sign=s1&t=1",
            "provider": "vidlink", "type": "mp4"}


def hls(tid, ep=1, provider="vidup", host=FINAL):
    return {"url": f"https://{host}/r6/{tid}-{ep}-{provider}/master.m3u8",
            "provider": provider, "type": "m3u8"}


def node_of(item):
    return d._normalize_url_entry(item)


def ident(tid, season=1, episode=1):
    return {"tmdbId": str(tid), "season": season, "episode": episode}


def _read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _scan(*reasons, near_miss=()):
    return {
        "verdict": "+".join(reasons) or "clean", "reasons": list(reasons),
        "sampled": 60, "hits": 24, "hit_ratio": 0.4, "distinct_texts": 20,
        "examples": ["weneedtotalk"],
        "watermark_texts": ([{"text": "piratesite", "presence": 1.0, "ring_std": 40.0}]
                            if "watermark" in reasons else []),
        "watermark_logo": None, "static_texts": [], "roaming_texts": [],
        "logo_peak_presence": 0.1, "near_miss": list(near_miss),
    }


# ------------------------------------------------------------ 版本标识

def test_mp4_versions_of_one_provider_are_one_version():
    """vidlink 给的几个 mp4（不同分辨率）是同一片源：算一个版本。"""
    a = node_of(mp4("7", name="1080"))
    b = node_of(mp4("7", name="720", host="bcdn4.hakunaymatata.com"))
    assert d.overlay_node_key(a) == d.overlay_node_key(b) == "vidlink|mp4"


def test_hls_version_is_provider_plus_host():
    a = node_of(hls("7"))
    b = node_of(hls("7", host="moon.goldenkite.top"))
    c = node_of(hls("7", provider="vidfast"))
    assert len({d.overlay_node_key(n) for n in (a, b, c)}) == 3


# ------------------------------------------------------------ 只处理这一集

def test_rejected_version_is_stripped_for_that_episode_only(monkeypatch):
    """🔴 一集检出的版本只从**这一集**去掉；同剧别的集的同一个版本照下（口径：只处理这一集）。"""
    d.record_overlay_node_reject(ident(70, 1, 2), "T", node_of(hls("70", 2)), _scan("hardsub"))
    ep2 = {"tmdbId": "70", "season": 1, "episode": 2, "urls": [hls("70", 2), mp4("70", 2)]}
    ep3 = {"tmdbId": "70", "season": 1, "episode": 3, "urls": [hls("70", 3), mp4("70", 3)]}
    stripped, removed = d.strip_rejected_nodes(ep2)
    assert removed == 1 and stripped["urls"] == [mp4("70", 2)]
    assert d.strip_rejected_nodes(ep3) == (ep3, 0), "同剧别的集不能受影响"

    # 跨运行：重读台账后仍然生效。
    monkeypatch.setattr(d, "_overlay_node_rejects", None)
    assert d.strip_rejected_nodes(ep2)[1] == 1
    rows = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    assert [(r["tmdbId"], r["season"], r["episode"], r["reason"]) for r in rows] == [
        ("70", 1, 2, "hardsub")]


def test_strip_returns_same_object_when_nothing_rejected():
    entry = {"tmdbId": "71", "season": 1, "episode": 1, "urls": [hls("71")]}
    assert d.strip_rejected_nodes(entry) == (entry, 0)


# ------------------------------------------------------------ finalize_one_entry

@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv")
    monkeypatch.setattr(d, "processing_ids", set())
    return tmp_path


def _job(sandbox, monkeypatch, tid, node, entry, season=1, episode=1):
    key = d.episode_key(tid, season, episode)
    temp_mp4 = os.path.join(str(sandbox), "temp", f"temp_{key}.mp4")
    os.makedirs(os.path.dirname(temp_mp4), exist_ok=True)

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"m" * 4096)
        return True

    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "kill")
    d.processing_ids.add(key)
    return {
        "tmdbId": str(tid), "season": season, "episode": episode, "normalized_id": key,
        "title": "Hard Sub", "year": 2010, "url": node["url"], "node": node_of(node),
        "provider": node["provider"], "node_type": node["type"], "entry": entry,
        "final_ts": os.path.join(str(sandbox), "temp", f"temp_{key}.ts"),
        "temp_mp4": temp_mp4, "cleanup_paths": [],
        "bitrate_kbps": 3000, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }


def test_version_with_hardsub_is_dropped_but_episode_is_not_dead(sandbox, monkeypatch):
    """🔴 还有别的版本：删掉这个版本、交回剩余节点，绝不判死、也不登记已完成。"""
    entry = {"tmdbId": "80", "season": 1, "episode": 4, "urls": [hls("80", 4), mp4("80", 4)]}
    job = _job(sandbox, monkeypatch, "80", hls("80", 4), entry, episode=4)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    processed = set()

    key, ok, info = d.finalize_one_entry(job, processed)

    assert (key, ok) == ("80_S01E04", False)
    assert info["overlay_reject"] == "hardsub"
    assert info["overlay_retry_entry"]["urls"] == [mp4("80", 4)]
    assert "换剩余 1 个节点重下" in info["error"]
    assert not os.path.exists(job["temp_mp4"]), "检出的版本必须就地删掉"
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []
    assert processed == set() and d.processing_ids == set()
    scan = _read_jsonl(d.HARDSUB_SCAN_LOG)[0]
    assert (scan["tmdbId"], scan["season"], scan["episode"]) == ("80", 1, 4)
    assert scan["node_key"] == "vidup|m3u8|" + FINAL


def test_last_version_with_overlay_kills_the_episode_with_all_reasons(sandbox, monkeypatch):
    entry = {"tmdbId": "81", "season": 2, "episode": 1, "urls": [hls("81"), mp4("81")]}
    d.record_overlay_node_reject(ident(81, 2, 1), "T", node_of(hls("81")), _scan("watermark"))
    job = _job(sandbox, monkeypatch, "81", mp4("81"), entry, season=2, episode=1)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    processed = set()

    _, ok, info = d.finalize_one_entry(job, processed)

    assert ok is False and "overlay_retry_entry" not in info
    dead = _read_jsonl(d.DOWNLOAD_DEAD_LOG)
    assert [(r["tmdbId"], r["season"], r["episode"], r["reason_class"]) for r in dead] == [
        ("81", 2, 1, "hardsub")]
    assert set(dead[0]["reasons"]) == {"hardsub", "watermark"}
    assert d.load_dead_keys() == {"81_S02E01"}
    assert processed == {"81_S02E01"}, "判死的集本次运行也不再重下"


def test_feature_off_kills_on_first_detection(sandbox, monkeypatch):
    monkeypatch.setattr(d, "OVERLAY_RETRY_OTHER_NODES", False)
    entry = {"tmdbId": "82", "season": 1, "episode": 1, "urls": [hls("82"), mp4("82")]}
    job = _job(sandbox, monkeypatch, "82", hls("82"), entry)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("watermark"), []))
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is False and "overlay_retry_entry" not in info
    assert [r["reason_class"] for r in _read_jsonl(d.DOWNLOAD_DEAD_LOG)] == ["watermark"]


def test_record_mode_still_delivers(sandbox, monkeypatch):
    entry = {"tmdbId": "83", "season": 1, "episode": 1, "urls": [hls("83")]}
    job = _job(sandbox, monkeypatch, "83", hls("83"), entry)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "record")
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is True and os.path.exists(info["final_path"])
    assert _read_jsonl(d.HARDSUB_SCAN_LOG)[0]["action"] == "record"
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []


def test_detection_error_lets_the_episode_through(sandbox, monkeypatch):
    """检测故障不能变成丢集：按没有放行，结论记 unknown。"""
    entry = {"tmdbId": "84", "season": 1, "episode": 1, "urls": [hls("84")]}
    job = _job(sandbox, monkeypatch, "84", hls("84"), entry)

    def boom(path):
        raise RuntimeError("onnxruntime exploded")

    monkeypatch.setattr(d, "detect_overlays", boom)
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is True
    assert _read_jsonl(d.HARDSUB_SCAN_LOG)[0]["verdict"] == "unknown"


def test_check_disabled_skips_detection(sandbox, monkeypatch):
    entry = {"tmdbId": "85", "season": 1, "episode": 1, "urls": [hls("85")]}
    job = _job(sandbox, monkeypatch, "85", hls("85"), entry)
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "detect_overlays", lambda p: pytest.fail("关闭时不该检测"))
    _, ok, _ = d.finalize_one_entry(job, set())
    assert ok is True and _read_jsonl(d.HARDSUB_SCAN_LOG) == []


def test_near_miss_episode_is_delivered_with_evidence(sandbox, monkeypatch):
    """临门槛：照常交付，截图存到 near_miss/{集 key}/，success 记下版本。"""
    import numpy as np

    entry = {"tmdbId": "86", "season": 3, "episode": 7, "urls": [hls("86", 7)]}
    job = _job(sandbox, monkeypatch, "86", hls("86", 7), entry, season=3, episode=7)
    frame = np.zeros((10, 10, 3), np.uint8)
    monkeypatch.setattr(d, "detect_overlays",
                        lambda p: (_scan(near_miss=["hardsub"]), [frame, frame]))
    saved = []
    monkeypatch.setattr(d, "_save_overlay_evidence",
                        lambda key, frames, near_miss=False, version=None:
                        saved.append((key, len(frames), near_miss)) or "/ev/" + key)
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is True
    assert saved == [("86_S03E07", 2, True)]
    assert info["node_key"] == "vidup|m3u8|" + FINAL
    assert _read_jsonl(d.HARDSUB_SCAN_LOG)[0]["evidence_dir"] == "/ev/86_S03E07"


# ------------------------------------------------------------ NodeRetrySource

class Src:
    def __init__(self, items=(), closed=False):
        self.items = list(items)
        self.closed = closed

    def poll(self):
        if self.items:
            return "item", self.items.pop(0)
        return ("done", None) if self.closed else ("wait", None)


def test_requeued_entries_go_first():
    rs = d.NodeRetrySource()
    rs.set_source(Src([{"tmdbId": "1", "season": 1, "episode": 1, "urls": [hls("1")]}]))
    rs.requeue({"tmdbId": "2", "season": 1, "episode": 1, "urls": [hls("2")]})
    assert rs.poll()[1]["tmdbId"] == "2"
    assert rs.poll()[1]["tmdbId"] == "1"


def test_source_items_are_filtered_and_fully_rejected_episodes_are_killed():
    d.record_overlay_node_reject(ident(3), "T", node_of(hls("3")), _scan("hardsub"))
    killed = []
    rs = d.NodeRetrySource(on_all_rejected=killed.append)
    rs.set_source(Src([
        {"tmdbId": "3", "season": 1, "episode": 1, "urls": [hls("3")]},
        {"tmdbId": "3", "season": 1, "episode": 2, "urls": [hls("3", 2)]},
    ], closed=True))
    state, entry = rs.poll()
    assert (state, entry["episode"]) == ("item", 2), "全部版本已检出的那一集跳过，同剧下一集照出"
    assert [k["episode"] for k in killed] == [1]
    assert rs.poll() == ("done", None)


def test_waits_for_work_in_flight_before_finishing_the_round():
    busy = [True]
    rs = d.NodeRetrySource(work_in_flight=lambda: busy[0])
    rs.set_source(Src(closed=True))
    assert rs.poll() == ("wait", None)
    busy[0] = False
    assert rs.poll() == ("done", None)


# ------------------------------------------------------------ 主循环端到端

def _pipeline_env(tmp_path, monkeypatch, entries, verdict_of):
    """跑真实的 _run_pipeline：下载取条目的第一个节点，检测结论由 verdict_of(url) 决定。"""
    for name, filename in (
        ("FAILED_LOG", "failed.jsonl"), ("SUCCESS_LOG", "success.jsonl"),
        ("DOWNLOAD_OK_LOG", "download_ok.jsonl"),
        ("DOWNLOAD_FAIL_LOG", "download_fail.jsonl"),
        ("UPLOAD_PENDING_LOG", "upload_pending.jsonl"),
        ("MAIN_LOCK_FILE", "main.lock"),
    ):
        monkeypatch.setattr(d, name, str(tmp_path / filename))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv")
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "kill")
    monkeypatch.setattr(d, "processing_ids", set())
    os.makedirs(tmp_path / "temp", exist_ok=True)
    inp = tmp_path / "in.jsonl"
    inp.write_text("".join(json.dumps(e) + "\n" for e in entries), encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(inp))

    downloaded = []
    used = {}

    def fake_process(entry, ids):
        key = d.record_episode_key(entry)
        with d.processing_lock:
            d.processing_ids.add(key)
        node = d._normalize_url_entry(entry["urls"][0])
        downloaded.append((key, node["url"]))
        used[key] = node["url"]
        return key, True, {
            "tmdbId": str(entry["tmdbId"]), "season": entry["season"],
            "episode": entry["episode"], "normalized_id": key,
            "title": entry.get("title", key), "year": 2010, "url": node["url"],
            "node": node, "provider": node["provider"], "node_type": node["type"],
            "entry": entry,
            "final_ts": os.path.join(str(tmp_path / "temp"), f"temp_{key}.ts"),
            "temp_mp4": os.path.join(str(tmp_path / "temp"), f"temp_{key}.mp4"),
            "cleanup_paths": [], "bitrate_kbps": 3000, "resolution": "1920x1080",
            "missing_segment_count": 0, "missing_segment_indices": [],
        }

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"m" * 1024)
        return True

    def fake_detect(path):
        key = os.path.basename(path)[len("temp_"):-len(".mp4")]
        return _scan(*verdict_of(used[key])), []

    uploads = []
    monkeypatch.setattr(d, "process_one_entry", fake_process)
    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    monkeypatch.setattr(d, "detect_overlays", fake_detect)
    monkeypatch.setattr(
        d, "upload_one_entry",
        lambda info: uploads.append(d.record_episode_key(info)) or (
            d.record_episode_key(info), True, info),
    )
    return downloaded, uploads


def test_pipeline_retries_other_node_and_uploads_clean_version(tmp_path, monkeypatch, capsys):
    """🔴 端到端：第 1 集首个版本带硬字幕 → 当轮换节点重下、上传干净版本；
    同剧第 2 集的同一个版本是干净的，照常上传（只处理那一集）。"""
    entries = [
        {"tmdbId": "90", "season": 1, "episode": 1, "title": "x", "urls": [hls("90", 1), mp4("90", 1)]},
        {"tmdbId": "90", "season": 1, "episode": 2, "title": "x", "urls": [hls("90", 2), mp4("90", 2)]},
    ]
    downloaded, uploads = _pipeline_env(
        tmp_path, monkeypatch, entries,
        lambda url: ("hardsub",) if url == hls("90", 1)["url"] else (),
    )

    d._run_pipeline()

    assert ("90_S01E01", mp4("90", 1)["url"]) in downloaded, "必须当轮换节点重下"
    assert sorted(uploads) == ["90_S01E01", "90_S01E02"]
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []
    stages = [(d.record_episode_key(r), r["stage"]) for r in _read_jsonl(d.FAILED_LOG)]
    assert stages == [("90_S01E01", "hardsub_retry")]
    assert "硬字幕换节点" in capsys.readouterr().out


def test_episode_whose_every_version_was_rejected_earlier_is_killed_without_download(
        tmp_path, monkeypatch):
    d.record_overlay_node_reject(ident(91), "T", node_of(hls("91")), _scan("hardsub"))
    d.record_overlay_node_reject(ident(91), "T", node_of(mp4("91")), _scan("watermark"))
    entries = [{"tmdbId": "91", "season": 1, "episode": 1, "title": "x",
                "urls": [hls("91"), mp4("91")]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert downloaded == [] and uploads == []
    assert d.load_dead_keys() == {"91_S01E01"}
    dead = _read_jsonl(d.DOWNLOAD_DEAD_LOG)[0]
    assert (dead["season"], dead["episode"], dead["reason_class"]) == (1, 1, "hardsub")


# ------------------------------------------------------------ 判死账本

def test_retry_dead_never_revives_overlay_records():
    for reason in ("hardsub", "watermark", "truncated"):
        assert d.dead_record_passes_now({"reason_class": reason}) is False
    # 画质判死的老口径不变：没有依据仍按"放回"处理。
    assert d.dead_record_passes_now({"reason_class": "quality"}) is True


def test_dead_breakdown_splits_reasons():
    with open(d.DOWNLOAD_DEAD_LOG, "w", encoding="utf-8") as fh:
        for ep, kind in ((1, None), (2, "hardsub"), (3, "watermark"), (4, "truncated")):
            fh.write(json.dumps({"tmdbId": "5", "season": 1, "episode": ep,
                                 **({"reason_class": kind} if kind else {})}) + "\n")
    assert d.dead_breakdown(d.load_dead_keys()) == "画质 1 / 硬字幕 1 / 水印 1 / 时长不符 1"


# ------------------------------------------------------------ 启动预检

def test_preflight_rejects_bad_action_and_missing_ocr(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    monkeypatch.setattr(d, "release_main_lock", lambda: None)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "delete")
    with pytest.raises(SystemExit):
        d.preflight_check_hardsub()
    assert "action 只能是 kill 或 record" in capsys.readouterr().out

    monkeypatch.setattr(d, "HARDSUB_ACTION", "kill")
    real_import = d.importlib.import_module

    def no_ocr(name, *a, **kw):
        if name == "rapidocr_onnxruntime":
            raise ImportError("No module named 'rapidocr_onnxruntime'")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(d.importlib, "import_module", no_ocr)
    with pytest.raises(SystemExit):
        d.preflight_check_hardsub()
    assert "依赖缺失" in capsys.readouterr().out
    assert _read_jsonl(d.FAILED_LOG)[-1]["stage"] == "preflight"


def test_preflight_is_skipped_when_disabled(monkeypatch):
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", False)
    monkeypatch.setattr(d.importlib, "import_module", lambda *a, **k: pytest.fail("关闭时不该预检"))
    d.preflight_check_hardsub()


# ------------------------------------------------------------ near-miss / reject

class _FakeR2:
    def __init__(self, objects, fail_delete=False):
        self.objects = dict(objects)
        self.fail_delete = fail_delete
        self.deleted = []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        hits = [{"Key": k, "Size": v} for k, v in sorted(self.objects.items()) if k.startswith(Prefix)]
        return {"Contents": hits, "IsTruncated": False}

    def delete_object(self, Bucket, Key):
        if self.fail_delete:
            raise RuntimeError("R2 503")
        self.deleted.append(Key)
        self.objects.pop(Key, None)


def _setup_reject(tmp_path, monkeypatch, fail_delete=False):
    for name, filename in (("FAILED_LOG", "failed.jsonl"), ("SUCCESS_LOG", "success.jsonl"),
                           ("UPLOAD_PENDING_LOG", "upload_pending.jsonl")):
        monkeypatch.setattr(d, name, str(tmp_path / filename))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "results.jsonl"))
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "S3_PREFIX", "test")
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv")
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    r2 = _FakeR2({
        "test/tv/2010/66/S01/E02/E02.mp4": 900_000_000,
        "test/tv/2010/66/S01/E02/meta.json": 1_000,
        "test/tv/2010/66/S01/E02/subs/en.vtt": 2_000,
        "test/tv/2010/66/S01/E03/E03.mp4": 800_000_000,   # 同剧下一集，绝不能误删
        "test/tv/2011/77/S02/E01/E01.mp4": 700_000_000,
    }, fail_delete=fail_delete)
    monkeypatch.setattr(d, "get_s3_client", lambda: r2)
    with open(d.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        for tid, s, e, year, size in (("66", 1, 2, 2010, 900_000_000), ("66", 1, 3, 2010, 800_000_000),
                                      ("77", 2, 1, 2011, 700_000_000)):
            fh.write(json.dumps({
                "tmdbId": tid, "season": s, "episode": e, "title": f"T{tid}", "year": year,
                "uploaded": True, "url": hls(tid, e)["url"], "provider": "vidup",
                "node_type": "m3u8", "file_size_bytes": size, "missing_segment_count": 7,
            }) + "\n")
    near_dir = os.path.join(d.HARDSUB_EVIDENCE_DIR, "near_miss", "66_S01E02")
    os.makedirs(near_dir)
    open(os.path.join(near_dir, "1.jpg"), "wb").close()
    with open(d.HARDSUB_SCAN_LOG, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "66", "season": 1, "episode": 2, "title": "T66",
                             "verdict": "clean", "near_miss": ["hardsub"], "hit_ratio": 0.15,
                             "aligned_ratio": 0.05, "distinct_texts": 9, "checked_at": 100,
                             "evidence_dir": near_dir}) + "\n")
        fh.write(json.dumps({"tmdbId": "77", "season": 2, "episode": 1, "title": "T77",
                             "verdict": "clean", "near_miss": ["watermark"], "checked_at": 100,
                             "static_texts": [{"text": "sometext"}]}) + "\n")
    return r2


def test_near_miss_report_lists_pending_episodes(tmp_path, monkeypatch, capsys):
    _setup_reject(tmp_path, monkeypatch)
    d.report_near_miss()
    out = capsys.readouterr().out
    assert "66_S01E02\tT66\t临门槛:hardsub\t已上传 0.90 GB" in out
    assert "77_S02E01\tT77\t临门槛:watermark" in out and "常驻文字 ['sometext']" in out
    assert "66_S01E03" not in out
    assert "待核对 2 集" in out


def test_reject_removes_only_that_episode_and_rejects_only_its_version(tmp_path, monkeypatch):
    """🔴 R2 上这一集的目录全删（同剧下一集不动）、移出成品、只拉黑这个版本、截图挪走。"""
    r2 = _setup_reject(tmp_path, monkeypatch)

    done, freed = d.reject_episodes(["66_S01E02"])

    assert (done, freed) == (1, 900_003_000)
    assert sorted(r2.deleted) == ["test/tv/2010/66/S01/E02/E02.mp4",
                                  "test/tv/2010/66/S01/E02/meta.json",
                                  "test/tv/2010/66/S01/E02/subs/en.vtt"]
    assert "test/tv/2010/66/S01/E03/E03.mp4" in r2.objects
    assert sorted(d.record_episode_key(r) for r in _read_jsonl(d.SUCCESS_LOG)) == [
        "66_S01E03", "77_S02E01"]
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == [], "有版本信息时不整集判死"
    rejects = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    assert [(r["tmdbId"], r["season"], r["episode"], r["node_key"], r["manual"], r["reason"])
            for r in rejects] == [("66", 1, 2, "vidup|m3u8|" + FINAL, True, "hardsub")]
    assert [(d.record_episode_key(r), r["stage"]) for r in _read_jsonl(d.FAILED_LOG)] == [
        ("66_S01E02", "hardsub_retry")]
    assert not os.path.exists(os.path.join(d.HARDSUB_EVIDENCE_DIR, "near_miss", "66_S01E02"))
    # 同剧下一集的同一个版本不受影响。
    ep3 = {"tmdbId": "66", "season": 1, "episode": 3, "urls": [hls("66", 3)]}
    assert d.strip_rejected_nodes(ep3) == (ep3, 0)


def test_rejected_episode_is_not_listed_again_until_rescanned(tmp_path, monkeypatch, capsys):
    _setup_reject(tmp_path, monkeypatch)
    d.reject_episodes(["66_S01E02"])
    capsys.readouterr()
    d.report_near_miss()
    out = capsys.readouterr().out
    assert "66_S01E02" not in out and "另有 1 集已处置" in out
    with open(d.HARDSUB_SCAN_LOG, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "66", "season": 1, "episode": 2, "title": "T66",
                             "verdict": "clean", "near_miss": ["hardsub"],
                             "checked_at": 4_000_000_000}) + "\n")
    d.report_near_miss()
    assert "66_S01E02\tT66\t临门槛:hardsub" in capsys.readouterr().out


def test_reject_dry_run_changes_nothing(tmp_path, monkeypatch, capsys):
    r2 = _setup_reject(tmp_path, monkeypatch)
    before = open(d.SUCCESS_LOG, encoding="utf-8").read()
    d.reject_episodes(["66_S01E02"], dry_run=True)
    assert r2.deleted == [] and open(d.SUCCESS_LOG, encoding="utf-8").read() == before
    assert _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == []
    assert "将删除 test/tv/2010/66/S01/E02/E02.mp4" in capsys.readouterr().out


def test_reject_keeps_ledgers_when_r2_delete_fails(tmp_path, monkeypatch, capsys):
    _setup_reject(tmp_path, monkeypatch, fail_delete=True)
    before = open(d.SUCCESS_LOG, encoding="utf-8").read()
    assert d.reject_episodes(["66_S01E02"]) == (0, 0)
    assert open(d.SUCCESS_LOG, encoding="utf-8").read() == before
    assert _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == []
    assert "R2 删除失败，台账未改动" in capsys.readouterr().out


def test_reject_needs_reason_outside_the_near_miss_list(tmp_path, monkeypatch, capsys):
    r2 = _setup_reject(tmp_path, monkeypatch)
    assert d.reject_episodes(["66_S01E03"]) == (0, 0)
    assert "需用 --reason" in capsys.readouterr().out and r2.deleted == []
    assert d.reject_episodes(["66_S01E03"], reason="watermark")[0] == 1
    assert [r["reason"] for r in _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)] == ["watermark"]


def test_reject_gaps_lets_next_run_redownload(tmp_path, monkeypatch):
    """缺分片：R2 删干净、移出成品，但不判死、不拉黑版本——下次运行照常重下。"""
    r2 = _setup_reject(tmp_path, monkeypatch)
    assert d.reject_episodes(["77_S02E01"], reason="gaps") == (1, 700_000_000)
    assert r2.deleted == ["test/tv/2011/77/S02/E01/E01.mp4"]
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == [] and _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == []
    assert "77_S02E01" not in d.load_dead_keys()
    assert [r["stage"] for r in _read_jsonl(d.FAILED_LOG)] == ["gaps_retry"]


def test_reject_truncated_without_known_version_kills_as_truncated(tmp_path, monkeypatch):
    _setup_reject(tmp_path, monkeypatch)
    rows = _read_jsonl(d.SUCCESS_LOG)
    with open(d.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        for row in rows:
            row.pop("url", None)
            fh.write(json.dumps(row) + "\n")
    assert d.reject_episodes(["77_S02E01"], reason="truncated")[0] == 1
    dead = _read_jsonl(d.DOWNLOAD_DEAD_LOG)
    assert [(r["tmdbId"], r["season"], r["episode"], r["reason_class"], r["manual"])
            for r in dead] == [("77", 2, 1, "truncated", True)]
    assert d.dead_record_passes_now(dead[0]) is False


def test_reject_refuses_bad_reason_and_bad_key(tmp_path, monkeypatch, capsys):
    _setup_reject(tmp_path, monkeypatch)
    with pytest.raises(SystemExit):
        d.reject_episodes(["66_S01E02"], reason="blurry")
    assert d.reject_episodes(["66"]) == (0, 0)
    assert "不是集 key" in capsys.readouterr().out


def test_reject_refuses_while_main_runs(tmp_path, monkeypatch, capsys):
    r2 = _setup_reject(tmp_path, monkeypatch)
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    assert d.reject_episodes(["66_S01E02"]) == (0, 0)
    assert r2.deleted == [] and "主流程正在运行" in capsys.readouterr().out
