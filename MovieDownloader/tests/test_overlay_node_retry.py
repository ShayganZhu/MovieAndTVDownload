"""硬字幕/水印「换节点重下」（2026-10-24）的离线用例。

业务口径：带硬字幕或水印的**版本**不要；这部片还有没试过的节点就换节点重下，
所有版本都检出才整片判死。

重点锁死几件错了会丢片 / 白下 / 误判死的事：
  - 还有别的版本时绝不能写判死台账，也不能登记已完成（否则换节点重下被拦）；
  - 已检出的版本跨运行、跨重取都不能再下（重取会换回完整的一组节点）；
  - 所有版本都检出才判死，判死记录列全各版本的结论；
  - 最后几部片检出后的重下仍在本轮之内（来源取空也要等转封装跑完）。
"""

import json
import os

import pytest

import download_movies as d


PALEO = "sun.paleoak.top"
HAKUNA = "bcdn.hakunaymatata.com"


def mp4(tid, name="a", host=HAKUNA, sign="s1"):
    return {"url": f"https://{host}/resource/{tid}{name}.mp4?sign={sign}&t=1",
            "provider": "vidlink", "type": "mp4"}


def hls(tid, provider="videasy", host=PALEO):
    return {"url": f"https://{host}/r6/{tid}-{provider}/master.m3u8",
            "provider": provider, "type": "m3u8"}


def node_of(item):
    return d._normalize_url_entry(item)


def _read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _scan(*reasons):
    return {
        "verdict": "+".join(reasons) or "clean", "reasons": list(reasons),
        "sampled": 60, "hits": 24, "hit_ratio": 0.4, "distinct_texts": 20,
        "examples": ["weneedtotalk"],
        "watermark_texts": ([{"text": "piratesite", "presence": 1.0, "ring_std": 40.0}]
                            if "watermark" in reasons else []),
        "watermark_logo": None, "static_texts": [], "logo_peak_presence": 0.1,
    }


# ------------------------------------------------------------ 版本标识

def test_mp4_versions_of_one_provider_are_one_version():
    """vidlink 的 1920 / 1280 / 640 几个 mp4 是同一片源：一个检出，其余不再下。"""
    a = node_of(mp4("1", host="bcdn4.hakunaymatata.com", sign="x"))
    b = node_of(mp4("1", host="bcdnxw.hakunaymatata.com", sign="y"))
    other_file = node_of(mp4("1", name="b"))
    other_provider = node_of({**mp4("1"), "provider": "othermp4"})
    assert d.overlay_node_key(a) == d.overlay_node_key(b) == d.overlay_node_key(other_file)
    assert d.overlay_node_key(a) != d.overlay_node_key(other_provider)


def test_lower_resolution_variants_are_not_retried():
    entry = {"tmdbId": "2", "urls": [mp4("2", "1080"), mp4("2", "720"), hls("2")]}
    job = {"tmdbId": "2", "title": "T", "node": node_of(mp4("2", "1080")), "entry": entry}
    remaining = d.plan_overlay_retry(job, _scan("hardsub"))
    assert remaining["urls"] == [hls("2")]


def test_hls_version_is_provider_plus_host():
    """令牌每次重取都变：同一家同一主机算同一个版本；换家或换主机算别的版本。"""
    a = node_of(hls("1"))
    a2 = node_of({**hls("1"), "url": f"https://{PALEO}/r6/othertoken/master.m3u8"})
    assert d.overlay_node_key(a) == d.overlay_node_key(a2)
    assert d.overlay_node_key(a) != d.overlay_node_key(node_of(hls("1", provider="vidup")))
    assert d.overlay_node_key(a) != d.overlay_node_key(
        node_of(hls("1", host="moon.zenoak.top")))


# ------------------------------------------------------------ 台账与过滤

def test_rejected_version_is_stripped_and_survives_restart():
    entry = {"tmdbId": "7", "urls": [mp4("7"), hls("7")]}
    d.record_overlay_node_reject("7", "T", node_of(mp4("7")), _scan("hardsub"))
    stripped, removed = d.strip_rejected_nodes(entry)
    assert removed == 1 and stripped["urls"] == [hls("7")]
    assert entry["urls"] == [mp4("7"), hls("7")], "不能改原条目"

    d.reload_overlay_node_rejects()               # 模拟重启：从台账重读
    # 重取回来的新签名、新 CDN 主机，仍是同一个文件 → 照样滤掉。
    refetched = {"tmdbId": "7", "urls": [mp4("7", host="bcdn9.hakunaymatata.com",
                                             sign="new"), hls("7")]}
    assert d.strip_rejected_nodes(refetched)[0]["urls"] == [hls("7")]
    row = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)[0]
    assert (row["tmdbId"], row["reason"], row["provider"]) == ("7", "hardsub", "vidlink")


def test_strip_returns_same_object_when_nothing_rejected(monkeypatch):
    entry = {"tmdbId": "8", "urls": [mp4("8")]}
    assert d.strip_rejected_nodes(entry) == (entry, 0)
    d.record_overlay_node_reject("8", "T", node_of(mp4("8")), _scan("watermark"))
    monkeypatch.setattr(d, "OVERLAY_RETRY_OTHER_NODES", False)
    assert d.strip_rejected_nodes(entry) == (entry, 0)


def test_plan_retry_on_mp4_fallback_copy_returns_to_hls():
    """mp4 兜底副本（mp4 在前）检出：剩下的是 HLS 节点，回到原来的排队流程。"""
    entry = d._mp4_first({"tmdbId": "9", "urls": [hls("9"), mp4("9")]})
    job = {"tmdbId": "9", "title": "T", "node": node_of(mp4("9")), "entry": entry}
    remaining = d.plan_overlay_retry(job, _scan("hardsub"))
    assert remaining["urls"] == [hls("9")]


def test_plan_retry_returns_none_without_node_info():
    """老的转封装任务（不知道下的是哪个节点）：保持整片判死，不冒险重下同一版本。"""
    job = {"tmdbId": "9", "title": "T", "entry": {"tmdbId": "9", "urls": [mp4("9")]}}
    assert d.plan_overlay_retry(job, _scan("hardsub")) is None
    assert _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == []


# ------------------------------------------------------------ finalize 的分支

def _job(sandbox, monkeypatch, tmdb_id, node, entry):
    temp_mp4 = os.path.join(str(sandbox), "temp", f"temp_{tmdb_id}.mp4")
    os.makedirs(os.path.dirname(temp_mp4), exist_ok=True)

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"m" * 4096)
        return True

    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "kill")
    return {
        "tmdbId": tmdb_id, "normalized_id": tmdb_id, "title": "Hard Sub", "year": 2010,
        "url": node["url"], "node": node_of(node), "entry": entry,
        "final_ts": os.path.join(str(sandbox), "temp", f"temp_{tmdb_id}.ts"),
        "temp_mp4": temp_mp4, "cleanup_paths": [],
        "bitrate_kbps": 3000, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "processing_ids", set())
    return tmp_path


def test_version_with_hardsub_is_dropped_but_film_is_not_dead(sandbox, monkeypatch):
    """🔴 还有别的版本：删掉这个版本、换节点重下；绝不写判死台账、不登记已完成。"""
    entry = {"tmdbId": "66", "urls": [mp4("66"), hls("66")]}
    job = _job(sandbox, monkeypatch, "66", mp4("66"), entry)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    d._reset_overlay_stats()
    processed = set()
    d.processing_ids.add("66")

    tid, ok, info = d.finalize_one_entry(job, processed)

    assert ok is False and info["overlay_reject"] == "hardsub"
    assert info["overlay_retry_entry"]["urls"] == [hls("66")]
    assert not os.path.exists(job["temp_mp4"])
    assert "66" not in processed, "登记成已完成的话，换节点重下会被当成重复拦掉"
    assert "66" not in d.processing_ids
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []
    assert d.load_dead_ids() == set()
    assert len(_read_jsonl(d.OVERLAY_NODE_REJECT_LOG)) == 1
    scan = _read_jsonl(d.HARDSUB_SCAN_LOG)[0]
    assert scan["node_key"] == d.overlay_node_key(node_of(mp4("66")))
    assert d.overlay_stats["node_retry"] == 1 and d.overlay_stats["dead"] == 0


def test_last_version_with_overlay_kills_film_with_all_reasons(sandbox, monkeypatch):
    """所有版本都检出才整片判死；判死记录列全各版本、原因取并集。"""
    entry = {"tmdbId": "67", "urls": [hls("67")]}
    d.record_overlay_node_reject("67", "T", node_of(mp4("67")), _scan("watermark"))
    job = _job(sandbox, monkeypatch, "67", hls("67"), entry)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    processed = set()

    _, ok, info = d.finalize_one_entry(job, processed)

    assert ok is False and "overlay_retry_entry" not in info
    assert "67" in processed
    dead = _read_jsonl(d.DOWNLOAD_DEAD_LOG)
    assert len(dead) == 1
    assert dead[0]["reason_class"] == "hardsub"
    assert sorted(dead[0]["reasons"]) == ["hardsub", "watermark"]
    assert len(dead[0]["node_rejects"]) == 2
    assert d.load_dead_ids() == {"67"}


def test_feature_off_keeps_kill_on_first_detection(sandbox, monkeypatch):
    monkeypatch.setattr(d, "OVERLAY_RETRY_OTHER_NODES", False)
    entry = {"tmdbId": "68", "urls": [mp4("68"), hls("68")]}
    job = _job(sandbox, monkeypatch, "68", mp4("68"), entry)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan("hardsub"), []))
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is False and "overlay_retry_entry" not in info
    assert d.load_dead_ids() == {"68"}
    assert _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == []


# ------------------------------------------------------------ NodeRetrySource

class Src:
    def __init__(self, items=(), closed=True):
        self.items = list(items)
        self.closed = closed
        self.delivers_refetch_results = True

    def poll(self):
        if self.items:
            return "item", self.items.pop(0)
        return ("done", None) if self.closed else ("wait", None)

    def has_buffered(self, key):
        return key == "buf"


def test_requeued_entries_go_first():
    src = Src([{"tmdbId": "1", "urls": [mp4("1")]}])
    rs = d.NodeRetrySource()
    rs.set_source(src)
    rs.requeue({"tmdbId": "2", "urls": [hls("2")]})
    assert [rs.poll()[1]["tmdbId"] for _ in range(2)] == ["2", "1"]
    assert rs.poll() == ("done", None)


def test_source_items_are_filtered_and_fully_rejected_films_are_killed():
    """重取 / 存量里带回的已检出版本要滤掉；全部版本都检出过的不再派发。"""
    d.record_overlay_node_reject("3", "T", node_of(mp4("3")), _scan("hardsub"))
    d.record_overlay_node_reject("4", "T", node_of(mp4("4")), _scan("watermark"))
    killed = []
    rs = d.NodeRetrySource(on_all_rejected=killed.append)
    rs.set_source(Src([
        {"tmdbId": "3", "urls": [mp4("3", sign="fresh"), hls("3")]},
        {"tmdbId": "4", "urls": [mp4("4", sign="fresh")]},
        {"tmdbId": "5", "urls": [mp4("5")]},
    ]))
    got = []
    while True:
        state, entry = rs.poll()
        if state != "item":
            break
        got.append(entry)
    assert [(e["tmdbId"], e["urls"]) for e in got] == [("3", [hls("3")]), ("5", [mp4("5")])]
    assert [e["tmdbId"] for e in killed] == ["4"]


def test_waits_for_work_in_flight_before_finishing_the_round():
    """来源取空但还有下载/转封装在跑：先别收尾，那里检出的片可能马上要换节点重下。"""
    busy = {"n": 1}
    rs = d.NodeRetrySource(work_in_flight=lambda: busy["n"] > 0)
    rs.set_source(Src())
    assert rs.poll() == ("wait", None)
    rs.requeue({"tmdbId": "6", "urls": [hls("6")]})
    assert rs.poll()[1]["tmdbId"] == "6"
    busy["n"] = 0
    assert rs.poll() == ("done", None)


def test_passes_refetch_capability_through_for_the_gate():
    rs = d.NodeRetrySource()
    rs.set_source(Src())
    assert rs.delivers_refetch_results is True
    assert rs.has_buffered("buf") and not rs.has_buffered("x")


# ------------------------------------------------------------ 端到端

def _pipeline_env(tmp_path, monkeypatch, entries, verdict_of):
    """跑真实的 _run_pipeline：下载取条目的第一个节点，检测结论由 verdict_of(url) 决定。"""
    for name, filename in (
        ("FAILED_LOG", "failed.jsonl"), ("SUCCESS_LOG", "success.jsonl"),
        ("DOWNLOAD_OK_LOG", "download_ok.jsonl"),
        ("DOWNLOAD_FAIL_LOG", "download_fail.jsonl"),
        ("UPLOAD_PENDING_LOG", "upload_pending.jsonl"),
    ):
        monkeypatch.setattr(d, name, str(tmp_path / filename))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "async_refetch_hook", None)
    monkeypatch.setattr(d, "clean_temp_directory", lambda: None)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "HARDSUB_ACTION", "kill")
    monkeypatch.setattr(d, "processing_ids", set())
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    inp = tmp_path / "in.jsonl"
    inp.write_text("".join(json.dumps(e) + "\n" for e in entries), encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(inp))

    downloaded = []
    used = {}

    def fake_process(entry, ids):
        tid = str(entry["tmdbId"])
        with d.processing_lock:
            d.processing_ids.add(tid)
        node = d._normalize_url_entry(entry["urls"][0])
        downloaded.append((tid, node["url"]))
        used[tid] = node["url"]
        return tid, True, {
            "tmdbId": tid, "normalized_id": tid, "title": entry.get("title", tid),
            "year": 2010, "url": node["url"], "node": node, "entry": entry,
            "final_ts": os.path.join(d.TEMP_DIR, f"temp_{tid}.ts"),
            "temp_mp4": os.path.join(d.TEMP_DIR, f"temp_{tid}.mp4"),
            "cleanup_paths": [], "bitrate_kbps": 3000, "resolution": "1920x1080",
            "missing_segment_count": 0, "missing_segment_indices": [],
        }

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"m" * 1024)
        return True

    def fake_detect(path):
        tid = os.path.basename(path)[len("temp_"):-len(".mp4")]
        return _scan(*verdict_of(used[tid])), []

    uploads = []
    monkeypatch.setattr(d, "process_one_entry", fake_process)
    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    monkeypatch.setattr(d, "detect_overlays", fake_detect)
    monkeypatch.setattr(d, "upload_one_entry",
                        lambda info: uploads.append(info["tmdbId"]) or (info["tmdbId"], True, info))
    return downloaded, uploads


def test_pipeline_retries_other_node_and_uploads_clean_version(tmp_path, monkeypatch, capsys):
    """🔒 端到端：
    - 66：mp4 版本有硬字幕 → 换 paleoak 版本 → 干净 → 上传，不判死；
    - 67：两个版本都有（硬字幕 + 水印）→ 整片判死，判死记录列两个版本；
    - 68：只有一个版本且有水印 → 判死（和以前一样）。
    """
    entries = [
        {"tmdbId": "66", "title": "a", "year": 2010, "urls": [mp4("66"), hls("66")]},
        {"tmdbId": "67", "title": "b", "year": 2010, "urls": [mp4("67"), hls("67")]},
        {"tmdbId": "68", "title": "c", "year": 2010, "urls": [mp4("68")]},
    ]
    dirty = {mp4("66")["url"]: ("hardsub",), mp4("67")["url"]: ("hardsub",),
             hls("67")["url"]: ("watermark",), mp4("68")["url"]: ("watermark",)}
    downloaded, uploads = _pipeline_env(
        tmp_path, monkeypatch, entries, lambda url: dirty.get(url, ()))

    d._run_pipeline()

    assert uploads == ["66"]
    assert sorted(downloaded) == sorted([
        ("66", mp4("66")["url"]), ("66", hls("66")["url"]),
        ("67", mp4("67")["url"]), ("67", hls("67")["url"]),
        ("68", mp4("68")["url"]),
    ]), "每个版本恰好下一次"
    assert d.load_dead_ids() == {"67", "68"}
    dead = {r["tmdbId"]: r for r in _read_jsonl(d.DOWNLOAD_DEAD_LOG)}
    assert len(dead["67"]["node_rejects"]) == 2
    stages = sorted((r["tmdbId"], r["stage"]) for r in _read_jsonl(d.FAILED_LOG))
    assert stages == [("66", "hardsub_retry"), ("67", "hardsub_retry"),
                      ("67", "watermark"), ("68", "watermark")]
    out = capsys.readouterr().out
    assert "合计判死 2 部" in out and "检出后换节点重下 2 次" in out


def test_restart_skips_versions_rejected_in_an_earlier_run(tmp_path, monkeypatch):
    """跨运行：上次已检出的版本，这次（链接已换新签名）不再下，直接从剩下的版本开始。"""
    d.record_overlay_node_reject("70", "T", node_of(mp4("70")), _scan("hardsub"))
    entries = [{"tmdbId": "70", "title": "x", "year": 2010,
                "urls": [mp4("70", sign="new", host="bcdn7.hakunaymatata.com"), hls("70")]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert downloaded == [("70", hls("70")["url"])]
    assert uploads == ["70"]


def test_film_whose_every_version_was_rejected_earlier_is_killed_without_download(
        tmp_path, monkeypatch):
    d.record_overlay_node_reject("71", "T", node_of(mp4("71")), _scan("hardsub"))
    d.record_overlay_node_reject("71", "T", node_of(hls("71")), _scan("watermark"))
    entries = [{"tmdbId": "71", "title": "x", "year": 2010, "urls": [mp4("71"), hls("71")]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert downloaded == [] and uploads == []
    assert d.load_dead_ids() == {"71"}


def test_retry_source_keeps_asking_inner_source_after_done():
    """收尾那段时间里，下载失败投出的重取结果还会从原来源回来：不能记住 done 不再问。"""
    src = Src(closed=True)
    rs = d.NodeRetrySource(work_in_flight=lambda: True)
    rs.set_source(src)
    assert rs.poll() == ("wait", None)
    src.items.append({"tmdbId": "8", "urls": [hls("8")]})
    assert rs.poll()[1]["tmdbId"] == "8"


def test_success_record_carries_the_delivered_version(sandbox, monkeypatch):
    entry = {"tmdbId": "69", "urls": [mp4("69"), hls("69")]}
    job = _job(sandbox, monkeypatch, "69", hls("69"), entry)
    monkeypatch.setattr(d, "detect_overlays", lambda p: (_scan(), []))
    _, ok, info = d.finalize_one_entry(job, set())
    assert ok is True
    assert (info["provider"], info["node_type"]) == ("videasy", "m3u8")
    assert info["node_key"] == d.overlay_node_key(node_of(hls("69")))


# ------------------------------------------------------------ 临门槛人工 reject：只判这个版本

from test_download_movies import _setup_reject  # noqa: E402


def _reject_env(tmp_path, monkeypatch, success_extra, results_rows):
    r2 = _setup_reject(tmp_path, monkeypatch)
    rows = _read_jsonl(d.SUCCESS_LOG)
    with open(d.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        for row in rows:
            if row["tmdbId"] == "66":
                row.update(success_extra)
            fh.write(json.dumps(row) + "\n")
    results = tmp_path / "results.jsonl"
    results.write_text("".join(json.dumps(r) + "\n" for r in results_rows), encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(results))
    return r2


def test_manual_reject_rejects_only_the_uploaded_version(tmp_path, monkeypatch, capsys):
    """🔴 人工确认临门槛片带硬字幕：只判上传的那个版本，其它版本下次运行照常重下。"""
    _reject_env(tmp_path, monkeypatch, {"url": mp4("66")["url"]}, [
        {"tmdbId": "66", "urls": [mp4("66"), hls("66")]},
    ])
    done, freed = d.reject_films(["66"])

    assert done == 1 and freed > 0
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == [], "不能整片判死"
    assert "66" not in d.load_dead_ids()
    rejects = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    assert [(r["tmdbId"], r["node_key"], r["manual"], r["reason"]) for r in rejects] == [
        ("66", "vidlink|mp4", True, "hardsub")]
    assert [(r["tmdbId"], r["stage"]) for r in _read_jsonl(d.FAILED_LOG)] == [("66", "hardsub_retry")]
    version_dir = os.path.join(d.HARDSUB_EVIDENCE_DIR, "66",
                               d._overlay_version_label(node_of(mp4("66"))))
    assert os.listdir(version_dir) == ["1.jpg"]
    assert rejects[0]["evidence_dir"] == version_dir
    entry = {"tmdbId": "66", "urls": [mp4("66"), hls("66")]}
    assert d.strip_rejected_nodes(entry)[0]["urls"] == [hls("66")]
    capsys.readouterr()
    d.report_near_miss()
    assert "另有 1 部已处置" in capsys.readouterr().out


def test_manual_reject_uses_version_recorded_in_success_log(tmp_path, monkeypatch):
    _reject_env(tmp_path, monkeypatch,
                {"url": hls("66")["url"], "provider": "videasy", "node_type": "m3u8"}, [])
    d.reject_films(["66"])
    assert [r["node_key"] for r in _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)] == [
        d.overlay_node_key(node_of(hls("66")))]
    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []


def test_film_rescanned_after_manual_reject_is_listed_again(tmp_path, monkeypatch, capsys):
    """处置后换别的版本重下、新版本又临门槛：扫描记录比处置新，要重新列出来核对。"""
    _reject_env(tmp_path, monkeypatch, {"url": mp4("66")["url"]}, [
        {"tmdbId": "66", "urls": [mp4("66"), hls("66")]},
    ])
    d.reject_films(["66"])
    with open(d.HARDSUB_SCAN_LOG, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "66", "title": "T66", "verdict": "clean",
                             "near_miss": ["hardsub"], "checked_at": 4_000_000_000}) + "\n")
    capsys.readouterr()
    d.report_near_miss()
    assert "66\tT66\t临门槛:hardsub" in capsys.readouterr().out


def test_manual_truncated_reject_rejects_only_the_uploaded_version(tmp_path, monkeypatch):
    """🔴 人工复核确认时长不符（片源给的是片段）：和硬字幕一样只判这个版本，换别的版本重下。"""
    _reject_env(tmp_path, monkeypatch, {"url": mp4("66")["url"]}, [
        {"tmdbId": "66", "urls": [mp4("66"), hls("66")]},
    ])
    assert d.reject_films(["66"], reason="truncated")[0] == 1

    assert _read_jsonl(d.DOWNLOAD_DEAD_LOG) == []
    rejects = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    assert [(r["node_key"], r["reason"], r["manual"]) for r in rejects] == [
        ("vidlink|mp4", "truncated", True)]
    assert "人工复核确认时长不符" in rejects[0]["summary"]
    assert [(r["tmdbId"], r["stage"]) for r in _read_jsonl(d.FAILED_LOG)] == [("66", "truncated_retry")]
    entry = {"tmdbId": "66", "urls": [mp4("66"), hls("66")]}
    assert d.strip_rejected_nodes(entry)[0]["urls"] == [hls("66")]


def test_film_whose_every_version_is_truncated_is_killed_as_truncated(tmp_path, monkeypatch):
    """所有版本都是人工确认时长不符：整片判死，原因记 truncated，不能被错记成水印。"""
    for node in (mp4("72"), hls("72")):
        d.record_overlay_node_reject("72", "T", node_of(node), {"reasons": ["truncated"]})
    entries = [{"tmdbId": "72", "title": "x", "year": 2010, "urls": [mp4("72"), hls("72")]}]
    downloaded, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())

    d._run_pipeline()

    assert downloaded == [] and uploads == []
    dead = _read_jsonl(d.DOWNLOAD_DEAD_LOG)
    assert [(r["tmdbId"], r["reason_class"]) for r in dead] == [("72", "truncated")]
    assert d.dead_record_passes_now(dead[0]) is False
