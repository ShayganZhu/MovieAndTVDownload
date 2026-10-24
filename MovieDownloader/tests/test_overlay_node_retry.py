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

def test_mp4_version_ignores_cdn_host_and_signature():
    a = node_of(mp4("1", host="bcdn4.hakunaymatata.com", sign="x"))
    b = node_of(mp4("1", host="bcdnxw.hakunaymatata.com", sign="y"))
    other = node_of(mp4("1", name="b"))
    assert d.overlay_node_key(a) == d.overlay_node_key(b)
    assert d.overlay_node_key(a) != d.overlay_node_key(other)


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
