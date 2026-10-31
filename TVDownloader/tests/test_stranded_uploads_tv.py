"""滞留成品补传（电影侧 21244bf 移植，2026-10-31）的离线用例：磁盘上有成品、却没有上传成功记录的集，
启动时直接接进上传阶段，不用等一整轮跑完的收尾补传（23 号机重启后一次留下 494 集、801 GB）。"""

import json
import os

import download_tv as d


def _write(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


def _read(path):
    if not os.path.exists(path):
        return []
    return [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]


def _rec(tid, ep, path, uploaded):
    return {"tmdbId": tid, "season": 1, "episode": ep, "title": f"T{tid}", "year": 2020,
            "final_path": str(path), "uploaded": uploaded, "provider": "vidup", "duration_seconds": 1500.0}


def test_find_stranded_uploads(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    files = {}
    for ep in (1, 2, 3, 4, 5):
        files[ep] = tmp_path / f"E0{ep}.mp4"
        files[ep].write_bytes(b"x" * 10)
    files[5].unlink()                                         # 本地文件已不在
    _write(d.SUCCESS_LOG, [
        _rec("7", 1, files[1], False),                        # 滞留：要补传
        _rec("7", 2, files[2], False), _rec("7", 2, files[2], True),   # 最后一条已上传
        _rec("7", 3, files[3], False),                        # 判死：不补传
        _rec("7", 5, files[5], False),
    ])
    disk = {"7_S01E01", "7_S01E02", "7_S01E03", "7_S01E04", "7_S01E05"}
    jobs = d.find_stranded_uploads(disk, skip_ids={"7_S01E03"})
    assert [d.record_episode_key(j) for j in jobs] == ["7_S01E01"], "E04 没有成功记录（启动巡检会先补写）"
    job = jobs[0]
    assert job["stranded"] and "uploaded" not in job and job["final_path"] == str(files[1])
    assert job["provider"] == "vidup" and job["duration_seconds"] == 1500.0, "归因与实测字段原样带上"


def test_remove_video_pending_keeps_assets_only_and_others(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    _write(d.UPLOAD_PENDING_LOG, [
        {"tmdbId": "7", "season": 1, "episode": 1, "local_path": "a"},
        {"tmdbId": "7", "season": 1, "episode": 1, "assets_only": True, "local_path": "a"},
        {"tmdbId": "8", "season": 1, "episode": 1, "local_path": "b"},
    ])
    d.remove_video_pending_records(["7_S01E01"])
    assert [(d.record_episode_key(r), bool(r.get("assets_only"))) for r in _read(d.UPLOAD_PENDING_LOG)] == [
        ("7_S01E01", True), ("8_S01E01", False)]


def test_successful_stranded_upload_clears_old_ledgers(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    _write(d.UPLOAD_PENDING_LOG, [{"tmdbId": "7", "season": 1, "episode": 1, "local_path": "a"}])
    _write(d.FAILED_LOG, [{"tmdbId": "7", "season": 1, "episode": 1, "stage": "upload", "error": "降级"},
                          {"tmdbId": "7", "season": 1, "episode": 1, "stage": "download", "error": "x"}])
    monkeypatch.setattr(d, "upload_one_entry",
                        lambda info: (d.record_episode_key(info), True, {**info, "uploaded": True}))
    key, ok, _ = d._upload_stranded({"tmdbId": "7", "season": 1, "episode": 1, "final_path": "a"})
    assert (key, ok) == ("7_S01E01", True)
    assert _read(d.UPLOAD_PENDING_LOG) == []
    assert [r["stage"] for r in _read(d.FAILED_LOG)] == ["download"]


def test_failed_stranded_upload_keeps_ledgers(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    _write(d.UPLOAD_PENDING_LOG, [{"tmdbId": "7", "season": 1, "episode": 1, "local_path": "a"}])
    monkeypatch.setattr(d, "upload_one_entry",
                        lambda info: (d.record_episode_key(info), False, {"error": "R2 503"}))
    d._upload_stranded({"tmdbId": "7", "season": 1, "episode": 1, "final_path": "a"})
    assert len(_read(d.UPLOAD_PENDING_LOG)) == 1


def test_main_loop_uploads_stranded_episodes_in_the_same_run(tmp_path, monkeypatch, capsys):
    """🔴 端到端：启动时本地躺着的成品当轮就上传，不等整轮结束的收尾补传。"""
    from test_overlay_tv import _pipeline_env, hls

    entries = [{"tmdbId": "96", "season": 1, "episode": 1, "title": "y", "urls": [hls("96", 1)]}]
    _, uploads = _pipeline_env(tmp_path, monkeypatch, entries, lambda url: ())
    stranded = tmp_path / "stranded.mp4"
    stranded.write_bytes(b"m" * 100)
    _write(d.SUCCESS_LOG, [_rec("97", 1, stranded, False)])
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: ({"97_S01E01"}, {}))
    monkeypatch.setattr(d, "reconcile_unlogged_downloads", lambda disk, logged: 0)

    d._run_pipeline()

    assert sorted(uploads) == ["96_S01E01", "97_S01E01"]
    assert "发现 1 集成品已在本地、但没有上传成功记录" in capsys.readouterr().out
