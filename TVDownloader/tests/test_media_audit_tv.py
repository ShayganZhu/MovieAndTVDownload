"""成品时长 / 音轨记录与 audit 筛查（2026-10-30）的离线用例。"""

import json
import os
import shutil
import subprocess

import pytest

import download_tv as d


def _record(key="10_S01E01", **fields):
    tid, rest = key.split("_S")
    season, episode = rest.split("E")
    return {"tmdbId": tid, "season": int(season), "episode": int(episode), "title": f"T{tid}",
            "uploaded": True, "file_size_bytes": 1_000_000_000, "provider": "vidup",
            "node_type": "m3u8", **fields}


# ------------------------------------------------------------ 实测

@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="需要 ffmpeg")
def test_probe_media_reads_duration_and_audio(tmp_path):
    with_audio, silent = str(tmp_path / "a.mp4"), str(tmp_path / "b.mp4")
    base = ["ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "testsrc=duration=3:size=64x64:rate=10"]
    subprocess.run(base + ["-f", "lavfi", "-i", "sine=duration=3", "-shortest", with_audio], check=True)
    subprocess.run(base + [silent], check=True)
    got = d.probe_media(with_audio)
    assert got["audio_streams"] == 1 and got["video_streams"] == 1
    assert 2.5 <= got["duration_seconds"] <= 3.5
    assert d.probe_media(silent)["audio_streams"] == 0


def test_probe_media_failure_is_harmless(tmp_path):
    bad = tmp_path / "x.mp4"
    bad.write_bytes(b"not a video")
    assert d.probe_media(str(bad)) == {}
    fields = d._media_fields(str(bad), {"runtime_minutes": 30})
    assert fields["duration_seconds"] is None and fields["audio_streams"] is None
    assert fields["expected_runtime_minutes"] == 30 and fields["runtime_source"] == "imdb_show"


def test_expected_runtime_prefers_tmdb_episode_runtime():
    assert d.expected_runtime_of({"episode_runtime_minutes": 42, "runtime_minutes": 60}) == (
        42, "tmdb_episode")
    assert d.expected_runtime_of({"runtime_minutes": "60"}) == (60, "imdb_show")
    assert d.expected_runtime_of({"runtime_minutes": None}) == (None, None)
    assert d.expected_runtime_of(None) == (None, None)


# ------------------------------------------------------------ 判定

@pytest.fixture
def thresholds(monkeypatch):
    monkeypatch.setattr(d, "MEDIA_AUDIT_MIN_RUNTIME_RATIO", 0.5)
    monkeypatch.setattr(d, "MEDIA_AUDIT_MAX_RUNTIME_RATIO", 2.5)
    monkeypatch.setattr(d, "MEDIA_AUDIT_MIN_SOURCE_RATIO", 0.95)
    monkeypatch.setattr(d, "MEDIA_AUDIT_MIN_SECONDS", 300)


def _flags(**fields):
    return [reason for reason, _ in d._audit_flags(fields)]


def test_audit_flags(thresholds):
    ok = dict(duration_seconds=2580, audio_streams=1, expected_runtime_minutes=45,
              runtime_source="tmdb_episode", source_duration_seconds=2585)
    assert _flags(**ok) == []
    assert _flags(**{**ok, "audio_streams": 0}) == ["no_audio"]
    assert _flags(**{**ok, "duration_seconds": 120, "source_duration_seconds": 121}) == ["truncated"], \
        "预告片：片源本身就短，跟片源比没问题、跟参考时长比偏短"
    assert _flags(**{**ok, "duration_seconds": 45 * 60 * 3, "source_duration_seconds": None}) == [
        "truncated"]
    assert _flags(**{**ok, "source_duration_seconds": 2580 / 0.8}) == ["truncated"], "没下全"
    no_ref = dict(duration_seconds=100, audio_streams=1)
    assert _flags(**no_ref) == ["truncated"] and _flags(duration_seconds=900, audio_streams=1) == []
    assert d._audit_flags({"tmdbId": "1"}) is None, "加实测之前的老记录"


def test_audit_writes_review_list_and_reject_from_reads_it(tmp_path, monkeypatch, thresholds, capsys):
    log = tmp_path / "success.jsonl"
    rows = [
        _record("10_S01E01", duration_seconds=2600, audio_streams=1, expected_runtime_minutes=45,
                runtime_source="tmdb_episode"),
        _record("10_S01E02", duration_seconds=2600, audio_streams=0, expected_runtime_minutes=45),
        _record("11_S02E05", duration_seconds=95, audio_streams=1, expected_runtime_minutes=22,
                runtime_source="imdb_show"),
        _record("12_S01E01"),                               # 老记录：没有实测
    ]
    log.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    monkeypatch.setattr(d, "SUCCESS_LOG", str(log))
    out = tmp_path / "audit.tsv"

    assert d.audit_media(str(out)) == 2
    printed = capsys.readouterr().out
    assert "可疑 2 集（无音轨 1、时长不符 1）" in printed
    assert "另有 1 集是加上实测之前下的" in printed
    assert "reject --from" in printed
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("#")
    assert [line.split("\t")[:2] for line in lines[1:]] == [
        ["10_S01E02", "no_audio"], ["11_S02E05", "truncated"]]
    assert "IMDB 剧级时长 22 分钟" in lines[2]

    # 人工核对：删掉一行误报、另加一行不带理由的
    out.write_text(lines[0] + "\n" + lines[1] + "\n\n" + "13_S01E01\n", encoding="utf-8")
    assert d.read_reject_list(str(out)) == [("10_S01E02", "no_audio"), ("13_S01E01", None)]


def test_audit_refuses_device_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "none.jsonl"))
    with pytest.raises(SystemExit, match="普通文件"):
        d.audit_media("/dev/null")


def test_reject_list_groups_by_row_reason(monkeypatch):
    calls = []
    monkeypatch.setattr(d, "reject_episodes",
                        lambda keys, reason=None, dry_run=False: calls.append((keys, reason, dry_run))
                        or (len(keys), 10))
    items = [("1_S01E01", "no_audio"), ("2_S01E01", None), ("3_S01E01", "no_audio")]
    assert d.reject_list(items, reason="truncated", dry_run=True) == (3, 20)
    assert calls == [(["1_S01E01", "3_S01E01"], "no_audio", True), (["2_S01E01"], "truncated", True)]


def test_dead_no_audio_is_never_revived():
    assert d.dead_record_passes_now({"reason_class": "no_audio"}) is False
    assert d.REJECT_REASON_LABELS["no_audio"] == "无音轨"


def test_shipped_media_audit_config():
    import yaml

    cfg = yaml.safe_load(open(os.path.join(os.path.dirname(d.__file__), "config.yaml"),
                              encoding="utf-8"))["download_tv"]
    assert cfg["media_audit"] == {
        "min_runtime_ratio": 0.5, "max_runtime_ratio": 2.5, "min_source_ratio": 0.95,
        "min_seconds_without_reference": 300, "output": "audit_flagged.tsv"}
    assert cfg["segment_concurrency"] == 8
    assert cfg["m3u8_host_circuit_threshold"] == 30


# ------------------------------------------------------------ 落账

def test_finalize_records_media_fields_and_meta(sandbox_media, monkeypatch, capsys):
    """🔴 转封装后实测时长 / 音轨、片源声明时长、参考时长都进 success 记录与 meta.json。"""
    monkeypatch.setattr(d, "probe_media", lambda path: {
        "duration_seconds": 2581.2, "audio_streams": 0, "video_streams": 1})
    job = sandbox_media(entry={"episode_runtime_minutes": 44, "runtime_minutes": 60},
                        source_duration_seconds=2590.0)
    key, ok, info = d.finalize_one_entry(job, set())
    assert ok is True
    assert {k: info[k] for k in d._MEDIA_KEYS} == {
        "duration_seconds": 2581.2, "audio_streams": 0, "video_streams": 1,
        "source_duration_seconds": 2590.0, "expected_runtime_minutes": 44,
        "runtime_source": "tmdb_episode"}
    assert "成品没有音轨" in capsys.readouterr().out
    with open(os.path.join(os.path.dirname(info["final_path"]), "meta.json"), encoding="utf-8") as fh:
        video = json.load(fh)["video"]
    assert video["durationSeconds"] == 2581.2 and video["audioStreams"] == 0


@pytest.fixture
def sandbox_media(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "convert_ts_to_mp4",
                        lambda src, dst: open(dst, "wb").write(b"mp4") > 0)
    os.makedirs(tmp_path / "temp")

    def make(entry, **extra):
        ts = tmp_path / "temp" / "temp_5_S01E02.ts"
        ts.write_bytes(b"x")
        d.processing_ids.add("5_S01E02")
        return {
            "tmdbId": "5", "season": 1, "episode": 2, "normalized_id": "5_S01E02",
            "title": "T", "year": 2020, "url": "u", "final_ts": str(ts),
            "temp_mp4": str(tmp_path / "temp" / "temp_5_S01E02.mp4"), "cleanup_paths": [str(ts)],
            "bitrate_kbps": 2000, "resolution": "1920x1080", "missing_segment_count": 0,
            "missing_segment_indices": [], "entry": entry, **extra,
        }
    yield make
    d.processing_ids.discard("5_S01E02")
