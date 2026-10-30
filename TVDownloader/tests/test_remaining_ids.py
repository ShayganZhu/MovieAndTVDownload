"""remaining_ids.py（TV 版，逐集核对、按剧汇总）的离线用例。"""

import json
from datetime import date

import pytest

import remaining_ids as r


class _FakeR2:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, name):
        objects = self.objects

        class _P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k, "Size": v} for k, v in objects.items()
                                    if k.startswith(Prefix)]}

        return _P()


def _write_jsonl(path, rows):
    path.write_text("".join(json.dumps(x) + "\n" for x in rows), encoding="utf-8")


@pytest.fixture
def box(tmp_path):
    (tmp_path / "ids.txt").write_text("1\n2\n3\n4\n1\n", encoding="utf-8")
    _write_jsonl(tmp_path / "seasons_cache.jsonl", [
        {"tmdbId": "1", "ended": True, "seasons": [
            {"season": 0, "episodes": [1], "air_dates": {"1": "2001-01-01"}},
            {"season": 1, "episodes": [1, 2, 3, 4, 5, 6, 7, 8, 9],
             "air_dates": {str(e): "2001-01-01" for e in range(1, 9)} | {"9": "2099-01-01"}},
        ]},
        {"tmdbId": "2", "seasons": [{"season": 1, "episodes": [1]}]},       # 旧格式：无 air_dates 全算已播
    ])
    _write_jsonl(tmp_path / "success.jsonl", [
        {"tmdbId": "1", "season": 1, "episode": 1, "uploaded": True, "file_size_bytes": 100},
        {"tmdbId": "1", "season": 1, "episode": 2, "uploaded": True, "file_size_bytes": 200},
        {"tmdbId": "1", "season": 1, "episode": 3, "uploaded": False},
        {"tmdbId": "2", "season": 1, "episode": 1, "uploaded": True, "file_size_bytes": 50},
    ])
    _write_jsonl(tmp_path / "download_dead.jsonl", [
        {"tmdbId": "1", "season": 1, "episode": 4, "reason_class": "hardsub"},
    ])
    _write_jsonl(tmp_path / "results.jsonl", [
        {"tmdbId": "1", "season": 1, "episode": 5, "urls": []},
        {"tmdbId": "1", "season": 1, "episode": 6, "urls": []},
    ])
    _write_jsonl(tmp_path / "failed.jsonl", [{"tmdbId": "1", "season": 1, "episode": 6, "stage": "download"}])
    (tmp_path / "fail.txt").write_text("1\t1\t7\n1\t1\t8\tretry-exhausted\n3\t-\t-\n", encoding="utf-8")
    (tmp_path / "provider_down.txt").write_text("", encoding="utf-8")
    return tmp_path


def _reasons(tmp_path, **kw):
    report = tmp_path / "report.jsonl"
    summary = r.run(tmp_path, report="report.jsonl", today=date(2026, 10, 30), config={}, **kw)
    rows = [json.loads(line) for line in report.read_text(encoding="utf-8").splitlines()]
    return summary, {row.get("key") or row["tmdbId"]: row["reason"] for row in rows}


def test_episodes_are_classified_and_shows_with_open_episodes_are_listed(box):
    summary, reasons = _reasons(box)
    assert reasons == {
        "1_S00E01": "not_fetched",
        "1_S01E03": "downloaded_not_uploaded",
        "1_S01E04": "dead:hardsub",
        "1_S01E05": "links_not_downloaded",
        "1_S01E06": "download_failed",
        "1_S01E07": "no_source",
        "1_S01E08": "fetch_unresolved",
        "3": "show:no_source",
        "4": "show:not_expanded",
    }
    assert summary["done"] == {"uploaded": 3}
    assert summary["unaired"] == 1, "未播出的 1_S01E09 不算未完成"
    assert (box / "ids_remaining.txt").read_text(encoding="utf-8").split() == ["1", "3", "4"], \
        "剧 2 全部完成，不进补跑名单；重复的 1 只出现一次"


def test_verify_r2_reports_missing_mismatch_and_r2_only(box):
    r2 = _FakeR2({
        "tv/2001/1/S01/E01/E01.mp4": 100,
        "tv/2001/1/S01/E02/E02.mp4": 999,            # 大小与台账不符
        "tv/2001/1/S00/E01/E01.mp4": 10,             # 台账没记、R2 上有
        "tv/2001/1/S01/E01/meta.json": 1,
    })
    summary, reasons = _reasons(box, verify_r2=True, r2_client=(r2, "b"))
    assert reasons["1_S01E02"] == "r2_size_mismatch"
    assert reasons["2_S01E01"] == "r2_missing"
    assert "1_S00E01" not in reasons and summary["done"] == {"uploaded": 1, "r2_only": 1}
    assert summary["r2_listed"] == 3


def test_exclude_dead_drops_dead_and_no_source_episodes(box):
    summary, _ = _reasons(box, exclude_dead=True)
    assert summary["excluded"] == {"dead:hardsub": 1, "no_source": 1}
    assert (box / "ids_remaining.txt").read_text(encoding="utf-8").split() == ["1", "4"]


def test_specials_are_skipped_when_disabled(box):
    summary = r.run(box, today=date(2026, 10, 30),
                    config={"tv_ids_to_links": {"include_specials": False}})
    assert summary["remaining"].get("not_fetched", 0) == 0


@pytest.mark.parametrize("out", ["/dev/null", "/dev/stdout"])
def test_output_must_be_a_regular_file(box, out):
    """原子写会 os.replace 到目标：指向设备文件时（以 root 运行）会把设备换成普通文件。"""
    with pytest.raises(SystemExit, match="必须是普通文件"):
        r.run(box, out=out, config={})
    with pytest.raises(SystemExit, match="必须是普通文件"):
        r.run(box, report=out, config={})


def test_output_cannot_overwrite_the_input(box):
    with pytest.raises(SystemExit, match="不能指向输入文件"):
        r.run(box, out="ids.txt", config={})
