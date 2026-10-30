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
    assert (box / "ids_remaining.txt").read_text(encoding="utf-8").split() == ["1", "4"], \
        "剧 2 全部完成、剧 3 整剧无源，都不进补跑名单；重复的 1 只出现一次"
    assert summary["excluded"] == {"dead:hardsub": 1, "no_source": 1}, \
        "剧 1 的判死 / 无源集照常计数，只是不算补跑能推进的集"


def test_verify_r2_reports_missing_mismatch_and_r2_only(box):
    r2 = _FakeR2({
        "tvs/2001/1/S01/E01/E01.mp4": 100,
        "tvs/2001/1/S01/E02/E02.mp4": 999,            # 大小与台账不符
        "tvs/2001/1/S00/E01/E01.mp4": 10,             # 台账没记、R2 上有
        "tvs/2001/1/S01/E01/meta.json": 1,
    })
    summary, reasons = _reasons(box, verify_r2=True, r2_client=(r2, "b"))
    assert reasons["1_S01E02"] == "r2_size_mismatch"
    assert reasons["2_S01E01"] == "r2_missing"
    assert "1_S00E01" not in reasons and summary["done"] == {"uploaded": 1, "r2_only": 1}
    assert summary["r2_listed"] == 3


def _add_terminal_only_shows(box):
    """剧 5 只剩无源 + 硬字幕判死的集，剧 6 只剩画质判死的集：补跑都不会推进。"""
    with open(box / "ids.txt", "a", encoding="utf-8") as fh:
        fh.write("5\n6\n")
    with open(box / "seasons_cache.jsonl", "a", encoding="utf-8") as fh:
        for tid in ("5", "6"):
            fh.write(json.dumps({"tmdbId": tid, "ended": True,
                                 "seasons": [{"season": 1, "episodes": [1, 2]}]}) + "\n")
    with open(box / "fail.txt", "a", encoding="utf-8") as fh:
        fh.write("5\t1\t1\n")
    with open(box / "download_dead.jsonl", "a", encoding="utf-8") as fh:
        for tid, ep, cls in (("5", 2, "hardsub"), ("6", 1, "quality"), ("6", 2, "quality")):
            fh.write(json.dumps({"tmdbId": tid, "season": 1, "episode": ep, "reason_class": cls}) + "\n")


def test_shows_with_only_dead_or_no_source_episodes_are_held_back(box, capsys):
    """🔴 TV 集级有源率只有个位数百分比：只剩无源 / 判死集的剧照常计数、进明细，但不进补跑名单
    ——否则名单约等于整个 ids.txt，补跑一遍什么也不会发生。"""
    _add_terminal_only_shows(box)
    summary, reasons = _reasons(box)
    out = capsys.readouterr().out

    assert (box / "ids_remaining.txt").read_text(encoding="utf-8").split() == ["1", "4"]
    assert summary["held_back"] == 2
    assert summary["excluded"] == {"dead:hardsub": 2, "dead:quality": 2, "no_source": 2}
    assert summary["shows_by_reason"]["dead:quality"] == 1
    assert summary["shows_by_reason"]["no_source"] == 2, "剧 1 与剧 5 各有无源集"
    assert reasons["6_S01E01"] == "dead:quality", "明细照常列出"
    rows = [json.loads(x) for x in (box / "report.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {row.get("key") or row["tmdbId"]: row["in_remaining"] for row in rows
            if row["tmdbId"] in ("3", "5")} == {"3": False, "5_S01E01": False, "5_S01E02": False}

    assert "另有 3 部剧只剩无源 / 判死集" in out, "剧 5、6 加整剧无源的剧 3"
    assert "python tv_ids_to_links.py --recheck-dead" in out
    assert "python download_tv.py --retry-dead" in out


def test_include_dead_restores_the_old_list(box, capsys):
    _add_terminal_only_shows(box)
    summary, _ = _reasons(box, include_dead=True)
    assert (box / "ids_remaining.txt").read_text(encoding="utf-8").split() == ["1", "3", "4", "5", "6"]
    assert summary["excluded"] == {} and summary["held_back"] == 0
    assert "补跑不会推进" not in capsys.readouterr().out


def test_cli_keeps_exclude_dead_as_the_default(box, monkeypatch):
    seen = []
    monkeypatch.setattr(r, "run", lambda *a, **kw: seen.append(kw["include_dead"]))
    r.main(["--dir", str(box), "--exclude-dead"])
    r.main(["--dir", str(box)])
    r.main(["--dir", str(box), "--include-dead"])
    assert seen == [False, False, True]
    with pytest.raises(SystemExit):
        r.main(["--dir", str(box), "--include-dead", "--exclude-dead"])


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
