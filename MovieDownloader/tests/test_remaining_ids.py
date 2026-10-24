"""remaining_ids.py（从 ids.txt 去掉已进 R2 的 id，得到 ids_remaining.txt）的离线用例。

重点锁死：
  - 只有「success.jsonl 里 uploaded 为 true」才算完成；其余不论原因都留在名单里；
  - --exclude-dead 只排除判死的片，--verify-r2 以 R2 实际对象纠正台账；
  - 只读：除输出文件外一个字节都不改；输出保持 ids.txt 原顺序；
  - 文件位置按该目录 config.yaml 解析（各机器配置可能不同）。
"""

import json
import os

import pytest

import remaining_ids as r


def _jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                    encoding="utf-8")


@pytest.fixture
def machine(tmp_path):
    """一台机器的工作目录：每个 id 落在一种情况上。

      1  uploaded              2  uploaded=false、本地有成品
      3  判死画质              4  判死硬字幕        5  人工 reject 判死
      6  有链接、下载失败过（失败记录在 archive/）  7  有链接、还没下
      8  确认无源               9  取流多轮瞬时失败   10 还没取流
      11 uploaded（大小 1000） 12 只有本地成品（没 success 记录）
    """
    base = tmp_path / "MovieDownloader"
    base.mkdir()
    (base / "ids.txt").write_text("\n".join(
        ["10", "1", "2", "3", "4", "5", "6", "7", "8", "9", "11", "12", "1", "", "# note"]
    ) + "\n", encoding="utf-8")
    _jsonl(base / "success.jsonl", [
        {"tmdbId": "1", "uploaded": True, "file_size_bytes": 500},
        {"tmdbId": 2, "uploaded": False},
        {"tmdbId": "11", "uploaded": True, "file_size_bytes": 1000},
    ])
    with open(base / "success.jsonl", "a", encoding="utf-8") as fh:
        fh.write("{broken line\n")
    _jsonl(base / "download_dead.jsonl", [
        {"tmdbId": "3", "reason_class": "quality"},
        {"tmdbId": "4", "reason_class": "hardsub"},
        {"tmdbId": "5", "reason_class": "quality", "manual": True},
    ])
    _jsonl(base / "results.jsonl", [{"tmdbId": tid, "urls": []} for tid in ("1", "2", "6", "7")])
    _jsonl(base / "archive" / "failed_20261020_000000.jsonl", [{"tmdbId": "6", "stage": "download"}])
    _jsonl(base / "failed.jsonl", [{"tmdbId": "99", "stage": "download"}])
    (base / "fail.txt").write_text("8\n", encoding="utf-8")
    (base / "unresolved.txt").write_text("9\n", encoding="utf-8")
    for tid in ("2", "12"):
        film = base / "downloads" / "movies" / "2020" / tid
        film.mkdir(parents=True)
        (film / f"{tid}.mp4").write_bytes(b"x")
    return base


def _snapshot(base):
    out = {}
    for root, _dirs, files in os.walk(base):
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as fh:
                out[os.path.relpath(path, base)] = fh.read()
    return out


def _lines(path):
    return path.read_text(encoding="utf-8").split()


def test_only_uploaded_ids_are_removed_and_order_is_kept(machine):
    summary = r.run(machine)
    assert _lines(machine / "ids_remaining.txt") == [
        "10", "2", "3", "4", "5", "6", "7", "8", "9", "12"]
    assert summary["total"] == 12 and summary["duplicates"] == 1
    assert summary["done"] == {"uploaded": 2}
    assert summary["remaining"] == {
        "not_fetched": 1, "downloaded_not_uploaded": 2, "dead:quality": 1, "dead:hardsub": 1,
        "dead:manual": 1, "download_failed": 1, "links_not_downloaded": 1, "no_source": 1,
        "fetch_unresolved": 1,
    }
    assert summary["bad_lines"] == {"success.jsonl": 1}


def test_exclude_dead_drops_only_dead_films(machine):
    summary = r.run(machine, exclude_dead=True)
    assert _lines(machine / "ids_remaining.txt") == ["10", "2", "6", "7", "8", "9", "12"]
    assert summary["excluded"] == {"dead:quality": 1, "dead:hardsub": 1, "dead:manual": 1}


def test_report_gives_a_reason_per_film(machine):
    r.run(machine, report="remaining_report.jsonl", exclude_dead=True)
    rows = {row["tmdbId"]: row for row in map(
        json.loads, (machine / "remaining_report.jsonl").read_text().splitlines())}
    assert set(rows) == {"10", "2", "3", "4", "5", "6", "7", "8", "9", "12"}
    assert rows["5"] == {"tmdbId": "5", "reason": "dead:manual", "in_remaining": False,
                         "dead_classes": ["manual"], "has_links": False, "local_mp4": False,
                         "in_r2": None}
    assert rows["6"]["reason"] == "download_failed" and rows["6"]["has_links"] is True
    assert rows["12"]["reason"] == "downloaded_not_uploaded" and rows["12"]["local_mp4"] is True


class FakeR2:
    def __init__(self, objects):
        self.objects = objects
        self.prefixes = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        self.prefixes.append((Bucket, Prefix))
        keys = sorted(self.objects)
        for start in range(0, len(keys), 2):          # 分页
            yield {"Contents": [{"Key": k, "Size": self.objects[k]} for k in keys[start:start + 2]]}


def test_verify_r2_corrects_the_ledger(machine):
    fake = FakeR2({
        "movies/2020/1/1.mp4": 999,          # 台账 500 → 大小不符
        "movies/2020/1/meta.json": 10,
        "movies/2021/7/7.mp4": 123,          # 台账没记，R2 上已有 → 算完成
        "movies/2021/7/subs/7.en.srt": 5,
        "movies/2019/88/88.mp4": 1,          # 不在本机名单里
    })                                       # 11 台账记了已上传，R2 上没有
    summary = r.run(machine, verify_r2=True, r2_client=(fake, "bucket"))
    assert fake.prefixes == [("bucket", "movies/")]
    assert summary["done"] == {"r2_only": 1}
    assert summary["remaining"]["r2_size_mismatch"] == 1 and summary["remaining"]["r2_missing"] == 1
    remaining = _lines(machine / "ids_remaining.txt")
    assert "7" not in remaining and "1" in remaining and "11" in remaining
    assert summary["r2_listed"] == 3


def test_verify_r2_uses_configured_prefix(machine):
    (machine / "config.yaml").write_text(
        "download_movies:\n  folder_prefix: films\n  s3:\n    prefix: root/\n", encoding="utf-8")
    fake = FakeR2({"root/films/2020/1/1.mp4": 500, "root/films/2020/11/11.mp4": 1000})
    summary = r.run(machine, verify_r2=True, r2_client=(fake, "b"))
    assert fake.prefixes == [("b", "root/films/")]
    assert summary["done"] == {"uploaded": 2}


def test_reads_only_and_writes_just_the_outputs(machine):
    before = _snapshot(machine)
    r.run(machine, report="remaining_report.jsonl")
    after = _snapshot(machine)
    assert set(after) - set(before) == {"ids_remaining.txt", "remaining_report.jsonl"}
    assert {k: v for k, v in after.items() if k in before} == before


def test_refuses_to_overwrite_the_input(machine):
    with pytest.raises(SystemExit):
        r.run(machine, out="ids.txt")
    with pytest.raises(SystemExit):
        r.run(machine, report="ids_remaining.txt")
    with pytest.raises(SystemExit):
        r.run(machine, ids="missing.txt")


def test_paths_follow_the_machines_config(machine):
    """config.yaml 换了文件名：按配置找，不按默认名。"""
    os.replace(machine / "success.jsonl", machine / "ok_log.jsonl")
    os.replace(machine / "fail.txt", machine / "nosource.txt")
    (machine / "config.yaml").write_text(
        "download_movies:\n  success_log: ok_log.jsonl\n"
        "tmdb_ids_to_links:\n  fail_file: nosource.txt\n", encoding="utf-8")
    summary = r.run(machine)
    assert summary["done"] == {"uploaded": 2}
    assert summary["remaining"]["no_source"] == 1


def test_custom_ids_file_and_missing_ledgers(tmp_path):
    """全新目录（台账都还没有）：名单原样输出，全部算还没取流。"""
    (tmp_path / "subset.txt").write_text("5\n6\n", encoding="utf-8")
    summary = r.run(tmp_path, ids="subset.txt", out="left.txt")
    assert _lines(tmp_path / "left.txt") == ["5", "6"]
    assert summary["remaining"] == {"not_fetched": 2}


def test_warns_when_main_process_is_running(machine, capsys):
    (machine / "download_movies.main.lock").write_text(str(os.getpid()))
    r.run(machine)
    assert "主流程正在运行" in capsys.readouterr().out
    assert (machine / "download_movies.main.lock").exists(), "锁文件不能被动"


def test_cli_entry(machine, capsys):
    assert r.main(["--dir", str(machine), "--exclude-dead", "--report", "rep.jsonl"]) == 0
    out = capsys.readouterr().out
    assert "已完成（已进 R2）2 个" in out and "按 --exclude-dead 排除判死的 3 个" in out
    assert (machine / "rep.jsonl").exists()


def test_provider_down_is_its_own_reason(machine):
    """因取流源熔断推迟的片单独成类（排在 unresolved 前面），每行第二列的源名不影响读取。"""
    (machine / "provider_down.txt").write_text("9\tvideasy\n10\tvideasy,vidlink\n", encoding="utf-8")
    summary = r.run(machine)
    assert summary["remaining"]["provider_down"] == 2
    assert "fetch_unresolved" not in summary["remaining"]
    assert "not_fetched" not in summary["remaining"]
    assert {"9", "10"} <= set(_lines(machine / "ids_remaining.txt"))
