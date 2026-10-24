"""migrate-overlay-dead（存量硬字幕/水印判死片迁成节点级记录）的离线用例。

重点锁死：
  - 试运行一个字节都不改；
  - 人工 reject 的、还有其它判死原因的、没有别的版本的片绝不放回；
  - 推断节点：日志优先，mp4 按同 provider 整组记；
  - --apply 可安全重跑（中途断了再跑不重复写、不漏移）；主流程在跑时拒绝。
"""

import json
import os

import pytest

import download_movies as d


PALEO = "sun.paleoak.top"
HAKUNA = "bcdn.hakunaymatata.com"


def mp4(tid, name="a"):
    return {"url": f"https://{HAKUNA}/resource/{tid}{name}.mp4?sign=s&t=1",
            "provider": "vidlink", "type": "mp4"}


def hls(tid, provider="videasy", host=PALEO):
    return {"url": f"https://{host}/r6/{tid}-{provider}/master.m3u8",
            "provider": provider, "type": "m3u8"}


def key(item):
    return d.overlay_node_key(d._normalize_url_entry(item))


def _write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _dead(tid, reason_class, **extra):
    return {"tmdbId": tid, "title": f"t{tid}", "reason_class": reason_class,
            "reasons": [reason_class], **extra}


def _build(base):
    """一套覆盖各分支的生产数据。"""
    _write_jsonl(os.path.join(base, "download_dead.jsonl"), [
        _dead("101", "hardsub"),                      # 日志说下的是第 2 个（HLS）
        _dead("102", "watermark"),                    # 无日志 → 第 1 个（mp4，整组）
        _dead("103", "hardsub"),                      # 只有一个版本 → 保持判死
        _dead("104", "hardsub", manual=True),         # 人工 reject → 不动
        _dead("105", "quality"), _dead("105", "hardsub"),   # 还有画质判死 → 不动
        _dead("106", "hardsub"),                      # 找不到节点列表 → 不动
        _dead("107", "quality"),                      # 不是叠加物判死 → 不在范围
        _dead("108", "hardsub"),                      # 节点列表在 archive/ 里
    ])
    _write_jsonl(os.path.join(base, "failed.jsonl"), [
        {"tmdbId": "101", "stage": "hardsub", "urls": [mp4("101"), hls("101")]},
        {"tmdbId": "102", "stage": "watermark",
         "urls": [mp4("102", "a"), mp4("102", "b"), hls("102")]},
        {"tmdbId": "103", "stage": "hardsub", "urls": [mp4("103")]},
        {"tmdbId": "104", "stage": "hardsub", "urls": [mp4("104"), hls("104")]},
        {"tmdbId": "105", "stage": "hardsub", "urls": [mp4("105"), hls("105")]},
        {"tmdbId": "999", "stage": "download", "urls": [mp4("999")]},
    ])
    _write_jsonl(os.path.join(base, "archive", "failed_20261001_000000.jsonl"), [
        {"tmdbId": "108", "stage": "hardsub", "urls": [hls("108")]},
    ])
    _write_jsonl(os.path.join(base, "results.jsonl"), [
        {"tmdbId": "101", "fetched_at": 10, "urls": [mp4("101"), hls("101")]},
        {"tmdbId": "102", "fetched_at": 10,
         "urls": [mp4("102", "a"), mp4("102", "b"), hls("102")]},
        {"tmdbId": "103", "fetched_at": 10, "urls": [mp4("103")]},
        # 108：较新的取流结果多了一家 vidup（zenoak）。
        {"tmdbId": "108", "fetched_at": 5, "urls": [hls("108")]},
        {"tmdbId": "108", "fetched_at": 20,
         "urls": [hls("108"), hls("108", provider="vidup", host="moon.zenoak.top")]},
    ])
    with open(os.path.join(base, "pipeline.log"), "w", encoding="utf-8") as fh:
        fh.write("\n".join([
            "开始处理: 101 - t101",
            "开始处理: 555 - other",
            "  [101] 节点 1 失败，尝试下一个: 502",
            "  [101] 切换备用节点 2/2 (videasy/m3u8)",
            "  [555] 切换备用节点 3/3 (vidup/m3u8)",
            "  [101] 开始转封装为 MP4...",
            "  [101] 检测到硬字幕（字幕帧占比 0.4，不同文本 20 条） → 删除成品并判死",
        ]) + "\n")


@pytest.fixture
def prod(tmp_path, monkeypatch):
    base = str(tmp_path / "prod")
    _build(base)
    monkeypatch.setattr(d, "_SCRIPT_DIR", tmp_path / "prod")
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", os.path.join(base, "download_dead.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", os.path.join(base, "failed.jsonl"))
    monkeypatch.setattr(d, "INPUT_JSONL", os.path.join(base, "results.jsonl"))
    monkeypatch.setattr(d, "OVERLAY_NODE_REJECT_LOG",
                        os.path.join(base, "overlay_node_rejects.jsonl"))
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    return base


def _snapshot(base):
    out = {}
    for root, _dirs, files in os.walk(base):
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as fh:
                out[os.path.relpath(path, base)] = fh.read()
    return out


def test_dry_run_classifies_and_changes_nothing(prod, tmp_path):
    before = _snapshot(prod)
    report = str(tmp_path / "report.jsonl")

    counts = d.migrate_overlay_dead(report_path=report)

    assert _snapshot(prod) == before, "试运行不能改任何文件"
    assert counts == {
        "migrate": 3, "migrate_inferred_from_log": 1, "migrate_inferred_from_first_node": 2,
        "stay_dead_no_other_version": 1, "skip_manual": 1,
        "skip_other_dead_reason": 1, "skip_no_node_list": 1,
    }
    rows = {r["tmdbId"]: r for r in _read_jsonl(report)}
    assert "107" not in rows
    assert rows["101"]["killed"] == [key(hls("101"))]          # 日志：第 2 个
    assert rows["101"]["remaining"] == [f"vidlink/mp4/{HAKUNA}"]
    assert rows["102"]["killed"] == sorted([key(mp4("102", "a")), key(mp4("102", "b"))])
    assert rows["102"]["remaining"] == [f"videasy/m3u8/{PALEO}"]
    assert rows["108"]["remaining"] == ["vidup/m3u8/moon.zenoak.top"]


def test_apply_moves_dead_records_and_writes_node_rejects(prod):
    d.migrate_overlay_dead(apply=True)

    assert d.load_dead_ids() == {"103", "104", "105", "106", "107"}
    archive = os.path.join(prod, "archive")
    moved = [n for n in os.listdir(archive) if n.startswith("download_dead_overlay_migrated_")]
    assert len(moved) == 1
    assert sorted(r["tmdbId"] for r in _read_jsonl(os.path.join(archive, moved[0]))) == [
        "101", "102", "108"]
    assert any(n.startswith("download_dead.jsonl.bak_") for n in os.listdir(prod))
    rejects = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    assert all(r["migrated"] for r in rejects)
    assert sorted((r["tmdbId"], r["inferred_from"]) for r in rejects) == [
        ("101", "log"), ("102", "first_node"), ("102", "first_node"), ("108", "first_node")]
    # 放回后的派发：已检出版本被滤掉，只下剩下的。
    entry = {"tmdbId": "102", "urls": [mp4("102", "a"), mp4("102", "b"), hls("102")]}
    assert d.strip_rejected_nodes(entry)[0]["urls"] == [hls("102")]


def test_apply_twice_is_a_no_op(prod, capsys):
    d.migrate_overlay_dead(apply=True)
    rejects = _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)
    capsys.readouterr()
    d.migrate_overlay_dead(apply=True)
    assert "没有需要迁移的片" in capsys.readouterr().out
    assert _read_jsonl(d.OVERLAY_NODE_REJECT_LOG) == rejects


def test_apply_resumes_after_interruption_without_duplicates(prod):
    """上次写完节点级记录、还没重写判死台账就断了：再跑补完，不重复写节点记录。"""
    d.record_overlay_node_reject("101", "t101", d._normalize_url_entry(hls("101")),
                                 {"reasons": ["hardsub"]})
    d.migrate_overlay_dead(apply=True)
    assert [r["tmdbId"] for r in _read_jsonl(d.OVERLAY_NODE_REJECT_LOG)].count("101") == 1
    assert "101" not in d.load_dead_ids()


def test_apply_refused_while_main_process_runs(prod, monkeypatch, capsys):
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    before = _snapshot(prod)
    d.migrate_overlay_dead(apply=True)
    assert _snapshot(prod) == before
    assert "主流程正在运行" in capsys.readouterr().out


def test_data_dir_reads_another_directory_and_forbids_apply(prod, tmp_path, monkeypatch):
    """服务器试运行：新代码在临时目录，--data-dir 指向生产目录只读。"""
    monkeypatch.setattr(d, "_SCRIPT_DIR", tmp_path / "scratch")
    for name in ("DOWNLOAD_DEAD_LOG", "FAILED_LOG", "INPUT_JSONL", "OVERLAY_NODE_REJECT_LOG"):
        monkeypatch.setattr(d, name, str(tmp_path / "scratch" / os.path.basename(getattr(d, name))))
    before = _snapshot(prod)
    counts = d.migrate_overlay_dead(data_dir=prod)
    assert counts["migrate"] == 3
    assert _snapshot(prod) == before
    with pytest.raises(SystemExit):
        d.migrate_overlay_dead(apply=True, data_dir=prod)


def test_log_index_tracks_each_film_separately(tmp_path):
    log = tmp_path / "pipeline.log"
    log.write_text("\n".join([
        "开始处理: 1 - a", "开始处理: 2 - b",
        "  [1] 切换备用节点 3/3 (vidup/m3u8)",
        "  [2] 检测到水印（文字「x」） → 删除成品并判死",
        "  [1] 检测到硬字幕（…） → 删除成品并判死",
        "开始处理: 1 - a",                                # 之后再处理不影响已记下的判死
    ]) + "\n", encoding="utf-8")
    assert d._overlay_kill_node_index_from_log(str(log)) == {"1": 3, "2": 1}
    assert d._overlay_kill_node_index_from_log(str(tmp_path / "missing.log")) == {}
