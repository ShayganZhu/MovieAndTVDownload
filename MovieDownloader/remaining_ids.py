#!/usr/bin/env python3
"""从本机 ids.txt 里去掉已成功下载并上传 R2 的 id，得到 ids_remaining.txt（2026-10-24）。

用途：一台机器跑完（或跑到一半停下）后，拿到「这台机器还没成功进 R2 的 id」，
之后在同一台机器上补跑一遍，提高成功率。

判定口径（与 download_movies.load_uploaded_ids 一致）：
  - 「已完成」= success.jsonl 里该 id 有一条 uploaded 为 true 的记录。
    人工 reject 会删 R2 并把记录从 success.jsonl 移除，所以被 reject 的片自动回到名单里；
    success.jsonl 不轮转，不用翻 archive/。
  - --verify-r2：再把 R2 上 {prefix}/{folder_prefix}/ 下的对象列一遍核对——台账说已上传
    但 R2 上没有 mp4、或大小与台账不符的，算未完成；R2 上有 mp4 但台账没记的，算已完成
    （另行列出）。
  - 其余一律算未完成（不论原因）；--exclude-dead 才把判死的片（画质 / 硬字幕 / 水印 /
    人工）排除——它们重下多半还是同一个结论。

只读：除 --out / --report 指定的输出文件外，不改任何文件（陈旧的主流程锁也不清）。
不依赖 download_movies.py（各机器跑的代码版本不同），用该机器现有的 .venv 运行即可：

    .venv/bin/python remaining_ids.py --dir /mnt/MovieAndTVDownload/MovieDownloader \\
        --verify-r2 --report remaining_report.jsonl

文件位置按 --dir 下 config.yaml 的设置解析（相对路径以 --dir 为根），与主流程一致。
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

# 未完成原因（--report 的 reason 字段与汇总表都用这些）。顺序 = 汇总表的显示顺序。
REASONS = {
    "r2_missing": "台账记了已上传，R2 上却没有 mp4",
    "r2_size_mismatch": "R2 上的 mp4 大小与台账不符",
    "dead:quality": "判死：画质不达标",
    "dead:hardsub": "判死：硬字幕",
    "dead:watermark": "判死：水印",
    "dead:manual": "判死：人工 reject",
    "dead:other": "判死：其它原因",
    "downloaded_not_uploaded": "已下载、未上传（本地有成品或台账 uploaded=false）",
    "download_failed": "取到过链接，下载失败",
    "links_not_downloaded": "取到过链接，还没下载过",
    "no_source": "确认无源（fail.txt）",
    "fetch_unresolved": "取流多轮仍是瞬时错误（unresolved.txt）",
    "not_fetched": "还没取到链接（没轮到 / 取流瞬时失败）",
}
_TMDB_ID_RE = re.compile(r'"tmdbId"\s*:\s*"?(\d+)')
_R2_MP4_RE = re.compile(r"(?:^|/)(\d+)/(\d+)\.mp4$")


# ---------------------------------------------------------------- 路径与配置

def _load_config(base_dir):
    path = base_dir / "config.yaml"
    if not path.exists():
        return {}
    import yaml

    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _resolve(base_dir, value, default_name):
    raw = value.strip() if isinstance(value, str) else value
    return (base_dir / (raw or default_name)).resolve()


def build_paths(base_dir, ids=None, config=None):
    """按 config.yaml（与主流程同一套默认值）解析出要读的文件。"""
    config = _load_config(base_dir) if config is None else config
    dl = config.get("download_movies") or {}
    fetch = config.get("tmdb_ids_to_links") or {}
    s3 = dl.get("s3") or {}
    folder = (dl.get("folder_prefix") or "movies").strip().strip("/")
    return {
        "ids": (base_dir / ids).resolve() if ids else _resolve(base_dir, fetch.get("input"), "ids.txt"),
        "results": _resolve(base_dir, fetch.get("output"), "results.jsonl"),
        "fail": _resolve(base_dir, fetch.get("fail_file"), "fail.txt"),
        "unresolved": _resolve(base_dir, fetch.get("unresolved_file"), "unresolved.txt"),
        "success": _resolve(base_dir, dl.get("success_log"), "success.jsonl"),
        "dead": _resolve(base_dir, dl.get("download_dead_log"), "download_dead.jsonl"),
        "failed": _resolve(base_dir, dl.get("failed_log"), "failed.jsonl"),
        "archive": (base_dir / "archive").resolve(),
        "downloads": _resolve(base_dir, dl.get("base_dir"), "downloads") / folder,
        "lock": (base_dir / "download_movies.main.lock").resolve(),
        "folder_prefix": folder,
        "s3_prefix": (s3.get("prefix") or "").strip("/"),
        "s3_config": s3,
    }


# ---------------------------------------------------------------- 读台账

def _norm(value):
    return str(value).strip() if value is not None else ""


def read_ids(path):
    """ids.txt：一行一个 id，保留原顺序、去重，跳过空行与 # 注释。"""
    seen, ordered, duplicates = set(), [], 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            tid = line.strip()
            if not tid or tid.startswith("#"):
                continue
            if tid in seen:
                duplicates += 1
                continue
            seen.add(tid)
            ordered.append(tid)
    return ordered, duplicates


def _iter_jsonl(path, bad):
    if not path.exists():
        return
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                bad[path.name] += 1
                continue
            if isinstance(record, dict):
                yield record


def _ids_by_regex(paths):
    """大文件（results / failed）只要 id：按行正则抽，不整行解析 JSON。"""
    found = set()
    for path in paths:
        if not path.exists():
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                match = _TMDB_ID_RE.search(line)
                if match:
                    found.add(match.group(1))
    return found


def _ids_from_lines(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as fh:
        return {line.strip() for line in fh if line.strip()}


def _local_mp4_ids(downloads):
    """本地成品：{downloads}/{year}/{tmdbId}/{tmdbId}.mp4。"""
    found = set()
    if not downloads.is_dir():
        return found
    for year in os.scandir(downloads):
        if not year.is_dir():
            continue
        for film in os.scandir(year.path):
            if film.is_dir() and os.path.isfile(os.path.join(film.path, f"{film.name}.mp4")):
                found.add(film.name)
    return found


def load_ledgers(paths):
    bad = Counter()
    uploaded = {}            # id -> 台账里的成品大小（字节，可能为 None）
    downloaded = set()       # 有 success 记录（不论是否上传）
    for record in _iter_jsonl(paths["success"], bad):
        tid = _norm(record.get("tmdbId"))
        if not tid:
            continue
        downloaded.add(tid)
        if record.get("uploaded") is True:
            uploaded[tid] = record.get("file_size_bytes") or uploaded.get(tid)
    dead = {}                # id -> 判死类目集合
    for record in _iter_jsonl(paths["dead"], bad):
        tid = _norm(record.get("tmdbId"))
        if not tid:
            continue
        kind = "manual" if record.get("manual") else (record.get("reason_class") or "other")
        dead.setdefault(tid, set()).add(kind if f"dead:{kind}" in REASONS else "other")
    failed_paths = [paths["failed"]]
    if paths["archive"].is_dir():
        failed_paths += sorted(paths["archive"].glob("failed_*.jsonl"))
    return {
        "uploaded": uploaded,
        "downloaded": downloaded,
        "dead": dead,
        "results": _ids_by_regex([paths["results"]]),
        "failed": _ids_by_regex(failed_paths),
        "fail": _ids_from_lines(paths["fail"]),
        "unresolved": _ids_from_lines(paths["unresolved"]),
        "local_mp4": _local_mp4_ids(paths["downloads"]),
        "bad_lines": bad,
    }


# ---------------------------------------------------------------- R2

def _load_dotenv(path):
    """与主流程同样的轻量 .env 解析：不覆盖已有环境变量。"""
    if not path.exists():
        return
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip().strip('"').strip("'")


def make_r2_client(base_dir, s3_config):
    _load_dotenv(base_dir / ".env")

    def secret(cfg_key, env_key):
        return (os.environ.get(env_key, "").strip()
                or (s3_config.get(cfg_key) or "").strip())

    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=secret("endpoint_url", "R2_ENDPOINT_URL"),
        region_name=s3_config.get("region") or "auto",
        aws_access_key_id=secret("access_key", "R2_ACCESS_KEY"),
        aws_secret_access_key=secret("secret_key", "R2_SECRET_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"},
                      connect_timeout=15, read_timeout=60),
    )
    return client, secret("bucket", "R2_BUCKET")


def list_r2_mp4(client, bucket, prefix):
    """列出 prefix 下所有 {tmdbId}/{tmdbId}.mp4，返回 {tmdbId: 大小}。只读（ListObjectsV2）。"""
    found = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            match = _R2_MP4_RE.search(obj.get("Key", ""))
            if match and match.group(1) == match.group(2):
                found[match.group(1)] = obj.get("Size")
    return found


# ---------------------------------------------------------------- 判定

def classify(tid, ledgers, r2=None):
    """返回 (是否已完成, 原因)。已完成的原因是 uploaded / r2_only。"""
    size = ledgers["uploaded"].get(tid)
    on_ledger = tid in ledgers["uploaded"]
    if r2 is None:
        if on_ledger:
            return True, "uploaded"
    else:
        in_r2 = tid in r2
        if on_ledger and not in_r2:
            return False, "r2_missing"
        if on_ledger and size and r2.get(tid) is not None and int(r2[tid]) != int(size):
            return False, "r2_size_mismatch"
        if on_ledger:
            return True, "uploaded"
        if in_r2:
            return True, "r2_only"
    if tid in ledgers["dead"]:
        kinds = ledgers["dead"][tid]
        for kind in ("manual", "quality", "hardsub", "watermark"):
            if kind in kinds:
                return False, f"dead:{kind}"
        return False, "dead:other"
    if tid in ledgers["downloaded"] or tid in ledgers["local_mp4"]:
        return False, "downloaded_not_uploaded"
    if tid in ledgers["results"]:
        return False, "download_failed" if tid in ledgers["failed"] else "links_not_downloaded"
    if tid in ledgers["fail"]:
        return False, "no_source"
    if tid in ledgers["unresolved"]:
        return False, "fetch_unresolved"
    return False, "not_fetched"


def _write_atomic(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line + "\n")
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def _main_running(lock_path):
    try:
        pid = int(lock_path.read_text().strip())
        os.kill(pid, 0)
        return pid
    except (OSError, ValueError):
        return None


def run(base_dir, ids=None, out="ids_remaining.txt", report=None, verify_r2=False,
        exclude_dead=False, r2_client=None, config=None):
    """主逻辑（命令行与测试共用）。返回汇总字典。"""
    base_dir = Path(base_dir).resolve()
    paths = build_paths(base_dir, ids=ids, config=config)
    out_path = (base_dir / out).resolve()
    if out_path == paths["ids"]:
        raise SystemExit(f"--out 不能指向输入文件本身：{out_path}")
    report_path = (base_dir / report).resolve() if report else None
    if report_path in (paths["ids"], out_path):
        raise SystemExit(f"--report 不能指向输入或输出文件：{report_path}")
    if not paths["ids"].exists():
        raise SystemExit(f"找不到 id 名单：{paths['ids']}")

    pid = _main_running(paths["lock"])
    if pid:
        print(f"⚠️ 主流程正在运行（PID {pid}）：结果只是此刻的快照，之后才上传完的片也会留在名单里。",
              flush=True)
    started = time.time()
    id_list, duplicates = read_ids(paths["ids"])
    ledgers = load_ledgers(paths)

    r2 = None
    r2_note = ""
    if verify_r2:
        client, bucket = r2_client if r2_client is not None else make_r2_client(
            base_dir, paths["s3_config"])
        prefix = f"{paths['s3_prefix']}/{paths['folder_prefix']}/" if paths["s3_prefix"] \
            else f"{paths['folder_prefix']}/"
        t0 = time.time()
        r2 = list_r2_mp4(client, bucket, prefix)
        r2_note = f"R2 {bucket}/{prefix} 下共 {len(r2)} 部 mp4（列举 {time.time() - t0:.1f}s）"

    done, remaining, excluded, rows = Counter(), Counter(), Counter(), []
    keep = []
    for tid in id_list:
        finished, reason = classify(tid, ledgers, r2)
        if finished:
            done[reason] += 1
            continue
        dropped = exclude_dead and reason.startswith("dead:")
        (excluded if dropped else remaining)[reason] += 1
        if not dropped:
            keep.append(tid)
        if report_path:
            rows.append(json.dumps({
                "tmdbId": tid, "reason": reason, "in_remaining": not dropped,
                "dead_classes": sorted(ledgers["dead"].get(tid, ())),
                "has_links": tid in ledgers["results"],
                "local_mp4": tid in ledgers["local_mp4"],
                "in_r2": None if r2 is None else tid in r2,
            }, ensure_ascii=False))

    _write_atomic(out_path, keep)
    if report_path:
        _write_atomic(report_path, rows)

    summary = {
        "total": len(id_list), "duplicates": duplicates, "done": dict(done),
        "remaining": dict(remaining), "excluded": dict(excluded), "written": len(keep),
        "bad_lines": dict(ledgers["bad_lines"]), "out": str(out_path),
        "report": str(report_path) if report_path else None, "r2_listed": None if r2 is None else len(r2),
    }
    _print_summary(summary, r2_note, time.time() - started, exclude_dead)
    return summary


def _print_summary(s, r2_note, seconds, exclude_dead):
    total = s["total"] or 1
    print(f"\nid 名单 {s['total']} 个" + (f"（另有重复 {s['duplicates']} 个已去掉）" if s["duplicates"] else ""))
    if r2_note:
        print(r2_note)
    done = sum(s["done"].values())
    print(f"已完成（已进 R2）{done} 个（{done / total:.1%}）"
          + (f"，其中台账没记、R2 上已有 {s['done']['r2_only']} 个" if s["done"].get("r2_only") else ""))
    print("未完成：")
    for reason, label in REASONS.items():
        kept, dropped = s["remaining"].get(reason, 0), s["excluded"].get(reason, 0)
        if kept or dropped:
            tail = "（已按 --exclude-dead 排除）" if dropped else ""
            print(f"  {kept + dropped:6d}  {label}{tail}")
    if exclude_dead:
        print(f"按 --exclude-dead 排除判死的 {sum(s['excluded'].values())} 个")
    print(f"写入 {s['out']}：{s['written']} 个 id" + (f"；明细 {s['report']}" if s["report"] else ""))
    if s["bad_lines"]:
        print("⚠️ 跳过的坏行：" + "，".join(f"{k} {v} 行" for k, v in s["bad_lines"].items()))
    print(f"用时 {seconds:.1f}s")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="从 ids.txt 去掉已成功下载并上传 R2 的 id，写出 ids_remaining.txt（只读）")
    parser.add_argument("--dir", default=str(Path(__file__).resolve().parent),
                        help="该机器的 MovieDownloader 工作目录（默认本脚本所在目录）")
    parser.add_argument("--ids", default=None,
                        help="id 名单（默认按 config.yaml 的 tmdb_ids_to_links.input，即 ids.txt）")
    parser.add_argument("--out", default="ids_remaining.txt", help="输出文件（相对 --dir）")
    parser.add_argument("--report", default=None, help="逐部明细 jsonl（相对 --dir，可选）")
    parser.add_argument("--verify-r2", action="store_true", help="列一遍 R2 核对（需 .env 里的 R2 凭证）")
    parser.add_argument("--exclude-dead", action="store_true",
                        help="排除判死的片（画质 / 硬字幕 / 水印 / 人工 reject）")
    args = parser.parse_args(argv)
    run(args.dir, ids=args.ids, out=args.out, report=args.report,
        verify_r2=args.verify_r2, exclude_dead=args.exclude_dead)
    return 0


if __name__ == "__main__":
    sys.exit(main())
