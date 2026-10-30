#!/usr/bin/env python3
"""逐集核对本机还有哪些集没进 R2，写出要补跑的剧 id 名单 ids_remaining.txt（电影侧 28be976 移植，2026-10-30）。

用途：一台机器跑完（或跑到一半停下）后，拿到「这台机器还有哪些剧没下完」，之后在同一台
机器上用它补跑一遍；--verify-r2 顺带核对 R2 与台账是否一致（每轮跑之前的体检）。

与电影侧的差异：TV 的统计单位是**集**，补跑的输入却是**剧**（ids.txt 一行一部剧，
主流程自己按季集展开、跳过已完成的集）。所以逐集分类、按剧汇总：一部剧还有一集**补跑能推进**
的未完成集，这部剧就进 ids_remaining.txt。

只剩无源 / 判死集的剧默认不进名单（2026-10-30 定）：正常运行把这些集当已处理跳过（取流侧跳过
fail.txt 的无源集与整剧无源，下载侧跳过判死集），补跑一遍什么也不会发生；而 TV 集级有源率只有
个位数百分比，几乎每部剧都有无源集，不排除的话名单约等于整个 ids.txt。它们照常计数、照常进
--report 明细，由专用模式处理：无源集 `tv_ids_to_links.py --recheck-dead`、画质判死
`download_tv.py --retry-dead`（两者自己读台账，用完整的 ids.txt 跑）。--include-dead 恢复旧口径。

集的全集取自取流侧的季集缓存（seasons_cache.jsonl）：
  - 尚未播出的集（air_date + air_grace_days 在今天之后、或未完结剧的 TBA 集）不算未完成，
    单独计数——与取流侧的跳过口径一致；
  - include_specials 为 false 时不算第 0 季；
  - 缓存里没有的剧按剧报告：fail.txt 里整剧无源（tid\t-\t-）记「整剧无源」，否则「季集未展开」。

判定口径（与 download_tv 一致）：
  - 「已完成」= success.jsonl 里这一集有一条 uploaded 为 true 的记录（人工 reject 会把记录删掉，
    被 reject 的集自动回到未完成里）；
  - --verify-r2：再把 R2 上 {prefix}/{folder_prefix}/ 下的 E{ee}.mp4 列一遍核对——台账说已上传但
    R2 上没有、或大小与台账不符的算未完成；R2 上有但台账没记的算已完成（另行列出）。

只读：除 --out / --report 指定的输出文件外，不改任何文件。--out / --report 只接受普通文件路径
（拒绝 /dev 下的设备文件：原子写会把临时文件 os.replace 到目标上，以 root 运行时会把设备换成普通文件）。
不依赖 download_tv.py，用该机器现有的 .venv 运行即可：

    .venv/bin/python remaining_ids.py --dir /mnt/MovieAndTVDownload/TVDownloader \\
        --verify-r2 --report remaining_report.jsonl
"""

import argparse
import json
import os
import re
import tempfile
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

# 未完成原因（--report 的 reason 字段与汇总表都用这些）。顺序 = 汇总表的显示顺序。
REASONS = {
    "r2_missing": "台账记了已上传，R2 上却没有 mp4",
    "r2_size_mismatch": "R2 上的 mp4 大小与台账不符",
    "dead:quality": "判死：画质不达标",
    "dead:hardsub": "判死：硬字幕",
    "dead:watermark": "判死：水印",
    "dead:truncated": "判死：时长不符",
    "dead:no_audio": "判死：无音轨",
    "dead:manual": "判死：人工 reject",
    "dead:other": "判死：其它原因",
    "downloaded_not_uploaded": "已下载、未上传（本地有成品或台账 uploaded=false）",
    "download_failed": "取到过链接，下载失败",
    "links_not_downloaded": "取到过链接，还没下载过",
    "no_source": "确认无源（fail.txt）",
    "provider_down": "有取流源熔断、其余源无此集，待它恢复后再查（provider_down.txt）",
    "fetch_unresolved": "取流多轮仍是瞬时错误（fail.txt 标 retry-exhausted）",
    "not_fetched": "还没取到链接（没轮到 / 取流瞬时失败）",
}
SHOW_REASONS = {
    "show:no_source": "整剧无源（fail.txt）",
    "show:not_expanded": "季集还没展开（seasons_cache 里没有）",
}
# 补跑不会推进的原因：正常运行把这些集 / 剧当已处理跳过，只有专用模式才会再碰。
# 默认不让它们把剧留在补跑名单里（--include-dead 才留）。
TERMINAL_REASONS = frozenset(
    [reason for reason in REASONS if reason.startswith("dead:")] + ["no_source", "show:no_source"]
)
# 汇总表末尾给的处理办法（只列有专用模式的原因）。
TERMINAL_HINTS = {
    "no_source": "源站后补上架的无源集：python tv_ids_to_links.py --recheck-dead",
    "dead:quality": "调松画质门槛后放回画质判死集：python download_tv.py --retry-dead",
}
RETRY_EXHAUSTED_TAG = "retry-exhausted"
_KEY_FIELDS_RE = {
    "tmdbId": re.compile(r'"tmdbId"\s*:\s*"?(\d+)'),
    "season": re.compile(r'"season"\s*:\s*"?(\d+)'),
    "episode": re.compile(r'"episode"\s*:\s*"?(\d+)'),
}
_R2_MP4_RE = re.compile(r"(?:^|/)(\d+)/S(\d+)/E(\d+)/E(\d+)\.mp4$")


def episode_key(tid, season, episode):
    return f"{tid}_S{int(season):02d}E{int(episode):02d}"


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
    dl = config.get("download_tv") or {}
    fetch = config.get("tv_ids_to_links") or {}
    s3 = dl.get("s3") or {}
    folder = (dl.get("folder_prefix") or "tvs").strip().strip("/")
    return {
        "ids": (base_dir / ids).resolve() if ids else _resolve(base_dir, fetch.get("input"), "ids.txt"),
        "seasons_cache": _resolve(base_dir, fetch.get("seasons_cache"), "seasons_cache.jsonl"),
        "results": _resolve(base_dir, fetch.get("output"), "results.jsonl"),
        "fail": _resolve(base_dir, fetch.get("fail_file"), "fail.txt"),
        "provider_down": _resolve(base_dir, fetch.get("provider_down_file"), "provider_down.txt"),
        "success": _resolve(base_dir, dl.get("success_log"), "success.jsonl"),
        "dead": _resolve(base_dir, dl.get("download_dead_log"), "download_dead.jsonl"),
        "failed": _resolve(base_dir, dl.get("failed_log"), "failed.jsonl"),
        "archive": (base_dir / "archive").resolve(),
        "downloads": _resolve(base_dir, dl.get("base_dir"), "downloads") / folder,
        "lock": (base_dir / "download_tv.main.lock").resolve(),
        "folder_prefix": folder,
        "s3_prefix": (s3.get("prefix") or "").strip("/"),
        "s3_config": s3,
        "include_specials": bool(fetch.get("include_specials", True)),
        "air_grace_days": int(fetch.get("air_grace_days", 5) or 0),
    }


# ---------------------------------------------------------------- 读台账

def _norm(value):
    return str(value).strip() if value is not None else ""


def read_ids(path):
    """ids.txt：一行一部剧，保留原顺序、去重，跳过空行与 # 注释。"""
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


def _record_key(record):
    tid, season, episode = _norm(record.get("tmdbId")), record.get("season"), record.get("episode")
    try:
        return episode_key(tid, season, episode) if tid else ""
    except (TypeError, ValueError):
        return ""


def _keys_by_regex(paths):
    """大文件（results / failed）只要集 key：按行正则抽三个字段，不整行解析 JSON。"""
    found = set()
    for path in paths:
        if not path.exists():
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                parts = [rx.search(line) for rx in _KEY_FIELDS_RE.values()]
                if all(parts):
                    found.add(episode_key(*(p.group(1) for p in parts)))
    return found


def _read_fail(path):
    """fail.txt：tid\\ts\\te（集无源）、tid\\t-\\t-（整剧无源）、第 4 列 retry-exhausted（只是重试耗尽）。"""
    episodes, shows, exhausted = set(), set(), set()
    if not path.exists():
        return episodes, shows, exhausted
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 3 or not cols[0].strip():
                continue
            tid, season, episode = cols[0].strip(), cols[1].strip(), cols[2].strip()
            if season == "-" or episode == "-":
                shows.add(tid)
                continue
            try:
                key = episode_key(tid, season, episode)
            except ValueError:
                continue
            (exhausted if len(cols) >= 4 and cols[3].strip() == RETRY_EXHAUSTED_TAG
             else episodes).add(key)
    return episodes, shows, exhausted - episodes


def _read_provider_down(path):
    found = set()
    if not path.exists():
        return found
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            cols = line.split("\t")
            if len(cols) >= 3:
                try:
                    found.add(episode_key(cols[0].strip(), cols[1], cols[2]))
                except ValueError:
                    continue
    return found


def _local_mp4_keys(downloads):
    """本地成品：{downloads}/{year}/{tid}/S{ss}/E{ee}/E{ee}.mp4。"""
    found = set()
    if not downloads.is_dir():
        return found
    for path in downloads.glob("*/*/S*/E*/E*.mp4"):
        match = _R2_MP4_RE.search(path.as_posix())
        if match and match.group(3) == match.group(4):
            found.add(episode_key(match.group(1), match.group(2), match.group(3)))
    return found


def load_ledgers(paths):
    bad = Counter()
    uploaded = {}            # 集 key -> 台账里的成品大小（字节，可能为 None）
    downloaded = set()       # 有 success 记录（不论是否上传）
    for record in _iter_jsonl(paths["success"], bad):
        key = _record_key(record)
        if not key:
            continue
        downloaded.add(key)
        if record.get("uploaded") is True:
            uploaded[key] = record.get("file_size_bytes") or uploaded.get(key)
    dead = {}                # 集 key -> 判死类目集合
    for record in _iter_jsonl(paths["dead"], bad):
        key = _record_key(record)
        if not key:
            continue
        kind = "manual" if record.get("manual") else (record.get("reason_class") or "quality")
        dead.setdefault(key, set()).add(kind if f"dead:{kind}" in REASONS else "other")
    failed_paths = [paths["failed"]]
    if paths["archive"].is_dir():
        failed_paths += sorted(paths["archive"].glob("failed_*.jsonl"))
    fail_eps, fail_shows, exhausted = _read_fail(paths["fail"])
    return {
        "uploaded": uploaded,
        "downloaded": downloaded,
        "dead": dead,
        "results": _keys_by_regex([paths["results"]]),
        "failed": _keys_by_regex(failed_paths),
        "fail": fail_eps,
        "fail_shows": fail_shows,
        "unresolved": exhausted,
        "provider_down": _read_provider_down(paths["provider_down"]),
        "local_mp4": _local_mp4_keys(paths["downloads"]),
        "bad_lines": bad,
    }


def _is_unaired(air_date, today, ended, grace_days):
    """与取流侧 _is_unaired 同口径：缺日期时已完结的剧算已播、仍在播的算 TBA 未播。"""
    if not air_date:
        return not ended
    try:
        return date.fromisoformat(str(air_date)[:10]) + timedelta(days=grace_days) > today
    except ValueError:
        return False


def load_episodes(paths, today=None):
    """seasons_cache.jsonl → {tid: [(season, episode, 是否未播), ...]}（同一部剧以最后一行为准）。"""
    today = today or date.today()
    shows = {}
    bad = Counter()
    for record in _iter_jsonl(paths["seasons_cache"], bad):
        tid = _norm(record.get("tmdbId"))
        if not tid:
            continue
        ended = bool(record.get("ended", True))
        eps = []
        for season in record.get("seasons") or []:
            number = season.get("season")
            if number is None or (number == 0 and not paths["include_specials"]):
                continue
            air_dates = season.get("air_dates") or {}
            for episode in season.get("episodes") or []:
                eps.append((int(number), int(episode),
                            _is_unaired(air_dates.get(str(episode)), today, ended,
                                        paths["air_grace_days"]) if "air_dates" in season else False))
        shows[tid] = eps
    return shows


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
    """列出 prefix 下所有 {tid}/S{ss}/E{ee}/E{ee}.mp4，返回 {集 key: 大小}。只读（ListObjectsV2）。"""
    found = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            match = _R2_MP4_RE.search(obj.get("Key", ""))
            if match and int(match.group(3)) == int(match.group(4)):
                found[episode_key(match.group(1), match.group(2), match.group(3))] = obj.get("Size")
    return found


# ---------------------------------------------------------------- 判定

def classify(key, ledgers, r2=None):
    """返回 (是否已完成, 原因)。已完成的原因是 uploaded / r2_only。"""
    size = ledgers["uploaded"].get(key)
    on_ledger = key in ledgers["uploaded"]
    if r2 is None:
        if on_ledger:
            return True, "uploaded"
    else:
        in_r2 = key in r2
        if on_ledger and not in_r2:
            return False, "r2_missing"
        if on_ledger and size and r2.get(key) is not None and int(r2[key]) != int(size):
            return False, "r2_size_mismatch"
        if on_ledger:
            return True, "uploaded"
        if in_r2:
            return True, "r2_only"
    if key in ledgers["dead"]:
        kinds = ledgers["dead"][key]
        for kind in ("manual", "quality", "hardsub", "watermark", "truncated", "no_audio"):
            if kind in kinds:
                return False, f"dead:{kind}"
        return False, "dead:other"
    if key in ledgers["downloaded"] or key in ledgers["local_mp4"]:
        return False, "downloaded_not_uploaded"
    if key in ledgers["results"]:
        return False, "download_failed" if key in ledgers["failed"] else "links_not_downloaded"
    if key in ledgers["fail"]:
        return False, "no_source"
    if key in ledgers["provider_down"]:
        return False, "provider_down"
    if key in ledgers["unresolved"]:
        return False, "fetch_unresolved"
    return False, "not_fetched"


def _check_output_path(path, label):
    """--out / --report 只接受普通文件：原子写会把临时文件 os.replace 到目标上，指向 /dev/null
    之类的设备文件时（以 root 运行）会把设备换成普通文件。"""
    if str(path).startswith("/dev/") or (path.exists() and not path.is_file()):
        raise SystemExit(f"{label} 必须是普通文件路径，收到 {path}")


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
        include_dead=False, r2_client=None, config=None, today=None):
    """主逻辑（命令行与测试共用）。返回汇总字典。"""
    base_dir = Path(base_dir).resolve()
    paths = build_paths(base_dir, ids=ids, config=config)
    out_path = (base_dir / out).resolve()
    _check_output_path(out_path, "--out")
    if out_path == paths["ids"]:
        raise SystemExit(f"--out 不能指向输入文件本身：{out_path}")
    report_path = (base_dir / report).resolve() if report else None
    if report_path is not None:
        _check_output_path(report_path, "--report")
        if report_path in (paths["ids"], out_path):
            raise SystemExit(f"--report 不能指向输入或输出文件：{report_path}")
    if not paths["ids"].exists():
        raise SystemExit(f"找不到剧 id 名单：{paths['ids']}")

    pid = _main_running(paths["lock"])
    if pid:
        print(f"⚠️ 主流程正在运行（PID {pid}）：结果只是此刻的快照，之后才上传完的集也会算未完成。",
              flush=True)
    started = time.time()
    id_list, duplicates = read_ids(paths["ids"])
    ledgers = load_ledgers(paths)
    shows = load_episodes(paths, today=today)

    r2 = None
    r2_note = ""
    if verify_r2:
        client, bucket = r2_client if r2_client is not None else make_r2_client(
            base_dir, paths["s3_config"])
        prefix = f"{paths['s3_prefix']}/{paths['folder_prefix']}/" if paths["s3_prefix"] \
            else f"{paths['folder_prefix']}/"
        t0 = time.time()
        r2 = list_r2_mp4(client, bucket, prefix)
        r2_note = f"R2 {bucket}/{prefix} 下共 {len(r2)} 集 mp4（列举 {time.time() - t0:.1f}s）"

    done, remaining, excluded, show_reasons, rows = Counter(), Counter(), Counter(), Counter(), []
    shows_by_reason = Counter()   # 原因 -> 有这类未完成集的剧数（一部剧可计入多个原因）
    unaired = 0
    held_back = 0                 # 有未完成集、但只剩补跑不推进的集，没写进名单的剧
    keep = []
    for tid in id_list:
        if tid not in shows:
            reason = "show:no_source" if tid in ledgers["fail_shows"] else "show:not_expanded"
            show_reasons[reason] += 1
            listed = include_dead or reason not in TERMINAL_REASONS
            if listed:
                keep.append(tid)
            if report_path:
                rows.append(json.dumps({"tmdbId": tid, "reason": reason, "in_remaining": listed},
                                       ensure_ascii=False))
            continue
        show_open = False
        open_reasons = set()
        for season, episode, not_aired in shows[tid]:
            key = episode_key(tid, season, episode)
            if not_aired:
                unaired += 1
                continue
            finished, reason = classify(key, ledgers, r2)
            if finished:
                done[reason] += 1
                continue
            dropped = not include_dead and reason in TERMINAL_REASONS
            (excluded if dropped else remaining)[reason] += 1
            open_reasons.add(reason)
            show_open = show_open or not dropped
            if report_path:
                rows.append(json.dumps({
                    "tmdbId": tid, "season": season, "episode": episode, "key": key,
                    "reason": reason, "in_remaining": not dropped,
                    "dead_classes": sorted(ledgers["dead"].get(key, ())),
                    "has_links": key in ledgers["results"],
                    "local_mp4": key in ledgers["local_mp4"],
                    "in_r2": None if r2 is None else key in r2,
                }, ensure_ascii=False))
        shows_by_reason.update(open_reasons)
        if show_open:
            keep.append(tid)
        elif open_reasons:
            held_back += 1

    _write_atomic(out_path, keep)
    if report_path:
        _write_atomic(report_path, rows)

    summary = {
        "shows": len(id_list), "duplicates": duplicates, "episodes": sum(done.values())
        + sum(remaining.values()) + sum(excluded.values()), "unaired": unaired,
        "done": dict(done), "remaining": dict(remaining), "excluded": dict(excluded),
        "show_reasons": dict(show_reasons), "shows_by_reason": dict(shows_by_reason),
        "held_back": held_back, "written": len(keep),
        "bad_lines": dict(ledgers["bad_lines"]), "out": str(out_path),
        "report": str(report_path) if report_path else None,
        "r2_listed": None if r2 is None else len(r2),
    }
    _print_summary(summary, r2_note, time.time() - started, include_dead)
    return summary


def _print_summary(s, r2_note, seconds, include_dead):
    total = s["episodes"] or 1
    print(f"\n剧 {s['shows']} 部" + (f"（另有重复 {s['duplicates']} 个已去掉）" if s["duplicates"] else "")
          + f"，已播出的集 {s['episodes']} 集" + (f"（另有未播出 {s['unaired']} 集不计）" if s["unaired"] else ""))
    if r2_note:
        print(r2_note)
    done = sum(s["done"].values())
    print(f"已完成（已进 R2）{done} 集（{done / total:.1%}）"
          + (f"，其中台账没记、R2 上已有 {s['done']['r2_only']} 集" if s["done"].get("r2_only") else ""))
    sections = [("未完成", lambda reason: True)] if include_dead else [
        ("未完成·补跑能推进（进名单）", lambda reason: reason not in TERMINAL_REASONS),
        ("未完成·补跑不推进（不进名单）", lambda reason: reason in TERMINAL_REASONS),
    ]
    for title, wanted in sections:
        lines = []
        for reason, label in REASONS.items():
            count = s["remaining"].get(reason, 0) + s["excluded"].get(reason, 0)
            if count and wanted(reason):
                lines.append(f"  {count:9d} 集  {s['shows_by_reason'].get(reason, 0):6d} 部剧  {label}")
        for reason, label in SHOW_REASONS.items():
            if s["show_reasons"].get(reason) and wanted(reason):
                lines.append(f"  {'':12s}  {s['show_reasons'][reason]:6d} 部剧  {label}")
        if lines:
            print(f"{title}：")
            print("\n".join(lines))
    print(f"写入 {s['out']}：{s['written']} 部剧"
          + ("（有未完成集，含只剩无源 / 判死集的剧）" if include_dead else "（有补跑能推进的集）")
          + (f"；明细 {s['report']}" if s["report"] else ""))
    skipped_shows = s["held_back"] + (0 if include_dead else s["show_reasons"].get("show:no_source", 0))
    if skipped_shows:
        print(f"另有 {skipped_shows} 部剧只剩无源 / 判死集，补跑不会推进，未写入（--include-dead 可一并写入）。")
    hints = [hint for reason, hint in TERMINAL_HINTS.items()
             if s["remaining"].get(reason) or s["excluded"].get(reason)]
    if hints:
        print("这些集要用专用模式处理（自己读台账，用完整的 ids.txt 跑，不要用补跑名单）：")
        for hint in hints:
            print(f"  {hint}")
    if s["bad_lines"]:
        print("⚠️ 跳过的坏行：" + "，".join(f"{k} {v} 行" for k, v in s["bad_lines"].items()))
    print(f"用时 {seconds:.1f}s")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="逐集核对还没进 R2 的集，写出要补跑的剧 id 名单 ids_remaining.txt（只读）")
    parser.add_argument("--dir", default=str(Path(__file__).resolve().parent),
                        help="该机器的 TVDownloader 工作目录（默认本脚本所在目录）")
    parser.add_argument("--ids", default=None,
                        help="剧 id 名单（默认按 config.yaml 的 tv_ids_to_links.input，即 ids.txt）")
    parser.add_argument("--out", default="ids_remaining.txt", help="输出文件（相对 --dir，必须是普通文件）")
    parser.add_argument("--report", default=None, help="逐集明细 jsonl（相对 --dir，可选，必须是普通文件）")
    parser.add_argument("--verify-r2", action="store_true", help="列一遍 R2 核对（需 .env 里的 R2 凭证）")
    dead = parser.add_mutually_exclusive_group()
    dead.add_argument("--include-dead", action="store_true",
                      help="只剩判死（画质 / 硬字幕 / 水印 / 时长不符 / 无音轨 / 人工）或无源集的剧也写进名单"
                           "（默认不写：补跑不会推进它们）")
    dead.add_argument("--exclude-dead", action="store_true",
                      help="已是默认行为（保留以兼容电影侧的用法）")
    args = parser.parse_args(argv)
    run(args.dir, ids=args.ids, out=args.out, report=args.report,
        verify_r2=args.verify_r2, include_dead=args.include_dead)


if __name__ == "__main__":
    main()
