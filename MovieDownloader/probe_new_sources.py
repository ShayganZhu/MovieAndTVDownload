#!/usr/bin/env python3
"""新源候选探测（电影版）——核心产出是「源 × 出流主机」矩阵。

与既有 probe_sources.py 的区别，也是本脚本存在的唯一理由：
**除了命中率，还统计每次命中实际落在哪台出流主机上。**

动因（AGENTS §12.41 E 实测）：videasy 重取 23 次，23 次全部又回到
sun.peakstorm.top —— provider→出流主机是固定映射，不是负载均衡池。
所以当某主机长时间故障时，重取/多轮/跨运行全都无效（都不换主机）。
⇒ 判断一个新源有没有价值，命中率只是次要指标，**出流主机是否独立于
peakstorm / hakunaymatata 才是决定性的**。命中率再高但仍落在
peakstorm 上，对我们解决单点依赖毫无帮助。

两阶段用法（服务器 .venv 内）：
    # 阶段一：先用已知有源片验证协议写对了（hexa 上次就是栽在探针 bug）
    python probe_new_sources.py --smoke

    # 阶段二：协议验证通过后，用 dead 样本测真实增量
    python probe_new_sources.py --sample 300

    python probe_new_sources.py --smoke --providers hexa
    python probe_new_sources.py --sample 100 --no-proxy    # 本地直连试跑

输出：probe_new_detail.jsonl（每片×每家明细）+ 终端两张表
      （命中率表、**出流主机矩阵**）。
只做验证，绝不写 results.jsonl / fail.txt。

⚠️ 必须用 dead 样本测增量，不能用有源片对照组（AGENTS §10.2 的教训：
当年用有源片测，得出「videasy 无价值」的错误结论，dead 样本才发现它独占最多）。
有源片只用于 --smoke 阶段验证协议，不产出增量结论。
"""
import argparse
import json
import os
import random
import re
import secrets
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote, urlparse

import yaml
from curl_cffi import requests

HERE = Path(__file__).resolve().parent


# ---------- 配置 / 凭据（与 probe_sources.py 同一套，保证代理口径一致）----------
def _load_dotenv(path):
    try:
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


_load_dotenv(HERE / ".env")
_cfg_path = HERE / "config.yaml"
_CFG = {}
if _cfg_path.exists():
    with open(_cfg_path, encoding="utf-8") as f:
        _CFG = (yaml.safe_load(f) or {}).get("tmdb_ids_to_links", {}) or {}
_PROXY_CFG = _CFG.get("proxy", {}) or {}

API = _CFG.get("api", "https://enc-dec.app/api")
TIMEOUT = int(_CFG.get("timeout", 12))
# hexa 的 /images 端点实测明显慢于其它源，12s（config 给取流用的值）会
# 大量 Timeout 而被误记成 err。单独放宽，避免把「慢」误判成「不可用」。
HEXA_TIMEOUT = int(os.environ.get("HEXA_TIMEOUT", "35"))
PROXY_HOST = _PROXY_CFG.get("host", "")
PROXY_USER = os.environ.get("PROXY_USER", "").strip() or (_PROXY_CFG.get("user") or "").strip()
PROXY_PASS = os.environ.get("PROXY_PASSWORD", "").strip() or (_PROXY_CFG.get("password") or "").strip()
PROXY_PORT_RANGE = tuple(_PROXY_CFG.get("port_range", (9000, 9050)))
USE_PROXY = bool(_PROXY_CFG.get("enabled", True))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36")

# 已知的「自家人」主机：命中若落在这些上面，对解决单点依赖零价值。
# peakstorm  = vidup / videasy / vidfast 共用后端（§5.1 实测 url 一字不差）
# hakunaymatata = vidlink 的出流域（bcdnxw / hcdn3 / bcdn 都是它）
KNOWN_HOST_FAMILIES = {
    "peakstorm": "peakstorm(vidup/videasy/vidfast 共用)",
    "hakunaymatata": "hakunaymatata(vidlink)",
}


def build_proxy():
    if not USE_PROXY:
        return None
    port = random.randint(*PROXY_PORT_RANGE)
    url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{port}"
    return {"http": url, "https": url}


class NoSource(Exception):
    """源站明确表示无此片（404 / 空源 / null）。"""


class Transient(Exception):
    """瞬时错误（超时/5xx/403/解析失败），换 IP 可重试。"""


# enc-dec.app 全站限流 40 req/s，但实测 enc-hexa 在 4 并发下就大量
# 「oop! too many requests」——它是按端点单独计数的，且 hexa 每部片要占
# 1 次 enc + 1 次 dec。用一把全局节流锁给所有 enc-dec 调用限速，
# 避免把限流误读成「源站无此片」（smoke 首跑 5/6 err 就是这么来的）。
_ENCDEC_MIN_INTERVAL = float(os.environ.get("ENCDEC_MIN_INTERVAL", "0.35"))
_encdec_lock = threading.Lock()
_encdec_last = [0.0]


def _encdec_throttle():
    with _encdec_lock:
        wait = _ENCDEC_MIN_INTERVAL - (time.monotonic() - _encdec_last[0])
        if wait > 0:
            time.sleep(wait)
        _encdec_last[0] = time.monotonic()


def _encdec_get(session, url, **kw):
    """带节流 + 限流自动退避的 enc-dec GET。"""
    for attempt in range(4):
        _encdec_throttle()
        r = session.get(url, timeout=TIMEOUT, **kw)
        try:
            data = r.json()
        except Exception:  # noqa: BLE001
            raise Transient(f"enc-dec 非 json: {r.text[:80]}")
        if _is_rate_limited(data):
            time.sleep(1.5 * (attempt + 1))
            continue
        return data
    raise Transient("enc-dec 持续限流")


def _encdec_post(session, url, payload, **kw):
    for attempt in range(4):
        _encdec_throttle()
        r = session.post(url, json=payload, timeout=TIMEOUT, **kw)
        try:
            data = r.json()
        except Exception:  # noqa: BLE001
            raise Transient(f"enc-dec 非 json: {r.text[:80]}")
        if _is_rate_limited(data):
            time.sleep(1.5 * (attempt + 1))
            continue
        return data
    raise Transient("enc-dec 持续限流")


def _is_rate_limited(data):
    if not isinstance(data, dict):
        return False
    blob = f"{data.get('error', '')} {data.get('message', '')}".lower()
    return "too many requests" in blob or "rate limit" in blob


def _validate(data, path):
    if not isinstance(data, dict) or data.get("status") != 200:
        detail = data.get("error", data) if isinstance(data, dict) else data
        raise Transient(f"enc-dec {path}: {str(detail)[:120]}")
    return data["result"]


_MEDIA_RE = re.compile(r"https?://[^\s\"'\\]+\.(?:m3u8|mp4)(?:\?[^\s\"'\\]*)?", re.I)


def _find_media_urls(obj):
    """递归找出结构中的 m3u8/mp4 链接（各家返回结构不固定，宽松判定）。"""
    found = []
    if isinstance(obj, str):
        found.extend(_MEDIA_RE.findall(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            found.extend(_find_media_urls(v))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(_find_media_urls(v))
    return found


def host_of(url):
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def host_family(host):
    """把主机归到「家族」：同一后端的不同子域算一家，这才是判断独立性的粒度。"""
    for needle, label in KNOWN_HOST_FAMILIES.items():
        if needle in host:
            return label
    # 取注册域（last two labels）作为家族名，够用且不引入依赖
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host or "?"


# ---------- 各候选 provider ----------
# 协议全部照 github.com/smy778/EncDecEndpoints/samples 的官方样例改写为 movie 路由。

def probe_hexa(session, ep):
    """hexa.su —— challenge token + 32B hex key，端点是 /images（**这就是官方用法**）。

    ⚠️ AGENTS §7 记「hexa 探针 bug：用了 /images 端点只返回图片元数据」——
    那条记录是错的。官方样例用的就是 `/api/tmdb/{type}/{id}/images`，
    该端点返回的是**加密后的流数据**，名字叫 images 只是混淆。

    实测出来的两个坑（2026-09-19）：
      1. **cap-token 是一次性的**：复用会 403 `captcha_required`。
         所以每次探测都必须重新 GET enc-hexa 拿新 token（本函数已是如此），
         且**不能跨片复用 token**。
      2. **响应慢**：12s 超时（config 给取流用的值）会大量 Timeout，
         实测需要 30s 以上。故本函数用独立的 HEXA_TIMEOUT。
    """
    tid = ep["tid"]
    key = secrets.token_bytes(32).hex()
    headers = {
        "User-Agent": UA,
        "Referer": "https://hexa.su/",
        "Accept": "text/plain",
        "X-Fingerprint-Lite": "e9136c41504646444",
        "X-Api-Key": key,
    }
    token = _validate(_encdec_get(session, f"{API}/enc-hexa"),
                      "enc-hexa").get("token")
    if not token:
        raise Transient("enc-hexa 未返回 token")
    headers["X-Cap-Token"] = token

    r = session.get(f"https://theemoviedb.hexa.su/api/tmdb/movie/{tid}/images",
                    headers=headers, timeout=HEXA_TIMEOUT)
    if r.status_code == 404:
        raise NoSource("api 404")
    if r.status_code == 403 and "captcha" in r.text.lower():
        # token 被拒：换 IP/新 token 重试有意义，属瞬时
        raise Transient(f"captcha_required（token 失效，需换新 token）")
    if r.status_code != 200:
        raise Transient(f"api HTTP {r.status_code} {r.text[:60]}")
    body = r.text.strip()
    if not body:
        raise NoSource("api 空响应")
    d = _validate(_encdec_post(session, f"{API}/dec-hexa",
                               {"text": body, "key": key}), "dec-hexa")
    urls = _find_media_urls(d)
    if urls:
        return list(dict.fromkeys(urls))
    raise NoSource("响应内无 media url")


# meowtv 的 server 列表（照官方样例；turkce 标了 movieOnly，电影侧可用）
MEOWTV_SERVERS = ("ipcloud", "dcloud", "tik", "turkce", "hindiv3")


def probe_meowtv(session, ep):
    """meowtv.ru —— 逐 server 问 api.meowtv.ru，响应整体丢给 dec-meowtv。"""
    tid = ep["tid"]
    headers = {"User-Agent": UA, "Origin": "https://meowtv.ru",
               "Referer": "https://meowtv.ru/"}
    urls, miss, errs = [], 0, []
    for sv in MEOWTV_SERVERS:
        try:
            r = session.get(
                f"https://api.meowtv.ru/streams/movie/{tid}?s={quote(sv)}",
                headers=headers, timeout=TIMEOUT)
            if r.status_code in (404, 422):
                miss += 1
                continue
            if r.status_code != 200:
                errs.append(f"{sv} HTTP {r.status_code}")
                continue
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                errs.append(f"{sv} non-json")
                continue
            if not data:
                miss += 1
                continue
            d = _validate(_encdec_post(session, f"{API}/dec-meowtv",
                                       {"data": data}), "dec-meowtv")
            got = _find_media_urls(d)
            if got:
                urls.extend(got)
            else:
                miss += 1
        except Transient as ex:
            errs.append(f"{sv}: {str(ex)[:60]}")
        except Exception as ex:  # noqa: BLE001
            errs.append(f"{sv}: {type(ex).__name__} {str(ex)[:50]}")
    if urls:
        return list(dict.fromkeys(urls))
    if miss and not errs:
        raise NoSource(f"all {miss} servers empty")
    raise Transient(f"servers failed: {errs[:2]}")


PEACHIFY_SERVERS = ("multi", "hr", "holly", "air", "moviebox")
PEACHIFY_API = "https://x.eat-peach.sbs"


def probe_peachify(session, ep):
    """peachify.top —— {api}/{path}/movie/{tid} 拿 data 字段，再 dec。

    ⚠️ status 页显示 dec-peachify 当前 0 req/min（可能已挂），但 2026-08-24
    官方样例刚更新过（API 域名从 usa/uwu 改成 x.eat-peach.sbs）。
    0 req/min 也可能只是没人用，故仍纳入探测，由实测说话。
    """
    tid = ep["tid"]
    headers = {"User-Agent": UA, "Origin": "https://peachify.top",
               "Referer": "https://peachify.top/"}
    urls, miss, errs = [], 0, []
    for path in PEACHIFY_SERVERS:
        try:
            r = session.get(f"{PEACHIFY_API}/{path}/movie/{tid}",
                            headers=headers, timeout=TIMEOUT)
            if r.status_code == 404:
                miss += 1
                continue
            if r.status_code != 200:
                errs.append(f"{path} HTTP {r.status_code}")
                continue
            try:
                enc_data = r.json().get("data")
            except Exception:  # noqa: BLE001
                errs.append(f"{path} non-json")
                continue
            if not enc_data:
                miss += 1
                continue
            d = _validate(_encdec_post(session, f"{API}/dec-peachify",
                                       {"text": enc_data}), "dec-peachify")
            got = _find_media_urls(d)
            if got:
                urls.extend(got)
            else:
                miss += 1
        except Transient as ex:
            errs.append(f"{path}: {str(ex)[:60]}")
        except Exception as ex:  # noqa: BLE001
            errs.append(f"{path}: {type(ex).__name__} {str(ex)[:50]}")
    if urls:
        return list(dict.fromkeys(urls))
    if miss and not errs:
        raise NoSource(f"all {miss} servers empty")
    raise Transient(f"servers failed: {errs[:2]}")


def probe_cinejoy(session, ep):
    """cinejoy.to（后端 api.shegu.st）—— 需要 title/year/imdb，enc 出 data+state。

    ⚠️ status 页显示 dec-cinejoy 0 req/min。但 enc-cinejoy 有 206 req/min，
    且样例 2026-08-22 刚修过。纳入探测由实测判定。
    本源**强依赖 title/year/imdb**（按标题检索），缺元数据的片直接跳过。
    """
    import base64

    def b64u_encode(raw):
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    def b64u_decode(s):
        s += "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s)

    tid = ep["tid"]
    title = ep.get("title") or ""
    year = ep.get("year") or ""
    imdb = ep.get("imdb") or ""
    if not title:
        raise Transient("缺 title（cinejoy 按标题检索，无法探测）")

    headers = {"Accept": "*/*", "Origin": "https://cinejoy.to",
               "Referer": "https://cinejoy.to/", "User-Agent": UA}
    r = session.get("https://api.shegu.st/servers", headers=headers, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Transient(f"servers HTTP {r.status_code}")
    try:
        servers = [s["name"] for s in r.json().get("servers", [])]
    except Exception:  # noqa: BLE001
        raise Transient("servers non-json")
    if not servers:
        raise Transient("servers 空列表")

    urls, miss, errs = [], 0, []
    for sv in servers[:3]:   # 只探前 3 个 server，控制探测成本
        try:
            src = (f"https://api.shegu.st/?title={quote(title)}&type=movie"
                   f"&year={year}&imdb={imdb}&tmdb={tid}&server={quote(str(sv))}")
            enc = _validate(
                _encdec_get(session, f"{API}/enc-cinejoy?url={quote(src, safe='')}"),
                "enc-cinejoy")
            g = session.post("https://api.shegu.st/g",
                             data=b64u_decode(enc["data"]),
                             headers=headers, timeout=TIMEOUT)
            if g.status_code == 404:
                miss += 1
                continue
            if g.status_code != 200:
                errs.append(f"{sv} g HTTP {g.status_code}")
                continue
            d = _validate(_encdec_post(
                session, f"{API}/dec-cinejoy",
                {"text": b64u_encode(g.content), "state": enc["state"]}),
                "dec-cinejoy")
            got = _find_media_urls(d)
            if got:
                urls.extend(got)
            else:
                miss += 1
        except Transient as ex:
            errs.append(f"{sv}: {str(ex)[:60]}")
        except Exception as ex:  # noqa: BLE001
            errs.append(f"{sv}: {type(ex).__name__} {str(ex)[:50]}")
    if urls:
        return list(dict.fromkeys(urls))
    if miss and not errs:
        raise NoSource(f"all {miss} servers empty")
    raise Transient(f"servers failed: {errs[:2]}")


def probe_vidsrc(session, ep):
    """vidsrc.in —— 纯 embed 站，无需 enc-dec。

    列进来是因为它不依赖 enc-dec.app（我们唯一的解密依赖），若能用则是
    架构上最健壮的一家。但它是 iframe 播放器，能否提到直链未知，探测即答案。
    """
    tid = ep["tid"]
    headers = {"User-Agent": UA, "Referer": "https://vidsrc.in/"}
    r = session.get(f"https://vidsrc.in/embed/movie/{tid}",
                    headers=headers, timeout=TIMEOUT)
    if r.status_code == 404:
        raise NoSource("embed 404")
    if r.status_code != 200:
        raise Transient(f"embed HTTP {r.status_code}")
    urls = _find_media_urls(r.text)
    if urls:
        return list(dict.fromkeys(urls))
    raise NoSource("embed 页内无直链（可能需要多跳解析）")


PROVIDERS = {
    "hexa": probe_hexa,
    "meowtv": probe_meowtv,
    "peachify": probe_peachify,
    "cinejoy": probe_cinejoy,
    "vidsrc": probe_vidsrc,
}
DEFAULT_PROVIDERS = ("hexa", "meowtv", "peachify", "cinejoy", "vidsrc")

# --smoke 用的已知有源片（都是超热门，任何正常源都该有）。
# 只为验证协议写对了，**不用于任何增量结论**。
SMOKE_SAMPLES = [
    {"tid": "27205", "title": "Inception", "year": 2010, "imdb": "tt1375666"},
    {"tid": "155", "title": "The Dark Knight", "year": 2008, "imdb": "tt0468569"},
    {"tid": "550", "title": "Fight Club", "year": 1999, "imdb": "tt0137523"},
    {"tid": "278", "title": "The Shawshank Redemption", "year": 1994, "imdb": "tt0111161"},
    {"tid": "238", "title": "The Godfather", "year": 1972, "imdb": "tt0068646"},
    {"tid": "680", "title": "Pulp Fiction", "year": 1994, "imdb": "tt0110912"},
]


def probe_one(provider, ep, retries):
    """返回 (status, detail)。status ∈ hit/miss/err。

    miss 需二次换 IP 确认（防 CDN/代理抖动把有源误记成无源），与
    probe_sources.py 的 dead_confirm 口径一致。
    """
    fn = PROVIDERS[provider]
    miss_hits, last, last_nosource = 0, "", ""
    for attempt in range(retries + 2):
        t0 = time.time()
        with requests.Session(impersonate="chrome") as session:
            p = build_proxy()
            if p:
                session.proxies = p
            try:
                urls = fn(session, ep)
                hosts = [host_of(u) for u in urls if host_of(u)]
                return "hit", {
                    "urls": urls[:3],
                    "n_urls": len(urls),
                    "hosts": sorted(set(hosts)),
                    "families": sorted({host_family(h) for h in hosts}),
                    "ms": int((time.time() - t0) * 1000),
                }
            except NoSource as ex:
                miss_hits += 1
                last_nosource = str(ex)
                if miss_hits >= 2:
                    return "miss", {"reason": last_nosource}
            except Exception as ex:  # noqa: BLE001
                last = f"{type(ex).__name__}: {str(ex)[:140]}"
                if attempt >= retries:
                    break
        time.sleep(3 if "429" in last else 0.5)
    if miss_hits:
        return "miss", {"reason": f"{last_nosource} (unconfirmed; {last})"}
    return "err", {"reason": last}


# ---------- 样本 ----------
def load_meta(keep_ids=None):
    """从 movies.jsonl 读 title/year/imdb（cinejoy 必需）。"""
    path = HERE / (_CFG.get("metadata", "movies.jsonl"))
    table = {}
    if not path.exists():
        print(f"[meta] 未找到 {path} —— cinejoy 将无法探测（它按标题检索）")
        return table
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                m = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = m.get("tmdb_id")
            if tid is None:
                continue
            tid = str(tid)
            if keep_ids is not None and tid not in keep_ids:
                continue
            table[tid] = {
                "title": m.get("primary_title") or m.get("original_title") or "",
                "year": m.get("start_year"),
                "imdb": m.get("imdb_id") or "",
            }
    print(f"[meta] 已加载 {len(table)} 条影片元数据")
    return table


def load_dead(fail_path):
    ids, seen = [], set()
    with open(fail_path, encoding="utf-8") as f:
        for line in f:
            tid = line.strip()
            if tid and tid not in seen:
                seen.add(tid)
                ids.append(tid)
    return ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="用已知有源片验证协议（不产出增量结论）")
    ap.add_argument("--sample", type=int, default=0, help="从 fail.txt 抽多少部 dead 片")
    ap.add_argument("--input", help="自带样本文件，每行一个 tmdb_id")
    ap.add_argument("--fail-file", default=str(HERE / "fail.txt"))
    ap.add_argument("--providers", default=",".join(DEFAULT_PROVIDERS))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--no-proxy", action="store_true")
    ap.add_argument("--out", default=str(HERE / "probe_new_detail.jsonl"))
    ap.add_argument("--seed", type=int, default=20260919)
    args = ap.parse_args()

    global USE_PROXY
    if args.no_proxy:
        USE_PROXY = False
    if USE_PROXY and not (PROXY_HOST and PROXY_USER and PROXY_PASS):
        print("⚠️ 代理已启用但凭据不全，改为直连（可能被 CF 拦）")
        USE_PROXY = False

    providers = [p.strip() for p in args.providers.split(",") if p.strip()]
    for p in providers:
        if p not in PROVIDERS:
            raise SystemExit(f"未知 provider '{p}'，可选：{', '.join(PROVIDERS)}")

    # ---- 组样本 ----
    if args.smoke:
        eps = list(SMOKE_SAMPLES)
        mode = "smoke（已知有源片，仅验证协议）"
    else:
        if args.input:
            ids = load_dead(args.input)
        else:
            if not args.sample:
                raise SystemExit("需指定 --smoke 或 --sample N 或 --input 文件")
            ids = load_dead(args.fail_file)
            if not ids:
                raise SystemExit(f"{args.fail_file} 里没有可用 id")
            random.seed(args.seed)
            ids = random.sample(ids, min(args.sample, len(ids)))
        meta = load_meta(set(ids))
        eps = []
        for tid in ids:
            m = meta.get(tid, {})
            eps.append({"tid": tid, "title": m.get("title") or "",
                        "year": m.get("year") or "", "imdb": m.get("imdb") or ""})
        mode = f"dead 样本 {len(eps)} 部（真实增量）"

    print("=" * 72)
    print(f"模式：{mode}")
    print(f"候选源：{', '.join(providers)}")
    print(f"代理：{'启用 ' + PROXY_HOST if USE_PROXY else '直连'}，并发 {args.workers}")
    print("=" * 72, flush=True)

    tasks = [(p, ep) for ep in eps for p in providers]
    stats = defaultdict(Counter)
    host_matrix = defaultdict(Counter)      # provider -> 出流主机家族计数
    host_detail = defaultdict(Counter)      # provider -> 具体主机计数
    hit_ids = defaultdict(set)
    lock = threading.Lock()
    done = 0
    out_fh = open(args.out, "w", encoding="utf-8")

    def run(task):
        prov, ep = task
        status, detail = probe_one(prov, ep, args.retries)
        return prov, ep, status, detail

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(run, t) for t in tasks]
        for fut in as_completed(futs):
            prov, ep, status, detail = fut.result()
            with lock:
                done += 1
                stats[prov][status] += 1
                if status == "hit":
                    hit_ids[prov].add(ep["tid"])
                    for fam in detail.get("families", []):
                        host_matrix[prov][fam] += 1
                    for h in detail.get("hosts", []):
                        host_detail[prov][h] += 1
                out_fh.write(json.dumps(
                    {"tmdbId": ep["tid"], "provider": prov, "status": status,
                     "title": ep.get("title"), **detail},
                    ensure_ascii=False) + "\n")
                out_fh.flush()
                if status == "hit":
                    fams = ",".join(detail.get("families") or []) or "?"
                    print(f"[{done}/{len(tasks)}] ✅ {prov:9s} {ep['tid']:>8s} "
                          f"→ {fams}", flush=True)
                elif done % 20 == 0:
                    print(f"[{done}/{len(tasks)}] ...", flush=True)
    out_fh.close()

    # ---- 表一：命中率 ----
    print()
    print("=" * 72)
    print("表一：命中率")
    print("=" * 72)
    print(f"{'源':10s} {'hit':>5s} {'miss':>5s} {'err':>5s} {'命中率':>8s}")
    for p in providers:
        c = stats[p]
        total = c["hit"] + c["miss"] + c["err"]
        rate = f"{c['hit'] / total * 100:.1f}%" if total else "-"
        print(f"{p:10s} {c['hit']:5d} {c['miss']:5d} {c['err']:5d} {rate:>8s}")

    # ---- 表二：出流主机矩阵（本脚本的核心产出）----
    print()
    print("=" * 72)
    print("表二：出流主机矩阵 ← 决定接不接的关键")
    print("=" * 72)
    if not any(host_matrix.values()):
        print("（无命中，无法产出主机矩阵）")
    else:
        for p in providers:
            if not host_matrix[p]:
                continue
            print(f"\n{p}:")
            for fam, n in host_matrix[p].most_common():
                known = any(k in fam for k in KNOWN_HOST_FAMILIES.values())
                tag = "⚠️ 与现有源同族，对单点依赖零价值" if known else "✅ 独立出流主机"
                print(f"   {fam:42s} {n:4d}  {tag}")
            tops = host_detail[p].most_common(5)
            print(f"   具体主机：{', '.join(f'{h}×{n}' for h, n in tops)}")

    # ---- 独占分析（仅 dead 样本有意义）----
    if not args.smoke and len(providers) > 1:
        print()
        print("=" * 72)
        print("表三：独占命中（只有这一家有的片）")
        print("=" * 72)
        for p in providers:
            others = set()
            for q in providers:
                if q != p:
                    others |= hit_ids[q]
            only = hit_ids[p] - others
            print(f"{p:10s} 独占 {len(only):4d} 部  {sorted(only)[:8]}")

    print()
    print(f"明细已写入 {args.out}")
    if args.smoke:
        print("\n⚠️ smoke 模式只验证协议是否写对；增量结论必须跑 --sample（dead 样本）。")


if __name__ == "__main__":
    sys.exit(main())
