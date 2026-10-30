"""Shared pytest setup for TVDownloader offline tests.

The scripts under test read config.yaml and .env at import time and abort when
required secrets are missing. Provide harmless placeholders here so modules can
be imported without touching any network service.
"""

import os
import sys
from pathlib import Path

_TV_DIR = Path(__file__).resolve().parent.parent
if str(_TV_DIR) not in sys.path:
    sys.path.insert(0, str(_TV_DIR))

os.environ.setdefault("TMDB_API_KEY", "test-key")
os.environ.setdefault("SUBDL_API_KEY", "test-key")
os.environ.setdefault("PROXY_USER", "test-user")
os.environ.setdefault("PROXY_PASSWORD", "test-pass")
os.environ.setdefault("R2_ACCESS_KEY", "test-id")
os.environ.setdefault("R2_SECRET_KEY", "test-secret")
os.environ.setdefault("R2_ENDPOINT_URL", "https://example.invalid")
os.environ.setdefault("R2_BUCKET", "test-bucket")


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_hardsub_check(tmp_path, monkeypatch):
    """硬字幕检测默认关闭，台账与证据目录一律指到 tmp_path（与电影侧同口径）。

    许多用例把转封装 mock 成"直接成功"后走 finalize_one_entry：不关掉的话会
    对不存在的文件跑检测，并把台账写进真实工作区。需要它的用例自行打开。
    """
    d = sys.modules.get("download_tv")
    if d is None:
        yield
        return
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", False)
    # 按主机限流（host_limits）默认关掉：许多用例的节点就在 sun.finalsignal.top 上，
    # 不关的话会去拼真实的住宅代理、按名额排队。需要它的用例自行打开。
    monkeypatch.setattr(d, "HOST_LIMITS", {})
    monkeypatch.setattr(d, "HOST_CONNECTIONS", d.HostConnections(limits={}))
    monkeypatch.setattr(d, "HARDSUB_SCAN_LOG", str(tmp_path / "hardsub_scan.jsonl"))
    monkeypatch.setattr(d, "HARDSUB_EVIDENCE_DIR", str(tmp_path / "hardsub_evidence"))
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "download_dead.jsonl"))
    monkeypatch.setattr(d, "OVERLAY_NODE_REJECT_LOG",
                        str(tmp_path / "overlay_node_rejects.jsonl"))
    monkeypatch.setattr(d, "_overlay_node_rejects", None)
    yield


@pytest.fixture(autouse=True)
def _isolate_fetch_health(monkeypatch):
    """取流源熔断器与直连线路是全进程共用的单例：每个用例换一份新的，
    免得上一个用例攒下的连续失败计数、熔断或直连冷却串到下一个用例（与电影侧同口径）。"""
    t = sys.modules.get("tv_ids_to_links")
    if t is not None:
        monkeypatch.setattr(t, "PROVIDER_BREAKER", t.ProviderBreaker())
        monkeypatch.setattr(t, "ENCDEC_ROUTE", t._DirectRoute("enc-dec", t.ENCDEC_ROUTE.enabled))
        monkeypatch.setattr(t, "VIDLINK_ROUTE", t._DirectRoute("vidlink", t.VIDLINK_ROUTE.enabled))
    yield
