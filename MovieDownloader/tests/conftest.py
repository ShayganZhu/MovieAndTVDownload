"""Shared pytest setup for MovieDownloader offline tests.

The scripts under test read config.yaml and .env at import time and abort when
required secrets are missing. Provide harmless placeholders here so modules can
be imported without touching any network service.
"""

import os
import sys
from pathlib import Path

_MOVIE_DIR = Path(__file__).resolve().parent.parent
if str(_MOVIE_DIR) not in sys.path:
    sys.path.insert(0, str(_MOVIE_DIR))

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
    """硬字幕检测默认关闭，台账与证据目录一律指到 tmp_path。

    许多用例把转封装 mock 成"直接成功"后走 finalize_one_entry：不关掉的话会
    对不存在的文件跑检测，并把台账写进真实工作区。需要它的用例自行打开。
    """
    d = sys.modules.get("download_movies")
    if d is None:
        yield
        return
    monkeypatch.setattr(d, "HARDSUB_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "HARDSUB_SCAN_LOG", str(tmp_path / "hardsub_scan.jsonl"))
    monkeypatch.setattr(d, "HARDSUB_EVIDENCE_DIR", str(tmp_path / "hardsub_evidence"))
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "download_dead.jsonl"))
    yield
