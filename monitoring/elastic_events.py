from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _es_base_url() -> str | None:
    host = os.getenv("ES_HOST", "").strip()
    if not host or host in {
        "localhost",
        "http://localhost:9200",
        "https://localhost:9200",
    }:
        return None

    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")

    port = os.getenv("ES_PORT", "9200").strip() or "9200"
    return f"http://{host}:{port}".rstrip("/")


def _index_name(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"


def push_event(index_prefix: str, event_type: str, data: dict[str, Any]) -> bool:
    """Best-effort Elasticsearch event write. Never raises to callers."""
    base_url = _es_base_url()
    if not base_url:
        return False

    document = {
        "@timestamp": _utc_now(),
        "event_type": event_type,
        "service": os.getenv("SERVICE_NAME", "ai-token-optimizer"),
        "host": socket.gethostname(),
        **data,
    }

    body = json.dumps(document, default=str).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/{_index_name(index_prefix)}/_doc",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def timed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 3)
