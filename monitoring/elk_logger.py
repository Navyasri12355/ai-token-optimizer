"""
monitoring/elk_logger.py
========================
ELK-integrated structured logger for all Spark pipeline scripts.

Features
--------
- JSON-formatted console output (pythonjsonlogger)
- Elasticsearch handler (graceful fallback if ES is down)
- Per-event structured fields: pipeline_stage, event_type, metrics
- Index pattern: token-optimizer-logs-YYYY.MM.DD

Usage:
    from monitoring.elk_logger import get_elk_logger
    logger = get_elk_logger("preprocess")
    logger.info("Step done", extra={"rows": 5000, "stage": "explode"})
"""

import logging
import os
import json
import socket
from datetime import datetime
from typing import Any, Dict, Optional

# ── Optional dependencies (graceful fallback) ──────────────────────────────────
try:
    from pythonjsonlogger import jsonlogger
    _HAS_JSON_LOGGER = True
except ImportError:
    _HAS_JSON_LOGGER = False

try:
    from elasticsearch import Elasticsearch
    _HAS_ES = True
except ImportError:
    _HAS_ES = False

# ── Configuration from environment ────────────────────────────────────────────
ES_HOST      = os.environ.get("ES_HOST",      "localhost")
ES_PORT      = int(os.environ.get("ES_PORT",  "9200"))
INDEX_PREFIX = os.environ.get("ES_LOG_INDEX", "token-optimizer-logs")
LOG_LEVEL    = os.environ.get("LOG_LEVEL",    "INFO").upper()
HOSTNAME     = socket.gethostname()


# ── Custom Elasticsearch logging handler ──────────────────────────────────────
class ElasticsearchHandler(logging.Handler):
    """Ships log records to Elasticsearch as JSON documents."""

    def __init__(self, es_client, index_prefix: str = INDEX_PREFIX):
        super().__init__()
        self.es           = es_client
        self.index_prefix = index_prefix
        self._today_index = self._make_index()

    def _make_index(self) -> str:
        return f"{self.index_prefix}-{datetime.now().strftime('%Y.%m.%d')}"

    def emit(self, record: logging.LogRecord):
        try:
            doc = self._build_doc(record)
            self.es.index(index=self._today_index, document=doc)
        except Exception:
            self.handleError(record)

    def _build_doc(self, record: logging.LogRecord) -> Dict[str, Any]:
        doc = {
            "@timestamp":   datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level":        record.levelname,
            "logger":       record.name,
            "message":      record.getMessage(),
            "module":       record.module,
            "function":     record.funcName,
            "line":         record.lineno,
            "host":         HOSTNAME,
            "pipeline":     record.name.split(".")[-1],
        }
        # Attach any extra fields the caller passed
        for key, val in record.__dict__.items():
            if key not in logging.LogRecord("", 0, "", 0, "", (), None).__dict__ \
                    and key not in ("message", "asctime"):
                try:
                    json.dumps(val)   # only include JSON-serialisable extras
                    doc[key] = val
                except (TypeError, ValueError):
                    doc[key] = str(val)
        return doc


# ── Formatter ─────────────────────────────────────────────────────────────────
class _SparkJsonFormatter(logging.Formatter if not _HAS_JSON_LOGGER
                          else jsonlogger.JsonFormatter):
    """JSON formatter with @timestamp field."""
    def format(self, record):
        if _HAS_JSON_LOGGER:
            record.timestamp = datetime.utcnow().isoformat() + "Z"
        return super().format(record)


# ── Public factory ─────────────────────────────────────────────────────────────
_loggers: Dict[str, logging.Logger] = {}


def get_elk_logger(
    name: str,
    es_host: Optional[str] = None,
    es_port: Optional[int] = None,
    index_prefix: str      = INDEX_PREFIX,
    level: str             = LOG_LEVEL,
    console: bool          = True,
) -> logging.Logger:
    """
    Return a configured logger that writes to:
      1. Console (JSON if pythonjsonlogger available, else plain text)
      2. Elasticsearch (if reachable)

    The logger is cached so repeated calls with the same name return the same
    instance without adding duplicate handlers.
    """
    qualified = f"token_optimizer.{name}"
    if qualified in _loggers:
        return _loggers[qualified]

    logger = logging.getLogger(qualified)
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.propagate = False

    # ── Console handler ────────────────────────────────────────────────────────
    if console and not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, level, logging.INFO))
        if _HAS_JSON_LOGGER:
            fmt = _SparkJsonFormatter(
                "%(timestamp)s %(levelname)s %(name)s %(message)s"
            )
        else:
            fmt = logging.Formatter(
                "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    # ── Elasticsearch handler ──────────────────────────────────────────────────
    if _HAS_ES:
        host = es_host or ES_HOST
        port = es_port or ES_PORT
        try:
            # Use URL-style connection (works for both elasticsearch-py v7/v8/v9)
            es = Elasticsearch(f"http://{host}:{port}", request_timeout=3)
            info = es.info()
            es_version = info.get("version", {}).get("number", "unknown")
            eh = ElasticsearchHandler(es, index_prefix=index_prefix)
            eh.setLevel(logging.DEBUG)
            logger.addHandler(eh)
            logger.debug(f"Elasticsearch connected (server v{es_version})",
                         extra={"es_host": host, "es_port": port})
        except Exception as exc:
            # Includes ConnectionError, BadRequestError (version mismatch),
            # TransportError, etc. — always fall back gracefully.
            logger.warning(
                f"Elasticsearch unavailable - logging to console only "
                f"({host}:{port} - {type(exc).__name__}: {exc})"
            )


    _loggers[qualified] = logger
    return logger


# ── Helper: push a structured event directly to ES ───────────────────────────
def push_event(
    event_type: str,
    data: Dict[str, Any],
    es_host: str = ES_HOST,
    es_port: int = ES_PORT,
    index_prefix: str = "token-optimizer-events",
):
    """
    Push a one-off structured event document to Elasticsearch.
    Used by pipeline stages to record high-level lifecycle events.
    """
    if not _HAS_ES:
        return
    try:
        es = Elasticsearch(f"http://{es_host}:{es_port}", request_timeout=2)
        es.info()

        doc = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": event_type,
            "host":       HOSTNAME,
            **data,
        }
        idx = f"{index_prefix}-{datetime.now().strftime('%Y.%m.%d')}"
        es.index(index=idx, document=doc)
    except Exception:
        pass   # silent – never break the pipeline


# ── Kibana index-pattern auto-creation helper ─────────────────────────────────
def ensure_kibana_index_patterns(
    kibana_url: str = "http://localhost:5601",
    patterns: list  = None,
):
    """
    Create Kibana data-view (index pattern) for all pipeline indices.
    Called once after ELK stack starts.
    """
    patterns = patterns or [
        f"{INDEX_PREFIX}-*",
        "token-optimizer-events-*",
        "metrics-*",
    ]
    try:
        import requests
        headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
        for pattern in patterns:
            url = f"{kibana_url}/api/data_views/data_view"
            payload = {
                "data_view": {
                    "title":         pattern,
                    "timeFieldName": "@timestamp",
                }
            }
            r = requests.post(url, json=payload, headers=headers, timeout=5)
            if r.status_code in (200, 201):
                print(f"[OK] Kibana data-view created: {pattern}")
            elif r.status_code == 409:
                print(f"[INFO] Data-view already exists: {pattern}")
            else:
                print(f"[WARN] Could not create data-view {pattern}: {r.text[:200]}")
    except Exception as e:
        print(f"[WARN] Kibana data-view setup skipped: {e}")


# ── Self-test ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    lg = get_elk_logger("self_test")
    lg.debug("Debug message")
    lg.info("Info message – pipeline started", extra={"stage": "init", "rows": 0})
    lg.warning("Warning – sample data used")
    lg.error("Simulated error (not a real error)")
    push_event("pipeline_test", {"status": "ok", "script": "elk_logger.py"})
    print("✅ ELK logger self-test complete")
    print("📊 Check Kibana → http://localhost:5601")
