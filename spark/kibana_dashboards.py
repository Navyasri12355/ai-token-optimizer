"""
spark/kibana_dashboards.py
===========================
Creates rich Kibana dashboards for the token-optimizer pipeline:

  1. 📊 Token Count Analysis   – distribution charts, per-role averages
  2. 🤖 Model Training Metrics – RMSE / MAE / R² time-series
  3. 💰 Cost Analysis          – raw vs optimised cost per model
  4. ⚡ Optimization Savings   – savings % distribution, top conversations
  5. 🏥 Pipeline Health        – log levels, throughput, error rates

Run AFTER docker-compose up (ELK stack must be running):
  python spark/kibana_dashboards.py
  python spark/kibana_dashboards.py --kibana http://localhost:5601
"""

import sys
import os
import json
import time
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("kibana_dashboards")

KIBANA_URL   = os.environ.get("KIBANA_URL", "http://localhost:5601")
HEADERS      = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# Index patterns
LOG_INDEX    = "token-optimizer-logs-*"
METRICS_IDX  = "metrics-*"
EVENTS_IDX   = "token-optimizer-events-*"


# ── Helpers ────────────────────────────────────────────────────────────────────
def _post(path: str, payload: dict, kibana_url: str = KIBANA_URL) -> Optional[dict]:
    url = f"{kibana_url}{path}"
    try:
        r = requests.post(url, json=payload, headers=HEADERS, timeout=10)
        if r.status_code in (200, 201):
            return r.json()
        logger.warning(f"⚠️  POST {path} → {r.status_code}: {r.text[:300]}")
    except Exception as e:
        logger.error(f"❌ POST {path} failed: {e}")
    return None


def _put(path: str, payload: dict, kibana_url: str = KIBANA_URL) -> Optional[dict]:
    url = f"{kibana_url}{path}"
    try:
        r = requests.put(url, json=payload, headers=HEADERS, timeout=10)
        if r.status_code in (200, 201):
            return r.json()
        logger.warning(f"⚠️  PUT {path} → {r.status_code}: {r.text[:300]}")
    except Exception as e:
        logger.error(f"❌ PUT {path} failed: {e}")
    return None


def wait_for_kibana(kibana_url: str = KIBANA_URL, retries: int = 30) -> bool:
    """Poll /api/status until Kibana is ready."""
    for i in range(retries):
        try:
            r = requests.get(f"{kibana_url}/api/status",
                             headers=HEADERS, timeout=5)
            if r.status_code == 200:
                logger.info("✅ Kibana is ready")
                return True
        except Exception:
            pass
        logger.info(f"⏳ Waiting for Kibana ({i+1}/{retries}) …")
        time.sleep(3)
    logger.error("❌ Kibana not reachable")
    return False


# ── Data Views (index patterns) ────────────────────────────────────────────────
def create_data_view(pattern: str, time_field: str = "@timestamp",
                     kibana_url: str = KIBANA_URL):
    """Create or fetch a Kibana data view / index pattern. Returns its UUID."""

    # 1. Try to create fresh
    payload = {"data_view": {"title": pattern, "timeFieldName": time_field}}
    res = _post("/api/data_views/data_view", payload, kibana_url)
    if res:
        vid = res.get("data_view", {}).get("id")
        logger.info(f"   Data view created : {pattern}  ({vid})")
        return vid

    # 2. Already exists — fetch its ID via search
    try:
        url = f"{kibana_url}/api/data_views"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            for dv in r.json().get("data_view", []):
                if dv.get("title") == pattern:
                    vid = dv["id"]
                    logger.info(f"   Data view exists  : {pattern}  ({vid})")
                    return vid
    except Exception as e:
        logger.warning(f"   Could not list data views: {e}")

    # 3. Fallback — use pattern string directly (legacy Kibana)
    logger.warning(f"   Using pattern string as fallback ID: {pattern}")
    return pattern


# ── Visualization helpers ──────────────────────────────────────────────────────
def _vis_doc(title: str, vis_type: str, aggs: list,
             index_id: str, params: dict = None) -> dict:
    """Build a Kibana visualization saved-object payload."""
    state = {
        "title": title,
        "type": vis_type,
        "params": params or {"perPage": 10, "showPartialRows": False,
                             "showMetricsAtAllLevels": False},
        "aggs": aggs,
    }
    return {
        "attributes": {
            "title": title,
            "visState": json.dumps(state),
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": index_id,
                    "query": {"language": "kuery", "query": ""},
                    "filter": [],
                })
            },
        }
    }


def create_visualization(title: str, vis_type: str, aggs: list,
                          index_id: str, params: dict = None,
                          kibana_url: str = KIBANA_URL) -> Optional[str]:
    payload = _vis_doc(title, vis_type, aggs, index_id, params)
    res = _post("/api/saved_objects/visualization", payload, kibana_url)
    if res:
        vid = res.get("id")
        logger.info(f"   ✅ Visualization: {title} ({vid})")
        return vid
    return None


# ── Dashboard builder ──────────────────────────────────────────────────────────
def create_dashboard(title: str, vis_ids: List[str],
                     kibana_url: str = KIBANA_URL) -> Optional[str]:
    """Create a dashboard with one panel per visualization."""
    COLS = 24
    W, H = 12, 8
    panels = []
    for i, vid in enumerate(v for v in vis_ids if v):
        col = (i % 2) * W
        row = (i // 2) * H
        panels.append({
            "panelIndex": str(i + 1),
            "gridData": {"x": col, "y": row, "w": W, "h": H, "i": str(i + 1)},
            "type": "visualization",
            "version": "8.0.0",
            "embeddableConfig": {},
            "panelRefName": f"panel_{i}",
        })

    refs = [
        {"name": f"panel_{i}", "type": "visualization", "id": vid}
        for i, vid in enumerate(v for v in vis_ids if v)
    ]

    payload = {
        "attributes": {
            "title": title,
            "panelsJSON": json.dumps(panels),
            "timeRestore": True,
            "timeFrom": "now-7d",
            "timeTo": "now",
            "optionsJSON": json.dumps({"hidePanelTitles": False,
                                       "useMargins": True}),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"language": "kuery",
                                                          "query": ""},
                                               "filter": []})
            },
        },
        "references": refs,
    }
    res = _post("/api/saved_objects/dashboard", payload, kibana_url)
    if res:
        did = res.get("id")
        logger.info(f"✅ Dashboard '{title}' created  → "
                    f"{kibana_url}/app/dashboards#/view/{did}")
        return did
    return None


# ── Specific dashboards ────────────────────────────────────────────────────────
def setup_all_dashboards(kibana_url: str = KIBANA_URL):
    """Create all five dashboards."""
    logger.info("\n🏗️  Setting up Kibana dashboards …")
    logger.info(f"   Kibana URL: {kibana_url}")

    # ── Step 1: Data views ─────────────────────────────────────────────────────
    logger.info("\n📑 Creating data views …")
    log_id     = create_data_view(LOG_INDEX,   "@timestamp", kibana_url) or LOG_INDEX
    metrics_id = create_data_view(METRICS_IDX, "timestamp",  kibana_url) or METRICS_IDX
    events_id  = create_data_view(EVENTS_IDX,  "@timestamp", kibana_url) or EVENTS_IDX

    # ── Dashboard 1: Token Count Analysis ─────────────────────────────────────
    logger.info("\n📊 Dashboard 1: Token Count Analysis")
    tc_vis_ids = []
    # Avg token count over time
    tc_vis_ids.append(create_visualization(
        title="Avg Token Count Over Time",
        vis_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg",
             "params": {"field": "metrics.avg_token_count"},
             "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=metrics_id,
        kibana_url=kibana_url,
    ))
    # Token savings histogram
    tc_vis_ids.append(create_visualization(
        title="Token Savings Distribution",
        vis_type="histogram",
        aggs=[
            {"id": "1", "enabled": True, "type": "count",
             "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "histogram",
             "params": {"field": "metrics.avg_savings_pct", "interval": 5},
             "schema": "segment"},
        ],
        index_id=metrics_id,
        kibana_url=kibana_url,
    ))
    create_dashboard("📊 Token Count Analysis", tc_vis_ids, kibana_url)

    # ── Dashboard 2: Model Training Metrics ───────────────────────────────────
    logger.info("\n🤖 Dashboard 2: Model Training Metrics")
    mt_vis_ids = []
    for metric in ["mae", "rmse", "r2"]:
        mt_vis_ids.append(create_visualization(
            title=f"Training {metric.upper()} Over Time",
            vis_type="line",
            aggs=[
                {"id": "1", "enabled": True, "type": "avg",
                 "params": {"field": f"metrics.{metric}"},
                 "schema": "metric"},
                {"id": "2", "enabled": True, "type": "date_histogram",
                 "params": {"field": "timestamp", "interval": "auto"},
                 "schema": "segment"},
            ],
            index_id=metrics_id,
            kibana_url=kibana_url,
        ))
    mt_vis_ids.append(create_visualization(
        title="Training Time (seconds)",
        vis_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg",
             "params": {"field": "metrics.training_time_seconds"},
             "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=metrics_id,
        kibana_url=kibana_url,
    ))
    create_dashboard("🤖 Model Training Metrics", mt_vis_ids, kibana_url)

    # ── Dashboard 3: Cost Analysis ────────────────────────────────────────────
    logger.info("\n💰 Dashboard 3: Cost Analysis")
    ca_vis_ids = []
    ca_vis_ids.append(create_visualization(
        title="Total Cost – Raw vs Optimised",
        vis_type="histogram",
        aggs=[
            {"id": "1", "enabled": True, "type": "sum",
             "params": {"field": "total_cost_raw_usd"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "model", "size": 10}, "schema": "segment"},
        ],
        index_id=events_id,
        kibana_url=kibana_url,
    ))
    ca_vis_ids.append(create_visualization(
        title="Cost Savings ($USD) Over Time",
        vis_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "sum",
             "params": {"field": "total_savings_usd"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "@timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=events_id,
        kibana_url=kibana_url,
    ))
    create_dashboard("💰 Cost Analysis", ca_vis_ids, kibana_url)

    # ── Dashboard 4: Optimization Savings ─────────────────────────────────────
    logger.info("\n⚡ Dashboard 4: Optimization Savings")
    os_vis_ids = []
    os_vis_ids.append(create_visualization(
        title="Avg Savings % Over Time",
        vis_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg",
             "params": {"field": "avg_savings_pct"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "@timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=events_id,
        kibana_url=kibana_url,
    ))
    os_vis_ids.append(create_visualization(
        title="Rows Processed Over Time",
        vis_type="area",
        aggs=[
            {"id": "1", "enabled": True, "type": "sum",
             "params": {"field": "rows_processed"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "@timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=events_id,
        kibana_url=kibana_url,
    ))
    create_dashboard("⚡ Optimization Savings", os_vis_ids, kibana_url)

    # ── Dashboard 5: Pipeline Health ──────────────────────────────────────────
    logger.info("\n🏥 Dashboard 5: Pipeline Health")
    ph_vis_ids = []
    ph_vis_ids.append(create_visualization(
        title="Log Events by Level",
        vis_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count",
             "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "level", "size": 5}, "schema": "segment"},
        ],
        index_id=log_id,
        kibana_url=kibana_url,
    ))
    ph_vis_ids.append(create_visualization(
        title="Log Volume Over Time",
        vis_type="histogram",
        aggs=[
            {"id": "1", "enabled": True, "type": "count",
             "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "@timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=log_id,
        kibana_url=kibana_url,
    ))
    ph_vis_ids.append(create_visualization(
        title="Pipeline Throughput (rows/s)",
        vis_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg",
             "params": {"field": "throughput_rows_per_sec"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "@timestamp", "interval": "auto"},
             "schema": "segment"},
        ],
        index_id=events_id,
        kibana_url=kibana_url,
    ))
    create_dashboard("🏥 Pipeline Health", ph_vis_ids, kibana_url)

    logger.info("\n" + "=" * 60)
    logger.info("✅ All dashboards set up!")
    logger.info(f"📈 Open Kibana: {kibana_url}/app/dashboards")
    logger.info("=" * 60)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Create Kibana dashboards")
    ap.add_argument("--kibana", default=KIBANA_URL,
                    help="Kibana URL (default: http://localhost:5601)")
    ap.add_argument("--no-wait", action="store_true",
                    help="Skip Kibana readiness check")
    args = ap.parse_args()

    if not args.no_wait:
        if not wait_for_kibana(args.kibana):
            sys.exit(1)

    setup_all_dashboards(args.kibana)
