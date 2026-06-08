"""
spark/seed_kibana_data.py
==========================
Backfills Elasticsearch with 30 days of realistic historical pipeline
metrics so every Kibana dashboard panel shows meaningful time-series data.

Run once after `docker-compose up -d elasticsearch kibana`:
    python spark/seed_kibana_data.py
    python spark/seed_kibana_data.py --days 60  # longer history
    python spark/seed_kibana_data.py --es http://localhost:9200

Indices populated
-----------------
token-optimizer-events-YYYY.MM.DD   <- pipeline run events
    fields: avg_savings_pct, rows_processed, total_cost_raw_usd,
            total_savings_usd, model, throughput_rows_per_sec,
            total_cost_optimised_usd, savings_pct_by_model

token-optimizer-logs-YYYY.MM.DD     <- structured log stream
    fields: level, logger, message, @timestamp

metrics-YYYY.MM.DD                  <- ML model metrics
    fields: timestamp, metrics.{mae,rmse,r2,avg_token_count,
            avg_savings_pct,training_time_seconds}
"""

import sys
import os
import json
import random
import math
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from elasticsearch import Elasticsearch, helpers
    HAS_ES = True
except ImportError:
    HAS_ES = False
    print("ERROR: elasticsearch package not installed. Run: pip install 'elasticsearch>=8,<9'")
    sys.exit(1)


# ── Realistic value generators ─────────────────────────────────────────────────
MODELS = ["gpt-4o", "gpt-4-turbo", "gpt-3.5-turbo", "claude-3-opus",
          "claude-3-sonnet", "claude-3-haiku"]
LOG_LEVELS = ["INFO", "INFO", "INFO", "INFO", "WARNING", "ERROR"]
LOG_MESSAGES = [
    "Starting PySpark preprocessing pipeline",
    "SparkSession created successfully",
    "Loaded conversation records from raw.json",
    "Extracting features using native Spark SQL expressions",
    "Writing Parquet output",
    "Sample mode: capped at 50000 conversations",
    "Computing summary statistics",
    "Model training completed",
    "Ridge Regression R2 threshold met - Ridge selected as primary model",
    "Cost analysis completed for all models",
    "Pushing metrics to Elasticsearch",
    "Pipeline stage completed successfully",
    "Elasticsearch unavailable - logging to console only",
    "Java heap pressure detected - GC overhead warning",
    "Shuffle partition count adjusted for dataset size",
    "Checkpoint directory initialized for disk spill",
]
LOG_LOGGERS = ["token_optimizer.preprocess", "token_optimizer.train_model",
               "token_optimizer.cost_analysis", "token_optimizer.pipeline"]


def _ts(dt: datetime) -> str:
    """ISO 8601 UTC timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _jitter(base: float, pct: float = 0.15) -> float:
    """Add ±pct% random noise to a base value."""
    return base * (1 + random.uniform(-pct, pct))


def _trend(day: int, total_days: int, start: float, end: float) -> float:
    """Linear trend from start to end over total_days."""
    return start + (end - start) * (day / max(total_days - 1, 1))


def _sine_wave(day: int, period: int = 7, amplitude: float = 0.1) -> float:
    """Weekly seasonality component."""
    return amplitude * math.sin(2 * math.pi * day / period)


# ── Document builders ──────────────────────────────────────────────────────────
def build_pipeline_events(days: int):
    """
    Generate pipeline_run events — one per hour over `days` days.
    These land in token-optimizer-events-* and drive:
      - Avg Savings % Over Time
      - Rows Processed Over Time
      - Cost Savings ($USD) Over Time
      - Total Cost – Raw vs Optimised
      - Pipeline Throughput
    """
    now = datetime.now(timezone.utc)
    docs = []

    for day_offset in range(days, 0, -1):
        day_dt = now - timedelta(days=day_offset)
        # 2–4 pipeline runs per day
        runs_per_day = random.randint(2, 4)
        for run in range(runs_per_day):
            run_hour = random.randint(0, 22)
            run_dt   = day_dt.replace(hour=run_hour,
                                      minute=random.randint(0, 59),
                                      second=random.randint(0, 59))
            model = random.choice(MODELS)

            # Savings % improves slightly over time (model gets better)
            base_savings = _trend(days - day_offset, days, 18.0, 26.0)
            avg_savings_pct = max(5.0, _jitter(base_savings) + _sine_wave(days - day_offset))

            # Rows processed grows over time (more data)
            base_rows = _trend(days - day_offset, days, 80_000, 250_000)
            rows_processed = int(max(1000, _jitter(base_rows, 0.25)))

            # Cost per 1k tokens by model
            pricing = {
                "gpt-4o":          (0.005,  0.015),
                "gpt-4-turbo":     (0.010,  0.030),
                "gpt-3.5-turbo":   (0.0015, 0.002),
                "claude-3-opus":   (0.015,  0.075),
                "claude-3-sonnet": (0.003,  0.015),
                "claude-3-haiku":  (0.00025,0.00125),
            }
            inp_rate, out_rate = pricing[model]
            avg_tokens = _jitter(320.0, 0.3)
            total_tokens_raw   = rows_processed * avg_tokens
            total_tokens_opt   = total_tokens_raw * (1 - avg_savings_pct / 100)

            total_cost_raw_usd       = total_tokens_raw / 1000 * inp_rate
            total_cost_optimised_usd = total_tokens_opt / 1000 * inp_rate
            total_savings_usd        = total_cost_raw_usd - total_cost_optimised_usd

            throughput = _jitter(rows_processed / random.uniform(120, 600))

            doc = {
                "@timestamp":             _ts(run_dt),
                "event_type":             "pipeline_run",
                "model":                  model,
                "avg_savings_pct":        round(avg_savings_pct, 2),
                "rows_processed":         rows_processed,
                "total_cost_raw_usd":     round(total_cost_raw_usd, 6),
                "total_cost_optimised_usd": round(total_cost_optimised_usd, 6),
                "total_savings_usd":      round(total_savings_usd, 6),
                "total_savings_pct":      round(avg_savings_pct, 2),
                "avg_token_count":        round(avg_tokens, 1),
                "throughput_rows_per_sec": round(throughput, 2),
                "run_duration_seconds":   round(random.uniform(60, 600), 1),
                "host":                   "spark-driver-local",
                "pipeline":               "token-optimizer",
            }
            index = f"token-optimizer-events-{run_dt.strftime('%Y.%m.%d')}"
            docs.append({"_index": index, "_source": doc})

    return docs


def build_metrics_docs(days: int):
    """
    Generate ML model metric records — one training run per day.
    These land in metrics-* and drive:
      - Training MAE / RMSE / R² Over Time
      - Avg Token Count Over Time
      - Token Savings Distribution
    """
    now  = datetime.now(timezone.utc)
    docs = []

    for day_offset in range(days, 0, -1):
        run_dt = now - timedelta(days=day_offset,
                                 hours=random.randint(0, 5),
                                 minutes=random.randint(0, 59))

        # R² improves from ~0.82 to ~0.99 as pipeline matures
        r2   = min(0.9999, _trend(days - day_offset, days, 0.82, 0.995)
                   + _jitter(0, 0.03))
        rmse = max(0.05, _jitter(_trend(days - day_offset, days, 8.5, 0.3)))
        mae  = max(0.03, rmse * _jitter(0.68))

        # Token counts: typical GPT conversation
        avg_token_count = _jitter(320.0, 0.15)
        avg_savings_pct = _trend(days - day_offset, days, 16.0, 24.0) + _jitter(0, 0.1)

        doc = {
            "@timestamp":  _ts(run_dt),
            "timestamp":   _ts(run_dt),   # legacy field used by some aggs
            "event_type":  "model_training",
            "model_name":  "ridge_token_count",
            "metrics": {
                "mae":                  round(mae, 4),
                "rmse":                 round(rmse, 4),
                "r2":                   round(r2, 6),
                "avg_token_count":      round(avg_token_count, 2),
                "avg_savings_pct":      round(avg_savings_pct, 2),
                "training_time_seconds": round(random.uniform(30, 300), 1),
            },
            # Flat copies for simpler Kibana queries
            "mae":                  round(mae, 4),
            "rmse":                 round(rmse, 4),
            "r2":                   round(r2, 6),
            "avg_token_count":      round(avg_token_count, 2),
            "avg_savings_pct":      round(avg_savings_pct, 2),
            "training_time_seconds": round(random.uniform(30, 300), 1),
            "host":      "spark-driver-local",
            "pipeline":  "token-optimizer",
        }
        index = f"metrics-{run_dt.strftime('%Y.%m.%d')}"
        docs.append({"_index": index, "_source": doc})

    return docs


def build_log_docs(days: int):
    """
    Generate structured log events — 20–80 per day.
    These land in token-optimizer-logs-* and drive:
      - Log Events by Level (pie)
      - Log Volume Over Time
    """
    now  = datetime.now(timezone.utc)
    docs = []

    for day_offset in range(days, 0, -1):
        day_dt    = now - timedelta(days=day_offset)
        log_count = random.randint(20, 80)
        for _ in range(log_count):
            log_dt = day_dt + timedelta(hours=random.randint(0, 23),
                                        minutes=random.randint(0, 59),
                                        seconds=random.randint(0, 59))
            level   = random.choices(LOG_LEVELS,
                                     weights=[60, 60, 60, 60, 10, 3])[0]
            message = random.choice(LOG_MESSAGES)
            doc = {
                "@timestamp": _ts(log_dt),
                "level":      level,
                "levelname":  level,
                "logger":     random.choice(LOG_LOGGERS),
                "name":       random.choice(LOG_LOGGERS),
                "message":    message,
                "host":       "spark-driver-local",
                "pipeline":   "token-optimizer",
            }
            index = f"token-optimizer-logs-{log_dt.strftime('%Y.%m.%d')}"
            docs.append({"_index": index, "_source": doc})

    return docs


# ── Bulk index ─────────────────────────────────────────────────────────────────
def seed(es_url: str = "http://localhost:9200", days: int = 30):
    print(f"\n{'='*60}")
    print(f"  Seeding Elasticsearch at {es_url}")
    print(f"  History: {days} days")
    print(f"{'='*60}\n")

    es = Elasticsearch(es_url, request_timeout=30)
    try:
        info = es.info()
        print(f"[OK] Connected to Elasticsearch "
              f"v{info['version']['number']}\n")
    except Exception as e:
        print(f"[ERROR] Cannot connect to Elasticsearch: {e}")
        print("       Make sure Docker is running: docker-compose up -d elasticsearch")
        sys.exit(1)

    # Build all documents
    print("[...] Building pipeline event documents...")
    event_docs   = build_pipeline_events(days)
    print(f"      {len(event_docs):,} pipeline event documents")

    print("[...] Building model metrics documents...")
    metric_docs  = build_metrics_docs(days)
    print(f"      {len(metric_docs):,} model metric documents")

    print("[...] Building log stream documents...")
    log_docs     = build_log_docs(days)
    print(f"      {len(log_docs):,} log documents")

    all_docs = event_docs + metric_docs + log_docs
    print(f"\n[...] Bulk indexing {len(all_docs):,} total documents...\n")

    # Bulk index in chunks of 500
    success, failed = 0, 0
    chunk_size = 500
    for i in range(0, len(all_docs), chunk_size):
        chunk = all_docs[i:i + chunk_size]
        try:
            ok, errors = helpers.bulk(es, chunk, raise_on_error=False)
            success += ok
            failed  += len(errors)
            pct = min(100, int((i + len(chunk)) / len(all_docs) * 100))
            print(f"\r    Progress: {pct}%  ({success:,} ok, {failed} failed)",
                  end="", flush=True)
        except Exception as e:
            print(f"\n[WARN] Chunk {i//chunk_size} error: {e}")
            failed += len(chunk)

    print(f"\n\n[OK] Indexed {success:,} documents  ({failed} failed)\n")

    # Print index summary
    print("Index breakdown:")
    indices = es.cat.indices(index="token-optimizer-*,metrics-*",
                             h="index,docs.count,store.size",
                             format="json")
    for idx in sorted(indices, key=lambda x: x["index"]):
        print(f"  {idx['index']:<45} {idx['docs.count']:>8} docs  "
              f"{idx.get('store.size','?'):>8}")

    print(f"\n{'='*60}")
    print(f"  Done! Open Kibana -> http://localhost:5601/app/dashboards")
    print(f"  Set time range to 'Last 30 days' to see all data.")
    print(f"{'='*60}\n")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Seed Elasticsearch with realistic historical pipeline data")
    ap.add_argument("--es",   default="http://localhost:9200",
                    help="Elasticsearch URL")
    ap.add_argument("--days", type=int, default=30,
                    help="Number of days of history to generate (default: 30)")
    args = ap.parse_args()
    random.seed(42)   # reproducible data
    seed(es_url=args.es, days=args.days)
