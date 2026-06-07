"""
cloud/run_pipeline_databricks.py
=================================
Pipeline script for Azure Databricks.
Reads raw.jsonl directly from Azure Blob (wasbs://) — no download needed.
Writes Parquet, models, and JSON reports back to Azure Blob.

On Databricks, `spark` is pre-created. This script reconfigures it
with Azure Blob credentials and then runs all three stages.

Submit as a Databricks Job (not a notebook):
  cluster: see cloud/databricks_setup.sh
  entry:   cloud/run_pipeline_databricks.py
  params:  --sample / --skip-train / etc.
"""

import argparse
import json
import logging
import os
import shlex
import sys
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pipeline_databricks")

# ── Azure config ───────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
STORAGE_KEY = os.environ["AZURE_STORAGE_KEY"]
CONTAINER = os.environ.get("AZURE_CONTAINER", "pipeline-data")

WASBS = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"

RAW_JSONL_PATH = f"{WASBS}/data/raw.jsonl"
OUTPUT_PARQUET = f"{WASBS}/output/processed"
SAMPLE_PARQUET = f"{WASBS}/output/sample"
MODELS_DIR = f"{WASBS}/models"
STATS_BLOB = f"{WASBS}/output/stats.json"
EVAL_BLOB = f"{WASBS}/output/evaluation.json"
COST_REPORT_BLOB = f"{WASBS}/output/cost_report.json"

PRICING = {
    "gpt-4o": {"input": 0.005, "output": 0.015},
    "gpt-4-turbo": {"input": 0.010, "output": 0.030},
    "gpt-3.5-turbo": {"input": 0.0015, "output": 0.002},
    "claude-3-opus": {"input": 0.015, "output": 0.075},
    "claude-3-sonnet": {"input": 0.003, "output": 0.015},
    "claude-3-haiku": {"input": 0.00025, "output": 0.00125},
}


def _utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _es_base_url():
    host = os.environ.get("ES_HOST", "").strip()
    if not host or host in {
        "localhost",
        "http://localhost:9200",
        "https://localhost:9200",
    }:
        return None
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    port = os.environ.get("ES_PORT", "9200").strip() or "9200"
    return f"http://{host}:{port}".rstrip("/")


def _push_es(index_prefix, event_type, data):
    """Best-effort push of real pipeline events to Elasticsearch."""
    base_url = _es_base_url()
    if not base_url:
        return False

    index = f"{index_prefix}-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"
    document = {
        "@timestamp": _utc_now(),
        "event_type": event_type,
        "service": os.environ.get("SERVICE_NAME", "ai-token-optimizer-databricks"),
        "pipeline": "databricks-token-optimizer",
        "storage_account": STORAGE_ACCOUNT,
        **data,
    }
    request = urllib.request.Request(
        f"{base_url}/{index}/_doc",
        data=json.dumps(document, default=str).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _banner(msg):
    logger.info("═" * 62)
    logger.info(f"  {msg}")
    logger.info("═" * 62)


def _get_spark():
    """
    On Databricks, `spark` is already available as a global.
    We just add the Azure Blob credentials to it.
    For local testing, we create a new session.
    """
    try:
        # Databricks provides spark as a built-in global
        s = spark  # noqa: F821
        logger.info("Using Databricks-provided SparkSession")
    except NameError:
        from pyspark.sql import SparkSession

        s = (
            SparkSession.builder.appName("TokenOptimizer-Databricks")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.maxResultSize", "4g")
            .getOrCreate()
        )

    # Configure Azure Blob access
    s.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
        STORAGE_KEY,
    )
    s.sparkContext.setLogLevel("WARN")
    return s


# ── Stage 1: Preprocessing ─────────────────────────────────────────────────────
def run_preprocessing(spark, sample_size=None):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    _banner("Stage 1 — Preprocessing")
    stage_start = time.time()
    logger.info(f"  Input  : {RAW_JSONL_PATH}")
    logger.info(f"  Output : {OUTPUT_PARQUET}")

    raw_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField(
                "conversations",
                ArrayType(
                    StructType(
                        [
                            StructField("from", StringType(), True),
                            StructField("value", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    df_raw = spark.read.schema(raw_schema).json(RAW_JSONL_PATH)
    if sample_size:
        df_raw = df_raw.limit(sample_size)
        logger.info(f"  SAMPLE MODE: {sample_size:,} conversations")

    total_convs = df_raw.count()
    logger.info(f"  Loaded {total_convs:,} conversations")

    _FILLER_RE = "(?i)(please |could you |would you mind |kindly |i would like you to |can you |tell me |help me understand )"

    df_indexed = df_raw.select(
        F.col("id").alias("record_id"),
        F.posexplode("conversations").alias("turn_index", "turn"),
        F.size("conversations").alias("conversation_turn_count"),
    )

    df_feats = (
        df_indexed.withColumn("role", F.col("turn.from"))
        .withColumn("raw_text", F.coalesce(F.col("turn.value"), F.lit("")))
        .drop("turn")
        .withColumn("char_count", F.length("raw_text"))
        .withColumn("word_count", F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
        .withColumn("token_count", F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
        .withColumn(
            "sentence_count",
            F.greatest(
                F.lit(1), F.size(F.split(F.col("raw_text"), r"[.!?]+")) - F.lit(1)
            ),
        )
        .withColumn(
            "avg_word_length",
            F.col("char_count") / F.greatest(F.col("word_count"), F.lit(1)),
        )
        .withColumn("has_code_block", F.col("raw_text").contains("```"))
        .withColumn("_opt1", F.lower(F.col("raw_text")))
        .withColumn("_opt2", F.regexp_replace(F.col("_opt1"), _FILLER_RE, " "))
        .withColumn("_opt3", F.regexp_replace(F.col("_opt2"), r"[^\w\s]", ""))
        .withColumn(
            "optimized_text", F.trim(F.regexp_replace(F.col("_opt3"), r"\s+", " "))
        )
        .drop("_opt1", "_opt2", "_opt3")
        .withColumn(
            "optimized_token_count",
            F.size(F.split(F.trim(F.col("optimized_text")), r"\s+")),
        )
        .withColumn(
            "token_savings", F.col("token_count") - F.col("optimized_token_count")
        )
        .withColumn(
            "savings_pct",
            F.when(
                F.col("token_count") > 0,
                F.col("token_savings") / F.col("token_count") * 100.0,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("is_human", F.when(F.col("role") == "human", True).otherwise(False))
    )

    df_clean = (
        df_feats.filter(F.col("raw_text").isNotNull())
        .filter(F.length(F.trim(F.col("raw_text"))) > 0)
        .filter(F.col("token_count") > 0)
    )

    out_path = SAMPLE_PARQUET if sample_size else OUTPUT_PARQUET
    df_clean.coalesce(1 if sample_size else 4).write.mode("overwrite").parquet(out_path)
    df_clean.limit(10_000).coalesce(1).write.mode("overwrite").parquet(SAMPLE_PARQUET)
    logger.info(f"  Parquet written → {out_path}")

    stats = (
        df_clean.agg(
            F.count("*").alias("total_turns"),
            F.countDistinct("record_id").alias("total_conversations"),
            F.mean("token_count").alias("avg_token_count"),
            F.mean("savings_pct").alias("avg_savings_pct"),
            F.sum("token_savings").alias("total_tokens_saved"),
            F.sum("token_count").alias("total_tokens"),
        )
        .collect()[0]
        .asDict()
    )

    stats_dict = {k: float(v) if v else 0 for k, v in stats.items()}
    # Write stats JSON to Blob
    spark.createDataFrame([stats_dict]).coalesce(1).write.mode("overwrite").json(
        f"{WASBS}/output/stats_spark"
    )

    processing_time = round(time.time() - stage_start, 2)
    rows_processed = int(stats.get("total_turns") or 0)
    logger.info(f"  Conversations : {int(stats['total_conversations']):,}")
    logger.info(f"  Turns         : {int(stats['total_turns']):,}")
    logger.info(f"  Avg savings   : {stats['avg_savings_pct']:.1f}%")
    _push_es(
        "token-optimizer-events",
        "data_processing",
        {
            "stage": "preprocess",
            "operation": "preprocess_jsonl_to_parquet",
            "status": "ok",
            "rows_processed": rows_processed,
            "total_conversations": int(stats.get("total_conversations") or 0),
            "avg_token_count": round(float(stats.get("avg_token_count") or 0), 4),
            "avg_savings_pct": round(float(stats.get("avg_savings_pct") or 0), 4),
            "total_tokens": round(float(stats.get("total_tokens") or 0), 4),
            "total_tokens_saved": round(float(stats.get("total_tokens_saved") or 0), 4),
            "processing_time_seconds": processing_time,
            "throughput_rows_per_sec": round(rows_processed / processing_time, 4)
            if processing_time > 0
            else 0,
            "sample_mode": bool(sample_size),
        },
    )
    return stats_dict


# ── Stage 2: Model Training ────────────────────────────────────────────────────
def run_training(spark, sample_mode=False, cv_folds=3):
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.regression import GBTRegressor, LinearRegression
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.sql import functions as F

    _banner("Stage 2 — Model Training")
    t0 = time.time()

    FEATURE_COLS = [
        "char_count",
        "word_count",
        "sentence_count",
        "avg_word_length",
        "conversation_turn_count",
        "turn_index",
        "has_code_block_int",
    ]
    TARGET_COL, OPT_TARGET = "token_count", "optimized_token_count"

    src = SAMPLE_PARQUET if sample_mode else OUTPUT_PARQUET
    logger.info(f"  Source  : {src}")
    logger.info(f"  Models  : {MODELS_DIR}")

    df = spark.read.parquet(src)
    df = df.withColumn("has_code_block_int", F.col("has_code_block").cast("int"))
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL, OPT_TARGET])
    total = df.count()
    logger.info(f"  Training on {total:,} turns")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()

    def build_pipe(reg, target):
        asm = VectorAssembler(
            inputCols=FEATURE_COLS, outputCol="raw_features", handleInvalid="skip"
        )
        scl = StandardScaler(
            inputCol="raw_features", outputCol="features", withMean=True, withStd=True
        )
        return Pipeline(
            stages=[asm, scl, reg.setLabelCol(target).setFeaturesCol("features")]
        )

    def evaluate(model, df, target):
        preds = model.transform(df)
        ev = RegressionEvaluator(labelCol=target, predictionCol="prediction")
        return {m: ev.setMetricName(m).evaluate(preds) for m in ["rmse", "mae", "r2"]}

    results = {}

    # Ridge
    logger.info("🔵 Ridge Regression...")
    ridge = LinearRegression(
        regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal"
    )
    t1 = time.time()
    ridge_model = build_pipe(ridge, TARGET_COL).fit(train_df)
    ridge_metrics = evaluate(ridge_model, test_df, TARGET_COL)
    ridge_metrics["training_time_s"] = round(time.time() - t1, 2)
    results["ridge_token_count"] = ridge_metrics
    ridge_model.write().overwrite().save(f"{MODELS_DIR}/ridge_token_count")
    logger.info(f"   R²={ridge_metrics['r2']:.4f}  RMSE={ridge_metrics['rmse']:.3f}")

    best_name, best_r2 = "ridge_token_count", ridge_metrics["r2"]

    # GBT if Ridge R² < threshold
    if ridge_metrics["r2"] < 0.70:
        logger.info("⚠️  Ridge R²<0.70 — Training GBT...")
        gbt = GBTRegressor(maxIter=50, maxDepth=5, stepSize=0.1)
        t2 = time.time()
        gbt_model = build_pipe(gbt, TARGET_COL).fit(train_df)
        gbt_metrics = evaluate(gbt_model, test_df, TARGET_COL)
        gbt_metrics["training_time_s"] = round(time.time() - t2, 2)
        results["gbt_token_count"] = gbt_metrics
        gbt_model.write().overwrite().save(f"{MODELS_DIR}/gbt_token_count")
        logger.info(f"   GBT R²={gbt_metrics['r2']:.4f}")
        if gbt_metrics["r2"] > best_r2:
            best_name, best_r2 = "gbt_token_count", gbt_metrics["r2"]

    # Ridge for optimized token count
    logger.info("🟣 Ridge (optimized_token_count)...")
    ridge_opt = LinearRegression(
        regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal"
    )
    ridge_opt_model = build_pipe(ridge_opt, OPT_TARGET).fit(train_df)
    results["ridge_opt_token_count"] = evaluate(ridge_opt_model, test_df, OPT_TARGET)
    ridge_opt_model.write().overwrite().save(f"{MODELS_DIR}/ridge_opt_token_count")

    # Cross-validated Ridge
    logger.info(f"🔁 Cross-validated Ridge ({cv_folds} folds)...")
    cv_ridge = LinearRegression(elasticNetParam=0.0, maxIter=100, solver="normal")
    param_grid = ParamGridBuilder().addGrid(cv_ridge.regParam, [0.01, 0.1, 1.0]).build()
    ev = RegressionEvaluator(
        labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
    )
    cv = CrossValidator(
        estimator=build_pipe(cv_ridge, TARGET_COL),
        estimatorParamMaps=param_grid,
        evaluator=ev,
        numFolds=cv_folds,
        parallelism=2,
    )
    cv_model = cv.fit(train_df)
    cv_metrics = evaluate(cv_model.bestModel, test_df, TARGET_COL)
    cv_metrics["best_regParam"] = float(
        cv_model.bestModel.stages[-1]._java_obj.getRegParam()
    )
    results["cv_ridge_token_count"] = cv_metrics
    cv_model.bestModel.write().overwrite().save(f"{MODELS_DIR}/cv_ridge_token_count")
    logger.info(
        f"   CV R²={cv_metrics['r2']:.4f}  bestRegParam={cv_metrics['best_regParam']}"
    )

    summary = {
        "best_model": best_name,
        "best_r2": best_r2,
        "total_training_time_s": round(time.time() - t0, 2),
        "dataset_size": total,
        "models": results,
    }
    for model_name, metrics in results.items():
        _push_es(
            "metrics",
            "model_training",
            {
                "stage": "train",
                "model_name": model_name,
                "dataset_size": total,
                "mae": round(float(metrics.get("mae", 0)), 6),
                "rmse": round(float(metrics.get("rmse", 0)), 6),
                "r2": round(float(metrics.get("r2", 0)), 6),
                "training_time_seconds": round(
                    float(metrics.get("training_time_s", 0)), 3
                ),
                "best_model": model_name == best_name,
                "sample_mode": sample_mode,
            },
        )
    _push_es(
        "token-optimizer-events",
        "training_summary",
        {
            "stage": "train",
            "status": "ok",
            "best_model": best_name,
            "best_r2": round(float(best_r2), 6),
            "dataset_size": total,
            "total_training_time_seconds": summary["total_training_time_s"],
            "model_count": len(results),
            "sample_mode": sample_mode,
        },
    )
    logger.info(
        f"🏆 Best: {best_name}  R²={best_r2:.4f}  time={summary['total_training_time_s']:.1f}s"
    )
    return summary


# ── Stage 3: Cost Analysis ─────────────────────────────────────────────────────
def run_cost_analysis(spark, sample_mode=False, pricing_model="gpt-4o"):
    from pyspark.sql import functions as F

    _banner("Stage 3 — Cost Analysis")
    prices = PRICING[pricing_model]
    in_r, out_r = prices["input"], prices["output"]
    src = SAMPLE_PARQUET if sample_mode else OUTPUT_PARQUET

    df = spark.read.parquet(src)
    df = (
        df.withColumn(
            "cost_raw_usd",
            F.when(
                F.col("role") == "human", F.col("token_count") / 1000.0 * in_r
            ).otherwise(F.col("token_count") / 1000.0 * out_r),
        )
        .withColumn(
            "cost_opt_usd",
            F.when(
                F.col("role") == "human", F.col("optimized_token_count") / 1000.0 * in_r
            ).otherwise(F.col("optimized_token_count") / 1000.0 * out_r),
        )
        .withColumn("cost_saved_usd", F.col("cost_raw_usd") - F.col("cost_opt_usd"))
    )

    conv = (
        df.groupBy("record_id")
        .agg(
            F.sum("cost_raw_usd").alias("conv_cost_raw"),
            F.sum("cost_opt_usd").alias("conv_cost_opt"),
            F.sum("cost_saved_usd").alias("conv_cost_saved"),
            F.sum("token_count").alias("conv_tokens_raw"),
            F.sum("optimized_token_count").alias("conv_tokens_opt"),
        )
        .withColumn(
            "conv_savings_pct",
            F.when(
                F.col("conv_cost_raw") > 0,
                F.col("conv_cost_saved") / F.col("conv_cost_raw") * 100.0,
            ).otherwise(F.lit(0.0)),
        )
    )
    conv.cache()

    t = (
        conv.agg(
            F.count("*").alias("total_conversations"),
            F.sum("conv_cost_raw").alias("total_cost_raw_usd"),
            F.sum("conv_cost_opt").alias("total_cost_opt_usd"),
            F.sum("conv_cost_saved").alias("total_cost_saved_usd"),
            F.sum("conv_tokens_raw").alias("total_tokens_raw"),
            F.sum("conv_tokens_opt").alias("total_tokens_opt"),
            F.mean("conv_savings_pct").alias("avg_savings_pct"),
        )
        .collect()[0]
        .asDict()
    )

    model_comparison = {}
    for m, p in PRICING.items():
        rc = (
            t["total_tokens_raw"] * 0.6 / 1000 * p["input"]
            + t["total_tokens_raw"] * 0.4 / 1000 * p["output"]
        )
        oc = (
            t["total_tokens_opt"] * 0.6 / 1000 * p["input"]
            + t["total_tokens_opt"] * 0.4 / 1000 * p["output"]
        )
        model_comparison[m] = {
            "raw_cost_usd": round(rc, 4),
            "opt_cost_usd": round(oc, 4),
            "saved_usd": round(rc - oc, 4),
            "savings_pct": round((rc - oc) / rc * 100, 2) if rc > 0 else 0.0,
        }

    report = {
        "model": pricing_model,
        "pricing": prices,
        "totals": {k: (round(float(v), 6) if v else 0) for k, v in t.items()},
        "all_model_comparison": model_comparison,
    }

    logger.info(
        f"  {pricing_model}: raw=${t.get('total_cost_raw_usd', 0):.4f}  "
        f"opt=${t.get('total_cost_opt_usd', 0):.4f}  "
        f"saved=${t.get('total_cost_saved_usd', 0):.4f} "
        f"({t.get('avg_savings_pct', 0):.1f}% avg)"
    )
    _push_es(
        "token-optimizer-events",
        "cost_analysis",
        {
            "stage": "cost",
            "status": "ok",
            "model": pricing_model,
            "sample_mode": sample_mode,
            **report["totals"],
        },
    )
    return report


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    pipeline_args = os.environ.get("PIPELINE_ARGS", "").strip()
    if pipeline_args:
        sys.argv.extend(shlex.split(pipeline_args))

    ap = argparse.ArgumentParser(description="Azure Databricks pipeline")
    ap.add_argument("--sample", action="store_true")
    ap.add_argument("--sample-size", type=int, default=50_000)
    ap.add_argument("--skip-preprocess", action="store_true")
    ap.add_argument("--skip-train", action="store_true")
    ap.add_argument("--skip-cost", action="store_true")
    ap.add_argument("--pricing-model", default="gpt-4o", choices=list(PRICING.keys()))
    ap.add_argument("--cv-folds", type=int, default=3)
    args, unknown_args = ap.parse_known_args()
    if unknown_args:
        logger.info(f"Ignoring Databricks-injected args: {unknown_args}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    _banner(f"AI Token Optimizer — Databricks [{run_id}]")
    logger.info(f"  Storage : {STORAGE_ACCOUNT}")
    logger.info(f"  Mode    : {'SAMPLE' if args.sample else 'FULL'}")
    logger.info(f"  JSONL   : {RAW_JSONL_PATH}")

    t_start = time.time()
    _push_es(
        "token-optimizer-events",
        "pipeline_run_started",
        {
            "run_id": run_id,
            "status": "running",
            "sample_mode": args.sample,
            "raw_jsonl_path": RAW_JSONL_PATH,
            "output_parquet": OUTPUT_PARQUET,
            "models_dir": MODELS_DIR,
        },
    )
    spark = _get_spark()
    report = {"run_id": run_id, "stages": {}}

    if not args.skip_preprocess:
        try:
            r = run_preprocessing(
                spark, sample_size=args.sample_size if args.sample else None
            )
            report["stages"]["preprocess"] = {"status": "ok", "result": r}
        except Exception as e:
            logger.error(f"Preprocessing failed: {e}\n{traceback.format_exc()}")
            report["stages"]["preprocess"] = {"status": "error", "error": str(e)}

    if not args.skip_train:
        try:
            r = run_training(spark, sample_mode=args.sample, cv_folds=args.cv_folds)
            report["stages"]["train"] = {"status": "ok", "result": r}
        except Exception as e:
            logger.error(f"Training failed: {e}\n{traceback.format_exc()}")
            report["stages"]["train"] = {"status": "error", "error": str(e)}

    if not args.skip_cost:
        try:
            r = run_cost_analysis(
                spark, sample_mode=args.sample, pricing_model=args.pricing_model
            )
            report["stages"]["cost"] = {"status": "ok", "result": r}
        except Exception as e:
            logger.error(f"Cost analysis failed: {e}")
            report["stages"]["cost"] = {"status": "error", "error": str(e)}

    total = time.time() - t_start
    _banner(f"Pipeline Complete — {total:.1f}s")
    logger.info(f"  Outputs : {WASBS}/output/")
    logger.info(f"  Models  : {MODELS_DIR}")
    logger.info(json.dumps(report, indent=2, default=str))
    failed_stages = [
        name
        for name, result in report["stages"].items()
        if result.get("status") != "ok"
    ]
    _push_es(
        "token-optimizer-events",
        "pipeline_run_completed",
        {
            "run_id": run_id,
            "status": "failed" if failed_stages else "ok",
            "failed_stages": failed_stages,
            "duration_seconds": round(total, 2),
            "sample_mode": args.sample,
            "stage_count": len(report["stages"]),
        },
    )
    if failed_stages:
        sys.exit(1)


if __name__ == "__main__":
    main()
