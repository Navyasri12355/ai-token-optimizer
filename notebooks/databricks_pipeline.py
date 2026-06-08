# Databricks notebook source
# MAGIC %md
# MAGIC # AI Token Optimizer – Databricks Pipeline
# MAGIC
# MAGIC Runs all three compute-heavy stages on Databricks Community Edition:
# MAGIC - **Stage 1**: Preprocessing (`preprocess.py` logic)
# MAGIC - **Stage 2**: Model Training (`train_model.py` logic — Ridge / GBT / CV)
# MAGIC - **Stage 3**: Cost Analysis (`cost_analysis.py` logic)
# MAGIC
# MAGIC ELK dashboards are skipped — run those on Oracle Always Free VM separately.
# MAGIC
# MAGIC **Before running**: Upload `raw.jsonl` to DBFS via the Data tab:
# MAGIC   `dbfs:/FileStore/ai-token-optimizer/raw.jsonl`

# COMMAND ----------
# MAGIC %md ## 0. Setup — install dependencies

# COMMAND ----------

# DBTITLE 1,Install packages
# %pip installs are cluster-scoped in Databricks; run this cell first.
# PySpark is already installed — we only need the extras.

%pip install nltk matplotlib python-json-logger --quiet

# COMMAND ----------

import nltk
nltk.download("stopwords", quiet=True)

# COMMAND ----------
# MAGIC %md ## 1. Config

# COMMAND ----------

# DBTITLE 1,Config — edit these paths if needed
# ── DBFS paths ─────────────────────────────────────────────────────────────────
# Upload raw.jsonl via Databricks UI → Data → Add Data → DBFS
RAW_JSONL_PATH     = "dbfs:/FileStore/ai-token-optimizer/raw.jsonl"

OUTPUT_PARQUET     = "dbfs:/FileStore/ai-token-optimizer/output/processed"
SAMPLE_PARQUET     = "dbfs:/FileStore/ai-token-optimizer/output/sample"
MODELS_DIR         = "dbfs:/FileStore/ai-token-optimizer/models"
COST_REPORT_PATH   = "/tmp/cost_report.json"
STATS_PATH         = "/tmp/stats.json"
EVAL_PATH          = "/tmp/evaluation.json"
PLOT_DIR           = "/tmp/plots"

# ── Pipeline switches ──────────────────────────────────────────────────────────
SAMPLE_MODE        = False    # Set True for a quick test (50k conversations)
SAMPLE_SIZE        = 50_000   # Only used when SAMPLE_MODE = True
PRICING_MODEL      = "gpt-4o" # For cost analysis
CV_FOLDS           = 3

# COMMAND ----------
# MAGIC %md ## 2. SparkSession

# COMMAND ----------

# DBTITLE 1,Get SparkSession (Databricks provides one already)
from pyspark.sql import SparkSession

# Databricks already creates `spark` for you — just tune it.
spark = (
    SparkSession.builder
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled",   "true")
    .config("spark.driver.maxResultSize",   "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"App name      : {spark.sparkContext.appName}")

# COMMAND ----------
# MAGIC %md ## Stage 1 — Preprocessing

# COMMAND ----------

# DBTITLE 1,Stage 1: Preprocess raw.jsonl → Parquet
import time
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType, FloatType, BooleanType
)

t0 = time.time()

print(f"Loading JSONL from: {RAW_JSONL_PATH}")

raw_schema = StructType([
    StructField("id",            StringType(), True),
    StructField("conversations", ArrayType(
        StructType([
            StructField("from",  StringType(), True),
            StructField("value", StringType(), True),
        ])
    ), True),
])

df_raw = spark.read.schema(raw_schema).json(RAW_JSONL_PATH)

if SAMPLE_MODE:
    df_raw = df_raw.limit(SAMPLE_SIZE)
    print(f"⚠️  SAMPLE MODE: capped at {SAMPLE_SIZE:,} conversations")

total_convs = df_raw.count()
print(f"Loaded {total_convs:,} conversation records")

# ── Explode turns ──────────────────────────────────────────────────────────────
df_indexed = df_raw.select(
    F.col("id").alias("record_id"),
    F.posexplode("conversations").alias("turn_index", "turn"),
    F.size("conversations").alias("conversation_turn_count"),
)

# ── Feature extraction (native Spark SQL — no Python UDFs) ────────────────────
_FILLER_RE = "(?i)(please |could you |would you mind |kindly |i would like you to |can you |tell me |help me understand )"

df_feats = (
    df_indexed
    .withColumn("role",     F.col("turn.from"))
    .withColumn("raw_text", F.coalesce(F.col("turn.value"), F.lit("")))
    .drop("turn")
    .withColumn("char_count",   F.length("raw_text"))
    .withColumn("word_count",   F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
    .withColumn("token_count",  F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
    .withColumn("sentence_count",
                F.greatest(
                    F.lit(1),
                    F.size(F.split(F.col("raw_text"), r"[.!?]+")) - F.lit(1)
                ))
    .withColumn("avg_word_length",
                F.col("char_count") / F.greatest(F.col("word_count"), F.lit(1)))
    .withColumn("has_code_block", F.col("raw_text").contains("```"))
    # Optimised text
    .withColumn("_opt1", F.lower(F.col("raw_text")))
    .withColumn("_opt2", F.regexp_replace(F.col("_opt1"), _FILLER_RE, " "))
    .withColumn("_opt3", F.regexp_replace(F.col("_opt2"), r"[^\w\s]", ""))
    .withColumn("optimized_text", F.trim(F.regexp_replace(F.col("_opt3"), r"\s+", " ")))
    .drop("_opt1", "_opt2", "_opt3")
    .withColumn("optimized_token_count",
                F.size(F.split(F.trim(F.col("optimized_text")), r"\s+")))
    .withColumn("token_savings",
                F.col("token_count") - F.col("optimized_token_count"))
    .withColumn("savings_pct",
                F.when(F.col("token_count") > 0,
                       (F.col("token_savings") / F.col("token_count") * 100.0))
                .otherwise(F.lit(0.0)))
    .withColumn("is_human", F.when(F.col("role") == "human", True).otherwise(False))
)

df_clean = (
    df_feats
    .filter(F.col("raw_text").isNotNull())
    .filter(F.length(F.trim(F.col("raw_text"))) > 0)
    .filter(F.col("token_count") > 0)
)

# Write full processed Parquet
out_path = SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET
print(f"Writing Parquet → {out_path}")
(
    df_clean
    .coalesce(1 if SAMPLE_MODE else 4)
    .write
    .mode("overwrite")
    .parquet(out_path)
)

# Always write a 10k sample too (for fast ML iteration)
print(f"Writing sample Parquet → {SAMPLE_PARQUET}")
(
    df_clean
    .limit(10_000)
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(SAMPLE_PARQUET)
)

# Summary stats
stats_row = df_clean.agg(
    F.count("*").alias("total_turns"),
    F.countDistinct("record_id").alias("total_conversations"),
    F.mean("token_count").alias("avg_token_count"),
    F.mean("optimized_token_count").alias("avg_opt_token_count"),
    F.mean("savings_pct").alias("avg_savings_pct"),
    F.sum("token_savings").alias("total_tokens_saved"),
).collect()[0]

elapsed = time.time() - t0
print(f"\n{'='*60}")
print(f"✅ Preprocessing complete in {elapsed:.1f}s")
print(f"   Total conversations : {int(stats_row['total_conversations']):,}")
print(f"   Total turns         : {int(stats_row['total_turns']):,}")
print(f"   Avg token count     : {stats_row['avg_token_count']:.1f}")
print(f"   Avg savings         : {stats_row['avg_savings_pct']:.1f}%")
print(f"{'='*60}")

# Persist stats as notebook variable for later cells
preprocess_stats = stats_row.asDict()

# COMMAND ----------
# MAGIC %md ## Stage 2 — Model Training

# COMMAND ----------

# DBTITLE 1,Stage 2: Train Ridge / GBT / CrossValidated Ridge
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

t0 = time.time()

FEATURE_COLS = [
    "char_count", "word_count", "sentence_count",
    "avg_word_length", "conversation_turn_count",
    "turn_index", "has_code_block_int",
]
TARGET_COL   = "token_count"
OPT_TARGET   = "optimized_token_count"
RIDGE_R2_THRESHOLD = 0.70

train_src = SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET
print(f"Loading training data from: {train_src}")

df = spark.read.parquet(train_src)
df = df.withColumn("has_code_block_int", F.col("has_code_block").cast("int"))
df = df.dropna(subset=FEATURE_COLS + [TARGET_COL, OPT_TARGET])

total = df.count()
print(f"Training on {total:,} turns")

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()
test_df.cache()


def build_pipeline(regressor, target):
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS, outputCol="raw_features", handleInvalid="skip"
    )
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withMean=True, withStd=True
    )
    return Pipeline(stages=[assembler, scaler, regressor.setLabelCol(target).setFeaturesCol("features")])


def evaluate(model, df, target):
    preds = model.transform(df)
    ev = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    return {
        "rmse": ev.setMetricName("rmse").evaluate(preds),
        "mae":  ev.setMetricName("mae").evaluate(preds),
        "r2":   ev.setMetricName("r2").evaluate(preds),
    }


results = {}

# ── Model A: Ridge ─────────────────────────────────────────────────────────────
print("\n🔵 Training Ridge Regression (raw token_count)...")
ridge = LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal")
t1 = time.time()
ridge_model = build_pipeline(ridge, TARGET_COL).fit(train_df)
ridge_metrics = evaluate(ridge_model, test_df, TARGET_COL)
ridge_metrics["training_time_s"] = round(time.time() - t1, 2)
ridge_metrics["model_type"] = "RidgeRegression"
results["ridge_token_count"] = ridge_metrics
print(f"   RMSE={ridge_metrics['rmse']:.3f}  MAE={ridge_metrics['mae']:.3f}  R²={ridge_metrics['r2']:.4f}")

ridge_model.write().overwrite().save(f"{MODELS_DIR}/ridge_token_count")
print(f"   💾 Saved → {MODELS_DIR}/ridge_token_count")

# ── Model B: GBT (auto-selected if Ridge R² < threshold) ──────────────────────
best_model_name = "ridge_token_count"
best_r2 = ridge_metrics["r2"]

if ridge_metrics["r2"] < RIDGE_R2_THRESHOLD:
    print(f"\n⚠️  Ridge R²={ridge_metrics['r2']:.4f} < {RIDGE_R2_THRESHOLD}. Training GBT...")
    gbt = GBTRegressor(maxIter=50, maxDepth=5, stepSize=0.1)
    t2 = time.time()
    gbt_model = build_pipeline(gbt, TARGET_COL).fit(train_df)
    gbt_metrics = evaluate(gbt_model, test_df, TARGET_COL)
    gbt_metrics["training_time_s"] = round(time.time() - t2, 2)
    gbt_metrics["model_type"] = "GBTRegressor"
    results["gbt_token_count"] = gbt_metrics
    print(f"   RMSE={gbt_metrics['rmse']:.3f}  MAE={gbt_metrics['mae']:.3f}  R²={gbt_metrics['r2']:.4f}")
    gbt_model.write().overwrite().save(f"{MODELS_DIR}/gbt_token_count")
    if gbt_metrics["r2"] > best_r2:
        best_model_name, best_r2 = "gbt_token_count", gbt_metrics["r2"]
else:
    print(f"✅ Ridge R²={ridge_metrics['r2']:.4f} ≥ {RIDGE_R2_THRESHOLD}. Ridge selected.")

# ── Model C: Ridge for optimised token count ───────────────────────────────────
print("\n🟣 Training Ridge Regression (optimized_token_count)...")
ridge_opt = LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal")
t3 = time.time()
ridge_opt_model = build_pipeline(ridge_opt, OPT_TARGET).fit(train_df)
ridge_opt_metrics = evaluate(ridge_opt_model, test_df, OPT_TARGET)
ridge_opt_metrics["training_time_s"] = round(time.time() - t3, 2)
ridge_opt_metrics["model_type"] = "RidgeRegression"
results["ridge_opt_token_count"] = ridge_opt_metrics
print(f"   RMSE={ridge_opt_metrics['rmse']:.3f}  MAE={ridge_opt_metrics['mae']:.3f}  R²={ridge_opt_metrics['r2']:.4f}")
ridge_opt_model.write().overwrite().save(f"{MODELS_DIR}/ridge_opt_token_count")

# ── Model D: Cross-validated Ridge ────────────────────────────────────────────
print(f"\n🔁 Cross-validating Ridge ({CV_FOLDS} folds)...")
cv_ridge = LinearRegression(elasticNetParam=0.0, maxIter=100, solver="normal")
cv_pipe  = build_pipeline(cv_ridge, TARGET_COL)
param_grid = ParamGridBuilder().addGrid(cv_ridge.regParam, [0.01, 0.1, 1.0]).build()
evaluator  = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse")
cv = CrossValidator(
    estimator=cv_pipe, estimatorParamMaps=param_grid,
    evaluator=evaluator, numFolds=CV_FOLDS, parallelism=2,
)
t4 = time.time()
cv_model = cv.fit(train_df)
cv_metrics = evaluate(cv_model.bestModel, test_df, TARGET_COL)
cv_metrics["training_time_s"] = round(time.time() - t4, 2)
cv_metrics["model_type"] = "CrossValidated_RidgeRegression"
cv_metrics["best_regParam"] = float(cv_model.bestModel.stages[-1]._java_obj.getRegParam())
results["cv_ridge_token_count"] = cv_metrics
print(f"   RMSE={cv_metrics['rmse']:.3f}  MAE={cv_metrics['mae']:.3f}  R²={cv_metrics['r2']:.4f}  bestRegParam={cv_metrics['best_regParam']}")
cv_model.bestModel.write().overwrite().save(f"{MODELS_DIR}/cv_ridge_token_count")

# ── Summary ────────────────────────────────────────────────────────────────────
import json
elapsed = time.time() - t0
train_summary = {
    "best_model": best_model_name,
    "best_r2": best_r2,
    "total_training_time_s": round(elapsed, 2),
    "dataset_size": total,
    "models": results,
}
with open(EVAL_PATH, "w") as fh:
    json.dump(train_summary, fh, indent=2)

print(f"\n{'='*60}")
print(f"🏆 Best model : {best_model_name}  (R²={best_r2:.4f})")
print(f"⏱️  Total time  : {elapsed:.1f}s")
print(f"📄 Evaluation  → {EVAL_PATH}")
print(f"{'='*60}")

# COMMAND ----------
# MAGIC %md ## Stage 3 — Cost Analysis

# COMMAND ----------

# DBTITLE 1,Stage 3: Cost Analysis
import json, os

PRICING = {
    "gpt-4o":           {"input": 0.005,   "output": 0.015},
    "gpt-4-turbo":      {"input": 0.010,   "output": 0.030},
    "gpt-3.5-turbo":    {"input": 0.0015,  "output": 0.002},
    "claude-3-opus":    {"input": 0.015,   "output": 0.075},
    "claude-3-sonnet":  {"input": 0.003,   "output": 0.015},
    "claude-3-haiku":   {"input": 0.00025, "output": 0.00125},
}

t0 = time.time()
model   = PRICING_MODEL
prices  = PRICING[model]
in_rate, out_rate = prices["input"], prices["output"]

cost_src = SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET
print(f"💰 Cost analysis — model={model}, source={cost_src}")

df = spark.read.parquet(cost_src)
df = (
    df
    .withColumn("cost_raw_usd",
        F.when(F.col("role") == "human", F.col("token_count")           / 1000.0 * in_rate)
         .otherwise(                     F.col("token_count")           / 1000.0 * out_rate))
    .withColumn("cost_opt_usd",
        F.when(F.col("role") == "human", F.col("optimized_token_count") / 1000.0 * in_rate)
         .otherwise(                     F.col("optimized_token_count") / 1000.0 * out_rate))
    .withColumn("cost_saved_usd", F.col("cost_raw_usd") - F.col("cost_opt_usd"))
)

conv_costs = (
    df.groupBy("record_id")
    .agg(
        F.sum("cost_raw_usd").alias("conv_cost_raw"),
        F.sum("cost_opt_usd").alias("conv_cost_opt"),
        F.sum("cost_saved_usd").alias("conv_cost_saved"),
        F.sum("token_count").alias("conv_tokens_raw"),
        F.sum("optimized_token_count").alias("conv_tokens_opt"),
        F.count("*").alias("conv_turns"),
    )
    .withColumn("conv_savings_pct",
                F.when(F.col("conv_cost_raw") > 0,
                       F.col("conv_cost_saved") / F.col("conv_cost_raw") * 100.0)
                .otherwise(F.lit(0.0)))
)
conv_costs.cache()

totals = conv_costs.agg(
    F.count("*").alias("total_conversations"),
    F.sum("conv_cost_raw").alias("total_cost_raw_usd"),
    F.sum("conv_cost_opt").alias("total_cost_opt_usd"),
    F.sum("conv_cost_saved").alias("total_cost_saved_usd"),
    F.sum("conv_tokens_raw").alias("total_tokens_raw"),
    F.sum("conv_tokens_opt").alias("total_tokens_opt"),
    F.mean("conv_cost_raw").alias("avg_cost_raw_usd"),
    F.mean("conv_cost_opt").alias("avg_cost_opt_usd"),
    F.mean("conv_savings_pct").alias("avg_savings_pct"),
    F.max("conv_cost_saved").alias("max_savings_single_conv_usd"),
).collect()[0]

t = totals.asDict()

# Per-model comparison
model_comparison = {}
for m, p in PRICING.items():
    ir, or_ = p["input"], p["output"]
    tc_raw = int(t.get("total_tokens_raw") or 0)
    tc_opt = int(t.get("total_tokens_opt") or 0)
    raw_cost = tc_raw * 0.6 / 1000 * ir + tc_raw * 0.4 / 1000 * or_
    opt_cost = tc_opt * 0.6 / 1000 * ir + tc_opt * 0.4 / 1000 * or_
    model_comparison[m] = {
        "raw_cost_usd": round(raw_cost, 4),
        "opt_cost_usd": round(opt_cost, 4),
        "saved_usd":    round(raw_cost - opt_cost, 4),
        "savings_pct":  round((raw_cost - opt_cost) / raw_cost * 100, 2) if raw_cost > 0 else 0.0,
    }

elapsed = time.time() - t0
report = {
    "model": model, "pricing": prices,
    "elapsed_seconds": round(elapsed, 2),
    "totals": {k: (round(float(v), 6) if v is not None else None) for k, v in t.items()},
    "all_model_comparison": model_comparison,
}

with open(COST_REPORT_PATH, "w") as fh:
    json.dump(report, fh, indent=2)

print(f"\n{'='*60}")
print(f"💰 Cost Analysis — {model}")
print(f"   Conversations    : {int(t.get('total_conversations', 0)):,}")
print(f"   Raw tokens       : {int(t.get('total_tokens_raw', 0)):,}")
print(f"   Optimised tokens : {int(t.get('total_tokens_opt', 0)):,}")
print(f"   Raw cost         : ${t.get('total_cost_raw_usd', 0):.4f}")
print(f"   Optimised cost   : ${t.get('total_cost_opt_usd', 0):.4f}")
print(f"   💵 SAVED         : ${t.get('total_cost_saved_usd', 0):.4f}  ({t.get('avg_savings_pct', 0):.1f}% avg)")
print(f"   Report           : {COST_REPORT_PATH}")
print(f"{'='*60}")

# Show per-model comparison as a table
import pandas as pd
comp_df = pd.DataFrame(model_comparison).T.reset_index().rename(columns={"index": "model"})
display(comp_df)

# COMMAND ----------
# MAGIC %md ## Stage 4 — Download Results

# COMMAND ----------

# DBTITLE 1,Download outputs back to local (optional)
# Run this to copy results out of DBFS to /tmp so you can download them
# via File → Download (or use dbutils.fs.cp to an S3/GCS bucket).

import subprocess

# Copy evaluation JSON
dbutils.fs.cp(f"dbfs:/FileStore/ai-token-optimizer/models", "file:/tmp/models", recurse=True)
print("Models copied to /tmp/models")

# You can download /tmp/cost_report.json and /tmp/evaluation.json
# directly from the Databricks cluster via the Files API or scp.
print(f"Cost report  : {COST_REPORT_PATH}")
print(f"Eval report  : {EVAL_PATH}")
print(f"Stats        : {STATS_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Download `evaluation.json` and `cost_report.json` from `/tmp/`
# MAGIC - Download saved models from `dbfs:/FileStore/ai-token-optimizer/models`
# MAGIC - Run ELK + dashboard on your Oracle VM: `docker compose up -d`
# MAGIC - Push metrics to Elasticsearch from your local machine using the downloaded JSONs
