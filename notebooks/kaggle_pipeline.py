"""
kaggle/notebook_pipeline.py  (Azure edition)
=============================================
Runs the full pipeline on Kaggle (16 GB RAM, free PySpark)
with Azure Blob Storage as the cloud data layer.

Data flow:
  Azure Blob  →  download to /kaggle/working/  →  PySpark processes
  PySpark output  →  upload back to Azure Blob

Before running:
  1. In the Kaggle notebook editor:
     Add-ons → Secrets → add:
       AZURE_STORAGE_ACCOUNT  = your-storage-account
       AZURE_STORAGE_KEY      = your-storage-key
       AZURE_CONTAINER        = pipeline-data

  2. Enable Internet: right panel → Internet ON

Cell structure (run top to bottom):
  0. Install + secrets
  1. Config
  2. SparkSession
  3. Download raw.jsonl from Azure
  4. Stage 1 — Preprocess
  5. Stage 2 — Train models
  6. Stage 3 — Cost analysis
  7. Upload all outputs back to Azure
"""

# ── Cell 0: Install dependencies & load Azure secrets ─────────────────────────
# Uncomment and run this cell FIRST (once per session):

# !pip install pyspark azure-storage-blob nltk matplotlib --quiet

# import nltk
# nltk.download("stopwords", quiet=True)

# Load secrets from Kaggle Secrets
from kaggle_secrets import UserSecretsClient
secrets = UserSecretsClient()

import os
os.environ["AZURE_STORAGE_ACCOUNT"] = secrets.get_secret("AZURE_STORAGE_ACCOUNT")
os.environ["AZURE_STORAGE_KEY"]     = secrets.get_secret("AZURE_STORAGE_KEY")
os.environ["AZURE_CONTAINER"]       = secrets.get_secret("AZURE_CONTAINER")

print("Azure credentials loaded from Kaggle Secrets ✓")

# ── Cell 1: Config ─────────────────────────────────────────────────────────────
import os, time, json
from pathlib import Path
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
STORAGE_KEY     = os.environ["AZURE_STORAGE_KEY"]
CONTAINER       = os.environ["AZURE_CONTAINER"]

# ── Azure Blob SDK helpers ─────────────────────────────────────────────────────
CONN_STR = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT};"
    f"AccountKey={STORAGE_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_svc = BlobServiceClient.from_connection_string(CONN_STR)
cc       = blob_svc.get_container_client(CONTAINER)


def upload_file(local_path, blob_name):
    with open(local_path, "rb") as f:
        cc.upload_blob(name=blob_name, data=f, overwrite=True)
    size = os.path.getsize(local_path)
    print(f"  ↑ {blob_name}  ({size:,} bytes)")


def upload_json(data, blob_name):
    cc.upload_blob(
        name=blob_name,
        data=json.dumps(data, indent=2).encode(),
        overwrite=True,
    )
    print(f"  ↑ {blob_name}")


def upload_dir(local_dir, blob_prefix):
    """Recursively upload a directory to Azure Blob."""
    for root, _, files in os.walk(local_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel        = os.path.relpath(local_path, local_dir)
            blob_name  = f"{blob_prefix}/{rel}".replace("\\", "/")
            upload_file(local_path, blob_name)


def download_blob(blob_name, local_path):
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    data = cc.download_blob(blob_name).readall()
    with open(local_path, "wb") as f:
        f.write(data)
    print(f"  ↓ {blob_name}  →  {local_path}  ({len(data):,} bytes)")


# ── Local working paths ────────────────────────────────────────────────────────
WORK_DIR       = "/kaggle/working"
LOCAL_JSONL    = f"{WORK_DIR}/raw.jsonl"
OUTPUT_PARQUET = f"{WORK_DIR}/output/processed"
SAMPLE_PARQUET = f"{WORK_DIR}/output/sample"
MODELS_DIR     = f"{WORK_DIR}/models"
EVAL_PATH      = f"{WORK_DIR}/evaluation.json"
COST_PATH      = f"{WORK_DIR}/cost_report.json"
STATS_PATH     = f"{WORK_DIR}/stats.json"

os.makedirs(f"{WORK_DIR}/output", exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

# ── Pipeline switches ──────────────────────────────────────────────────────────
SAMPLE_MODE   = False    # True = 50k convs (fast test)
SAMPLE_SIZE   = 50_000
PRICING_MODEL = "gpt-4o"
CV_FOLDS      = 3

print(f"Storage account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print(f"Sample mode     : {SAMPLE_MODE}")

# ── Cell 2: SparkSession ───────────────────────────────────────────────────────
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TokenOptimizer-Azure")
    .config("spark.driver.memory",          "12g")
    .config("spark.driver.maxResultSize",   "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled",   "true")
    .config("spark.local.dir",              f"{WORK_DIR}/spark_tmp")
    .config("spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -XX:G1HeapRegionSize=32m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} ready")

# ── Cell 3: Download raw.jsonl from Azure Blob ─────────────────────────────────
print("\n=== Downloading raw.jsonl from Azure Blob ===")
t_dl = time.time()
download_blob("data/raw.jsonl", LOCAL_JSONL)
print(f"Download complete in {time.time()-t_dl:.1f}s")

# ── Cell 4: Stage 1 — Preprocessing ───────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

t0 = time.time()
print("\n=== Stage 1: Preprocessing ===")

raw_schema = StructType([
    StructField("id", StringType(), True),
    StructField("conversations", ArrayType(
        StructType([
            StructField("from",  StringType(), True),
            StructField("value", StringType(), True),
        ])
    ), True),
])

df_raw = spark.read.schema(raw_schema).json(LOCAL_JSONL)
if SAMPLE_MODE:
    df_raw = df_raw.limit(SAMPLE_SIZE)
    print(f"SAMPLE MODE: {SAMPLE_SIZE:,} conversations")

total_convs = df_raw.count()
print(f"Loaded {total_convs:,} conversations")

_FILLER_RE = "(?i)(please |could you |would you mind |kindly |i would like you to |can you |tell me |help me understand )"

df_indexed = df_raw.select(
    F.col("id").alias("record_id"),
    F.posexplode("conversations").alias("turn_index", "turn"),
    F.size("conversations").alias("conversation_turn_count"),
)

df_feats = (
    df_indexed
    .withColumn("role",     F.col("turn.from"))
    .withColumn("raw_text", F.coalesce(F.col("turn.value"), F.lit("")))
    .drop("turn")
    .withColumn("char_count",   F.length("raw_text"))
    .withColumn("word_count",   F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
    .withColumn("token_count",  F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
    .withColumn("sentence_count",
                F.greatest(F.lit(1),
                    F.size(F.split(F.col("raw_text"), r"[.!?]+")) - F.lit(1)))
    .withColumn("avg_word_length",
                F.col("char_count") / F.greatest(F.col("word_count"), F.lit(1)))
    .withColumn("has_code_block", F.col("raw_text").contains("```"))
    .withColumn("_opt1", F.lower(F.col("raw_text")))
    .withColumn("_opt2", F.regexp_replace(F.col("_opt1"), _FILLER_RE, " "))
    .withColumn("_opt3", F.regexp_replace(F.col("_opt2"), r"[^\w\s]", ""))
    .withColumn("optimized_text",
                F.trim(F.regexp_replace(F.col("_opt3"), r"\s+", " ")))
    .drop("_opt1", "_opt2", "_opt3")
    .withColumn("optimized_token_count",
                F.size(F.split(F.trim(F.col("optimized_text")), r"\s+")))
    .withColumn("token_savings",
                F.col("token_count") - F.col("optimized_token_count"))
    .withColumn("savings_pct",
                F.when(F.col("token_count") > 0,
                       F.col("token_savings") / F.col("token_count") * 100.0)
                .otherwise(F.lit(0.0)))
    .withColumn("is_human",
                F.when(F.col("role") == "human", True).otherwise(False))
)

df_clean = (
    df_feats
    .filter(F.col("raw_text").isNotNull())
    .filter(F.length(F.trim(F.col("raw_text"))) > 0)
    .filter(F.col("token_count") > 0)
)

out_path = SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET
df_clean.coalesce(1 if SAMPLE_MODE else 4).write.mode("overwrite").parquet(out_path)
df_clean.limit(10_000).coalesce(1).write.mode("overwrite").parquet(SAMPLE_PARQUET)

stats_row = df_clean.agg(
    F.count("*").alias("total_turns"),
    F.countDistinct("record_id").alias("total_conversations"),
    F.mean("token_count").alias("avg_token_count"),
    F.mean("savings_pct").alias("avg_savings_pct"),
    F.sum("token_savings").alias("total_tokens_saved"),
).collect()[0]

preprocess_stats = stats_row.asDict()
preprocess_stats["elapsed_seconds"] = round(time.time() - t0, 2)

with open(STATS_PATH, "w") as f:
    json.dump({k: float(v) if v else 0 for k, v in preprocess_stats.items()}, f, indent=2)

print(f"✅ Preprocessing done in {preprocess_stats['elapsed_seconds']:.1f}s")
print(f"   Conversations : {int(preprocess_stats['total_conversations']):,}")
print(f"   Turns         : {int(preprocess_stats['total_turns']):,}")
print(f"   Avg tokens    : {preprocess_stats['avg_token_count']:.1f}")
print(f"   Avg savings   : {preprocess_stats['avg_savings_pct']:.1f}%")

# ── Cell 5: Stage 2 — Model Training ──────────────────────────────────────────
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

print("\n=== Stage 2: Model Training ===")
t0 = time.time()

FEATURE_COLS = [
    "char_count", "word_count", "sentence_count", "avg_word_length",
    "conversation_turn_count", "turn_index", "has_code_block_int",
]
TARGET_COL, OPT_TARGET = "token_count", "optimized_token_count"

src = SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET
df  = spark.read.parquet(src)
df  = df.withColumn("has_code_block_int", F.col("has_code_block").cast("int"))
df  = df.dropna(subset=FEATURE_COLS + [TARGET_COL, OPT_TARGET])
total = df.count()
print(f"Training on {total:,} turns")

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache(); test_df.cache()


def build_pipe(regressor, target):
    asm = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features", handleInvalid="skip")
    scl = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    return Pipeline(stages=[asm, scl, regressor.setLabelCol(target).setFeaturesCol("features")])


def evaluate(model, df, target):
    preds = model.transform(df)
    ev = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    return {m: ev.setMetricName(m).evaluate(preds) for m in ["rmse", "mae", "r2"]}


results = {}

# Ridge
print("🔵 Ridge Regression...")
ridge = LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal")
t1 = time.time()
ridge_model   = build_pipe(ridge, TARGET_COL).fit(train_df)
ridge_metrics = evaluate(ridge_model, test_df, TARGET_COL)
ridge_metrics["training_time_s"] = round(time.time() - t1, 2)
results["ridge_token_count"] = ridge_metrics
ridge_model.write().overwrite().save(f"{MODELS_DIR}/ridge_token_count")
print(f"   R²={ridge_metrics['r2']:.4f}  RMSE={ridge_metrics['rmse']:.3f}")

best_model_name, best_r2 = "ridge_token_count", ridge_metrics["r2"]

# GBT (auto-selected if Ridge R² < 0.70)
if ridge_metrics["r2"] < 0.70:
    print("⚠️  Ridge R²<0.70. Training GBT...")
    gbt = GBTRegressor(maxIter=50, maxDepth=5, stepSize=0.1)
    t2 = time.time()
    gbt_model   = build_pipe(gbt, TARGET_COL).fit(train_df)
    gbt_metrics = evaluate(gbt_model, test_df, TARGET_COL)
    gbt_metrics["training_time_s"] = round(time.time() - t2, 2)
    results["gbt_token_count"] = gbt_metrics
    gbt_model.write().overwrite().save(f"{MODELS_DIR}/gbt_token_count")
    print(f"   GBT R²={gbt_metrics['r2']:.4f}")
    if gbt_metrics["r2"] > best_r2:
        best_model_name, best_r2 = "gbt_token_count", gbt_metrics["r2"]

# Ridge for optimized token count
print("🟣 Ridge (optimized_token_count)...")
ridge_opt = LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal")
ridge_opt_model = build_pipe(ridge_opt, OPT_TARGET).fit(train_df)
results["ridge_opt_token_count"] = evaluate(ridge_opt_model, test_df, OPT_TARGET)
ridge_opt_model.write().overwrite().save(f"{MODELS_DIR}/ridge_opt_token_count")

# Cross-validated Ridge
print(f"🔁 Cross-validated Ridge ({CV_FOLDS} folds)...")
cv_ridge   = LinearRegression(elasticNetParam=0.0, maxIter=100, solver="normal")
param_grid = ParamGridBuilder().addGrid(cv_ridge.regParam, [0.01, 0.1, 1.0]).build()
ev         = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse")
cv = CrossValidator(estimator=build_pipe(cv_ridge, TARGET_COL),
                    estimatorParamMaps=param_grid, evaluator=ev,
                    numFolds=CV_FOLDS, parallelism=2)
cv_model   = cv.fit(train_df)
cv_metrics = evaluate(cv_model.bestModel, test_df, TARGET_COL)
cv_metrics["best_regParam"] = float(cv_model.bestModel.stages[-1]._java_obj.getRegParam())
results["cv_ridge_token_count"] = cv_metrics
cv_model.bestModel.write().overwrite().save(f"{MODELS_DIR}/cv_ridge_token_count")
print(f"   CV R²={cv_metrics['r2']:.4f}  bestRegParam={cv_metrics['best_regParam']}")

train_summary = {
    "best_model": best_model_name, "best_r2": best_r2,
    "total_training_time_s": round(time.time() - t0, 2),
    "dataset_size": total, "models": results,
}
with open(EVAL_PATH, "w") as f:
    json.dump(train_summary, f, indent=2)

print(f"\n🏆 Best model : {best_model_name}  (R²={best_r2:.4f})")
print(f"⏱️  Total time  : {train_summary['total_training_time_s']:.1f}s")

# ── Cell 6: Stage 3 — Cost Analysis ───────────────────────────────────────────
print("\n=== Stage 3: Cost Analysis ===")
t0 = time.time()

PRICING = {
    "gpt-4o":          {"input": 0.005,   "output": 0.015},
    "gpt-4-turbo":     {"input": 0.010,   "output": 0.030},
    "gpt-3.5-turbo":   {"input": 0.0015,  "output": 0.002},
    "claude-3-opus":   {"input": 0.015,   "output": 0.075},
    "claude-3-sonnet": {"input": 0.003,   "output": 0.015},
    "claude-3-haiku":  {"input": 0.00025, "output": 0.00125},
}
prices    = PRICING[PRICING_MODEL]
in_r, out_r = prices["input"], prices["output"]

df = spark.read.parquet(SAMPLE_PARQUET if SAMPLE_MODE else OUTPUT_PARQUET)
df = (
    df
    .withColumn("cost_raw_usd",
        F.when(F.col("role") == "human", F.col("token_count")           / 1000.0 * in_r)
         .otherwise(F.col("token_count")                                 / 1000.0 * out_r))
    .withColumn("cost_opt_usd",
        F.when(F.col("role") == "human", F.col("optimized_token_count") / 1000.0 * in_r)
         .otherwise(F.col("optimized_token_count")                       / 1000.0 * out_r))
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
    .withColumn("conv_savings_pct",
                F.when(F.col("conv_cost_raw") > 0,
                       F.col("conv_cost_saved") / F.col("conv_cost_raw") * 100.0)
                .otherwise(F.lit(0.0)))
)

t = conv.agg(
    F.count("*").alias("total_conversations"),
    F.sum("conv_cost_raw").alias("total_cost_raw_usd"),
    F.sum("conv_cost_opt").alias("total_cost_opt_usd"),
    F.sum("conv_cost_saved").alias("total_cost_saved_usd"),
    F.sum("conv_tokens_raw").alias("total_tokens_raw"),
    F.sum("conv_tokens_opt").alias("total_tokens_opt"),
    F.mean("conv_savings_pct").alias("avg_savings_pct"),
).collect()[0].asDict()

model_comparison = {}
for m, p in PRICING.items():
    rc = t["total_tokens_raw"]*0.6/1000*p["input"] + t["total_tokens_raw"]*0.4/1000*p["output"]
    oc = t["total_tokens_opt"]*0.6/1000*p["input"] + t["total_tokens_opt"]*0.4/1000*p["output"]
    model_comparison[m] = {
        "raw_cost_usd": round(rc, 4), "opt_cost_usd": round(oc, 4),
        "saved_usd": round(rc - oc, 4),
        "savings_pct": round((rc - oc) / rc * 100, 2) if rc > 0 else 0.0,
    }

cost_report = {
    "model": PRICING_MODEL, "pricing": prices,
    "elapsed_seconds": round(time.time() - t0, 2),
    "totals": {k: (round(float(v), 6) if v else 0) for k, v in t.items()},
    "all_model_comparison": model_comparison,
}
with open(COST_PATH, "w") as f:
    json.dump(cost_report, f, indent=2)

print(f"💰 {PRICING_MODEL}: raw=${t.get('total_cost_raw_usd',0):.4f}  "
      f"opt=${t.get('total_cost_opt_usd',0):.4f}  "
      f"saved=${t.get('total_cost_saved_usd',0):.4f} "
      f"({t.get('avg_savings_pct',0):.1f}% avg)")

import pandas as pd
comp_df = pd.DataFrame(model_comparison).T.reset_index().rename(columns={"index": "model"})
print(comp_df.to_string(index=False))

spark.stop()

# ── Cell 7: Upload everything back to Azure Blob ───────────────────────────────
print("\n=== Uploading outputs to Azure Blob Storage ===")

# JSON reports
upload_json(preprocess_stats, "output/stats.json")
upload_json(train_summary,    "output/evaluation.json")
upload_json(cost_report,      "output/cost_report.json")

# Parquet datasets
print("Uploading Parquet datasets...")
upload_dir(OUTPUT_PARQUET, "output/processed")
upload_dir(SAMPLE_PARQUET, "output/sample")

# Trained models
print("Uploading models...")
upload_dir(MODELS_DIR, "models")

print("\n✅ All outputs uploaded to Azure Blob!")
print(f"   Container  : {CONTAINER}")
print(f"   Account    : {STORAGE_ACCOUNT}")
print(f"\n   output/stats.json")
print(f"   output/evaluation.json")
print(f"   output/cost_report.json")
print(f"   output/processed/  (Parquet)")
print(f"   output/sample/     (Parquet)")
print(f"   models/            (Spark MLlib models)")
