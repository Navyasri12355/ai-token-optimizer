"""
Hybrid model: segment-specific ML + heuristic blending
Trains separate models for code/question/general and blends predictions
"""

import os
import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import glob
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------------------------------------------------------------------
# 1. Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("HybridOutputTokenModel")
    .config("spark.driver.memory",           "4g")
    .config("spark.executor.memory",         "4g")
    .config("spark.sql.shuffle.partitions",  "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Load enhanced parquet
# ---------------------------------------------------------------------------
PARQUET_DIR = "data/processed_enhanced.parquet"
MODEL_DIR   = "ml/models"

if not os.path.isdir(PARQUET_DIR):
    print(f"\n[ERROR] {PARQUET_DIR} not found.")
    print("   Please run: python ml/enhance_preprocessing.py")
    spark.stop()
    sys.exit(1)

print(f"[*] Loading enhanced parquet from {PARQUET_DIR} ...")
df = spark.read.parquet(PARQUET_DIR)

# Cast target
df = df.withColumn("log_output_tokens", F.log1p(F.col("output_tokens")))

total = df.count()
print(f"   Total rows: {total:,}")

# ---------------------------------------------------------------------------
# 3. Re-derive segment column with tighter thresholds
#    The enhanced parquet used very loose thresholds that classify ~100% of
#    rows as 'code' (token_density_ratio > 0.08, word_text_ratio < 0.15).
#    We override here so all three segments get meaningful populations.
# ---------------------------------------------------------------------------
print(f"[*] Re-deriving segment labels with tighter thresholds ...")

# Recompute ratios in case they differ (they're cheap)
df = df.withColumn(
    "_token_density",
    F.col("input_tokens") / F.greatest(F.lit(1.0), F.col("text_len"))
).withColumn(
    "_word_text",
    F.col("num_words") / F.greatest(F.lit(1.0), F.col("text_len"))
)

# Tighter code signal:
#   token_density > 0.25  (very dense — genuine code/markup)
#   OR word_text_ratio < 0.07  (extremely sparse words — symbols, brackets)
df = df.withColumn(
    "segment",
    F.when(
        (F.col("_token_density") > 0.25) | (F.col("_word_text") < 0.07),
        "code"
    ).when(
        F.col("question_flag") == 1,
        "question"
    ).otherwise("general")
)

df = df.drop("_token_density", "_word_text")

print(f"[*] Segment distribution after re-labelling:")
seg_counts = df.groupBy("segment").count().orderBy("segment").collect()
for row in seg_counts:
    pct = (row["count"] / total) * 100
    print(f"   {row['segment']:<12} {row['count']:>10,}  ({pct:>5.1f}%)")

# --- Feature columns for segmented models ---
# Use only columns available in enhanced parquet
OUTPUT_FEATURE_COLS_BASE = [
    "text_len",
    "context_len",
    "num_words",
    "avg_word_len",
    "question_flag",
    "input_tokens",
    "turn_pos",
]

# ENHANCED features (proxy-based, no prompt text available)
OUTPUT_FEATURE_COLS = OUTPUT_FEATURE_COLS_BASE + [
    "token_density_ratio",
    "word_text_ratio",
    "is_code_indicator",
    "is_long_form",
    "is_deep_conversation",
]

# ---------------------------------------------------------------------------
# 4. Helper functions
# ---------------------------------------------------------------------------
def _evaluator(label_col: str, metric: str) -> RegressionEvaluator:
    return RegressionEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName=metric,
    )

def _orig_rmse(preds_df, log_label_col: str) -> float:
    tmp = (
        preds_df
        .withColumn("pred_orig",  F.expm1("prediction"))
        .withColumn("label_orig", F.expm1(log_label_col))
        .withColumn("sq_err",     (F.col("pred_orig") - F.col("label_orig")) ** 2)
    )
    mse = tmp.agg(F.mean("sq_err")).collect()[0][0]
    return math.sqrt(mse)

# ---------------------------------------------------------------------------
# 5. Train segment-specific models
# ---------------------------------------------------------------------------
def train_segment_model(train_data, test_data, segment_name: str):
    """Train a GBT model for a specific content segment"""
    
    segment_size = train_data.count()
    test_size = test_data.count()
    
    print(f"\n{'='*70}")
    print(f"  SEGMENT: {segment_name.upper()}")
    print(f"  Train: {segment_size:,}  |  Test: {test_size:,}")
    print(f"{'='*70}")
    
    assembler = VectorAssembler(
        inputCols=OUTPUT_FEATURE_COLS,
        outputCol="raw_features",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    
    # GBT tuned for smaller segment sizes
    gbt = GBTRegressor(
        labelCol="log_output_tokens",
        featuresCol="features",
        maxIter=100,
        maxDepth=9,
        stepSize=0.04,
        subsamplingRate=0.85,
        minInstancesPerNode=4,
        seed=42,
    )
    
    print(f"  [GBT] Fitting model ...")
    model = Pipeline(stages=[assembler, scaler, gbt]).fit(train_data)
    preds = model.transform(test_data)
    
    rmse = _evaluator("log_output_tokens", "rmse").evaluate(preds)
    r2 = _evaluator("log_output_tokens", "r2").evaluate(preds)
    mae = _evaluator("log_output_tokens", "mae").evaluate(preds)
    orig_rmse_val = _orig_rmse(preds, "log_output_tokens")
    
    print(f"  [METRICS]")
    print(f"    Log-RMSE: {rmse:.4f}")
    print(f"    R²:       {r2:.4f}")
    print(f"    MAE:      {mae:.4f}")
    print(f"    Orig-RMSE: {orig_rmse_val:.1f} tokens")
    
    # Feature importance (top 10)
    gbt_stage = model.stages[-1]
    importance_list = list(zip(OUTPUT_FEATURE_COLS, gbt_stage.featureImportances))
    importance_list.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\n  [TOP FEATURES]")
    for fname, imp in importance_list[:10]:
        print(f"    {fname:.<35} {imp:.4f}")
    
    # Save model
    save_path = f"{MODEL_DIR}/output_token_model_{segment_name}"
    print(f"\n  [SAVE] Saving to {save_path} ...")
    model.write().overwrite().save(save_path)
    
    return model, {
        "r2": r2,
        "rmse": rmse,
        "mae": mae,
        "orig_rmse": orig_rmse_val,
    }, preds

# ---------------------------------------------------------------------------
# 6. Train models for each segment
# ---------------------------------------------------------------------------
print(f"\n[*] Preparing segment data ...")

segments = {}
overall_metrics = {}

for segment_name in ["code", "question", "general"]:
    segment_df = df.filter(F.col("segment") == segment_name).cache()
    count = segment_df.count()
    
    if count < 100:
        print(f"   ⚠️  Segment '{segment_name}' too small ({count} rows) — skipping")
        continue
    
    train_seg, test_seg = segment_df.randomSplit([0.8, 0.2], seed=42)
    segments[segment_name] = {
        "train": train_seg,
        "test": test_seg,
        "count": count,
    }
    print(f"   ✅ {segment_name:.<15} {count:>10,} rows")

# Train each segment
for segment_name in segments.keys():
    model, metrics, preds = train_segment_model(
        segments[segment_name]["train"],
        segments[segment_name]["test"],
        segment_name
    )
    overall_metrics[segment_name] = metrics
    segments[segment_name]["model"] = model
    segments[segment_name]["preds"] = preds

# ---------------------------------------------------------------------------
# 7. Overall summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("  HYBRID MODEL - SEGMENT SUMMARY")
print("=" * 70)

for segment_name, metrics in overall_metrics.items():
    print(f"\n  [{segment_name.upper()}]")
    print(f"    R² Score:         {metrics['r2']:.4f}")
    print(f"    Log-scale RMSE:   {metrics['rmse']:.4f}")
    print(f"    MAE:              {metrics['mae']:.4f}")
    print(f"    Original RMSE:    {metrics['orig_rmse']:.1f} tokens")

# Aggregate average
if overall_metrics:
    avg_r2 = sum(m['r2'] for m in overall_metrics.values()) / len(overall_metrics)
    avg_rmse = sum(m['orig_rmse'] for m in overall_metrics.values()) / len(overall_metrics)
    print(f"\n  [AVERAGE ACROSS SEGMENTS]")
    print(f"    Avg R²:           {avg_r2:.4f}")
    print(f"    Avg Orig-RMSE:    {avg_rmse:.1f} tokens")

print("\n" + "=" * 70)
print("  NEXT: Use hybrid_predictor.py for blended predictions")
print("=" * 70)

df.unpersist()
spark.stop()
print("\n[DONE] Hybrid model training complete!")
