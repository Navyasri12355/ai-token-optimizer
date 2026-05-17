"""
Distributed Spark MLlib training pipeline for token prediction models.

Two separate models with tailored feature sets:
  - INPUT  model: features derived from prompt text only
  - OUTPUT model: prompt features + log_input_tokens + conversational context
                  (all known BEFORE the API call, so no data leakage)
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
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------------------------------------------------------------------
# 1.  Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TokenOptimizerMLlib")
    .config("spark.driver.memory",           "4g")
    .config("spark.executor.memory",         "4g")
    .config("spark.sql.shuffle.partitions",  "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2.  Load preprocessed Parquet
# ---------------------------------------------------------------------------
PARQUET_DIR = "data/processed.parquet"
MODEL_DIR   = "ml/models"

_parquet_files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))
if not os.path.isdir(PARQUET_DIR) or not _parquet_files:
    print(f"\n[ERROR] No parquet files found in '{PARQUET_DIR}'.")
    print("   Please run:  python spark/preprocess_chunked.py")
    spark.stop()
    sys.exit(1)

print(f"[*] Loading {len(_parquet_files)} parquet file(s) from {PARQUET_DIR} ...")
df = spark.read.parquet(PARQUET_DIR)

# ---------------------------------------------------------------------------
# 3.  Feature engineering
#     Fixes preprocessing bugs and adds richer features — no re-preprocessing.
# ---------------------------------------------------------------------------

# Cast raw columns
df = (
    df
    .withColumn("text_len",      F.col("text_len").cast("double"))
    .withColumn("context_len",   F.col("context_len").cast("double"))
    .withColumn("input_tokens",  F.col("input_tokens").cast("double"))
    .withColumn("output_tokens", F.col("output_tokens").cast("double"))
    .withColumn("question_flag", F.col("question_flag").cast("double"))
    .withColumn("turn_pos",      F.col("turn_pos").cast("double"))
)

# --- Fix preprocessing bugs ---
# num_words was non-whitespace char count; re-approximate as text_len / 5
df = df.withColumn("word_count",
                   F.greatest(F.lit(1.0), F.col("text_len") / F.lit(5.0)))
# avg_word_len was always 1.0; re-derive
df = df.withColumn("avg_word_len_fixed",
                   F.col("text_len") / F.greatest(F.lit(1.0), F.col("word_count")))

# --- Log-transform features (compress long-tail for tree splits) ---
df = (
    df
    .withColumn("log_text_len",      F.log1p(F.col("text_len")))
    .withColumn("log_context_len",   F.log1p(F.col("context_len")))
    .withColumn("log_word_count",    F.log1p(F.col("word_count")))
    .withColumn("log_input_tokens",  F.log1p(F.col("input_tokens")))
    .withColumn("log_output_tokens", F.log1p(F.col("output_tokens")))
    .withColumn("log_turn_pos",      F.log1p(F.col("turn_pos")))
)

# --- Conversational context features ---
df = df.withColumn("is_first_turn",
                   (F.col("turn_pos") <= 1.0).cast("double"))

# --- Token density: captures code/math (low chars/token) vs prose (high) ---
df = df.withColumn("token_density",
                   F.col("input_tokens") / F.greatest(F.lit(1.0), F.col("text_len")))

# --- Prompt length bucket flags ---
# Short prompts (<30 tokens): conversational, typically get shorter replies
# Long prompts  (>400 tokens): code/technical, typically get longer replies
df = (
    df
    .withColumn("prompt_short_flag",
                (F.col("input_tokens") < 30).cast("double"))
    .withColumn("prompt_long_flag",
                (F.col("input_tokens") > 400).cast("double"))
    .withColumn("prompt_medium_flag",
                ((F.col("input_tokens") >= 30) &
                 (F.col("input_tokens") <= 400)).cast("double"))
)

# --- Interaction: question × input_tokens (long questions behave differently) ---
df = df.withColumn("interaction_q_tokens",
                   F.col("question_flag") * F.col("log_input_tokens"))

# --- Complexity proxy: avg words per turn (long-turn conversations) ---
df = df.withColumn("log_turn_x_tokens",
                   F.col("log_turn_pos") * F.col("log_input_tokens"))

# ---------------------------------------------------------------------------
# 4.  Clip extreme outliers (top 0.5%)
# ---------------------------------------------------------------------------
print("[*] Computing outlier thresholds ...")
p995_in  = df.approxQuantile("input_tokens",  [0.995], 0.005)[0]
p995_out = df.approxQuantile("output_tokens", [0.995], 0.005)[0]
print(f"   Clipping input_tokens  > {p995_in:,.0f}")
print(f"   Clipping output_tokens > {p995_out:,.0f}")
df = df.filter(
    (F.col("input_tokens")  <= p995_in) &
    (F.col("output_tokens") <= p995_out)
)

# ---------------------------------------------------------------------------
# 5.  Separate feature column lists
#     INPUT model : only prompt-derived features (nothing about the response)
#     OUTPUT model: prompt features + input_tokens + conversational context
#                   (all available at inference time before the API call)
# ---------------------------------------------------------------------------
INPUT_FEATURE_COLS = [
    "log_text_len",
    "log_word_count",
    "avg_word_len_fixed",
    "question_flag",
    "log_context_len",
    "log_turn_pos",
    "is_first_turn",
]

OUTPUT_FEATURE_COLS = [
    # Prompt text features
    "log_text_len",
    "log_word_count",
    "avg_word_len_fixed",
    "question_flag",
    "log_context_len",
    # Input token count — single strongest predictor of response length
    "log_input_tokens",
    # Conversational context
    "log_turn_pos",
    "is_first_turn",
    # Token density (code/math vs prose detection)
    "token_density",
    # Prompt length buckets
    "prompt_short_flag",
    "prompt_medium_flag",
    "prompt_long_flag",
    # Interaction terms
    "interaction_q_tokens",
    "log_turn_x_tokens",
]

df.cache()
total = df.count()
print(f"\n   Total rows after clipping: {total:,}")
print(f"   Input  feature count: {len(INPUT_FEATURE_COLS)}")
print(f"   Output feature count: {len(OUTPUT_FEATURE_COLS)}")

# ---------------------------------------------------------------------------
# 6.  Train / Test split
# ---------------------------------------------------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"   Train: {train_df.count():,}  |  Test: {test_df.count():,}")

# ---------------------------------------------------------------------------
# 7.  Helpers
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
# 8.  Train + Evaluate (accepts per-model feature list)
# ---------------------------------------------------------------------------
def train_and_evaluate(train_data, test_data,
                       log_label_col: str,
                       feature_cols: list,
                       model_name: str):
    print(f"\n{'='*62}")
    print(f"  TARGET : {log_label_col.upper()}  [{model_name}]")
    print(f"  FEATURES ({len(feature_cols)}): {feature_cols}")
    print(f"{'='*62}")

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    # -- Random Forest --
    rf = RandomForestRegressor(
        labelCol=log_label_col, featuresCol="features",
        numTrees=150, maxDepth=12,
        minInstancesPerNode=5, featureSubsetStrategy="auto",
        seed=42,
    )
    print("  [RF]  Fitting RandomForest ...")
    model_rf  = Pipeline(stages=[assembler, scaler, rf]).fit(train_data)
    preds_rf  = model_rf.transform(test_data)
    rmse_rf   = _evaluator(log_label_col, "rmse").evaluate(preds_rf)
    r2_rf     = _evaluator(log_label_col, "r2").evaluate(preds_rf)
    orig_rf   = _orig_rmse(preds_rf, log_label_col)
    print(f"     RF  -> log-RMSE={rmse_rf:.4f}  R2={r2_rf:.4f}  orig-RMSE={orig_rf:.1f} tokens")

    # -- GBT --
    gbt = GBTRegressor(
        labelCol=log_label_col, featuresCol="features",
        maxIter=100, maxDepth=8,
        stepSize=0.05, subsamplingRate=0.8,
        minInstancesPerNode=5, seed=42,
    )
    print("  [GBT] Fitting GBTRegressor ...")
    model_gbt = Pipeline(stages=[assembler, scaler, gbt]).fit(train_data)
    preds_gbt = model_gbt.transform(test_data)
    rmse_gbt  = _evaluator(log_label_col, "rmse").evaluate(preds_gbt)
    r2_gbt    = _evaluator(log_label_col, "r2").evaluate(preds_gbt)
    orig_gbt  = _orig_rmse(preds_gbt, log_label_col)
    print(f"     GBT -> log-RMSE={rmse_gbt:.4f}  R2={r2_gbt:.4f}  orig-RMSE={orig_gbt:.1f} tokens")

    if rmse_rf <= rmse_gbt:
        best_model, best_preds, winner = model_rf, preds_rf, "RandomForest"
        best_metrics = {"log_rmse": rmse_rf, "r2": r2_rf, "orig_rmse": orig_rf}
    else:
        best_model, best_preds, winner = model_gbt, preds_gbt, "GBT"
        best_metrics = {"log_rmse": rmse_gbt, "r2": r2_gbt, "orig_rmse": orig_gbt}

    print(f"\n  [WIN] {winner} -> log-RMSE={best_metrics['log_rmse']:.4f}"
          f"  R2={best_metrics['r2']:.4f}  orig-RMSE={best_metrics['orig_rmse']:.1f} tokens")

    # Sample predictions (back-transformed)
    print(f"\n  [SAMPLE] {log_label_col} (original token scale):")
    best_preds.withColumn("predicted_tokens", F.round(F.expm1("prediction"), 0)) \
              .withColumn("actual_tokens",    F.round(F.expm1(log_label_col), 0)) \
              .select("log_text_len", "actual_tokens", "predicted_tokens") \
              .limit(8).show(truncate=False)

    save_path = f"{MODEL_DIR}/{model_name}"
    print(f"  [SAVE] Saving to {save_path} ...")
    best_model.write().overwrite().save(save_path)

    return best_model, best_metrics


# ---------------------------------------------------------------------------
# 9.  Train both models
# ---------------------------------------------------------------------------
model_input, metrics_in = train_and_evaluate(
    train_df, test_df,
    log_label_col="log_input_tokens",
    feature_cols=INPUT_FEATURE_COLS,
    model_name="input_token_model",
)

model_output, metrics_out = train_and_evaluate(
    train_df, test_df,
    log_label_col="log_output_tokens",
    feature_cols=OUTPUT_FEATURE_COLS,
    model_name="output_token_model",
)

# ---------------------------------------------------------------------------
# 10. Final summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 62)
print("  TRAINING SUMMARY")
print("=" * 62)
print(f"  INPUT  model -> R2={metrics_in['r2']:.4f}  "
      f"log-RMSE={metrics_in['log_rmse']:.4f}  "
      f"orig-RMSE={metrics_in['orig_rmse']:.1f} tokens")
print(f"  OUTPUT model -> R2={metrics_out['r2']:.4f}  "
      f"log-RMSE={metrics_out['log_rmse']:.4f}  "
      f"orig-RMSE={metrics_out['orig_rmse']:.1f} tokens")
print(f"\n  Models saved under: {MODEL_DIR}/")
print("=" * 62)

df.unpersist()
spark.stop()
print("\n[DONE] Training completed successfully!")
