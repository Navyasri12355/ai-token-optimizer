"""
Optimized training script for OUTPUT token model only.
Focuses on improving metrics with enhanced features and tuned hyperparameters.
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
from pyspark.ml.feature import VectorAssembler, StandardScaler, PolynomialExpansion
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------------------------------------------------------------------
# 1. Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("OutputTokenOptimizer")
    .config("spark.driver.memory",           "4g")
    .config("spark.executor.memory",         "4g")
    .config("spark.sql.shuffle.partitions",  "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Load preprocessed Parquet
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
# 3. Feature engineering (enhanced)
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

# Fix preprocessing bugs
df = df.withColumn("word_count",
                   F.greatest(F.lit(1.0), F.col("text_len") / F.lit(5.0)))
df = df.withColumn("avg_word_len_fixed",
                   F.col("text_len") / F.greatest(F.lit(1.0), F.col("word_count")))

# Log-transform features
df = (
    df
    .withColumn("log_text_len",      F.log1p(F.col("text_len")))
    .withColumn("log_context_len",   F.log1p(F.col("context_len")))
    .withColumn("log_word_count",    F.log1p(F.col("word_count")))
    .withColumn("log_input_tokens",  F.log1p(F.col("input_tokens")))
    .withColumn("log_output_tokens", F.log1p(F.col("output_tokens")))
    .withColumn("log_turn_pos",      F.log1p(F.col("turn_pos")))
)

# Conversational context features
df = df.withColumn("is_first_turn",
                   (F.col("turn_pos") <= 1.0).cast("double"))

# Token density
df = df.withColumn("token_density",
                   F.col("input_tokens") / F.greatest(F.lit(1.0), F.col("text_len")))

# Prompt length buckets
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

# Interaction terms
df = df.withColumn("interaction_q_tokens",
                   F.col("question_flag") * F.col("log_input_tokens"))
df = df.withColumn("log_turn_x_tokens",
                   F.col("log_turn_pos") * F.col("log_input_tokens"))

# --- ENHANCED: More powerful interaction features ---
# Response complexity proxy: questions with longer context
df = df.withColumn("q_context_interaction",
                   F.col("question_flag") * F.col("log_context_len"))

# Turn position × text length (later turns with longer text are more complex)
df = df.withColumn("turn_text_interaction",
                   F.col("log_turn_pos") * F.col("log_text_len"))

# Token density × prompt length (code detection)
df = df.withColumn("density_length_interaction",
                   F.col("token_density") * F.col("log_input_tokens"))

# Non-linear features for better expressiveness
df = df.withColumn("text_len_squared",
                   F.col("log_text_len") * F.col("log_text_len"))
df = df.withColumn("input_tokens_squared",
                   F.col("log_input_tokens") * F.col("log_input_tokens"))

# Turn progression complexity
df = df.withColumn("turn_pos_input_tokens",
                   F.col("turn_pos") * F.col("log_input_tokens"))

# Context-to-input ratio (how rich the conversation is)
df = df.withColumn("context_input_ratio",
                   F.col("log_context_len") / 
                   F.greatest(F.lit(0.1), F.col("log_input_tokens")))

# ---------------------------------------------------------------------------
# 4. Clip extreme outliers (top 0.5%)
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
# 5. Enhanced OUTPUT feature set
# ---------------------------------------------------------------------------
OUTPUT_FEATURE_COLS = [
    # Core prompt features
    "log_text_len",
    "log_word_count",
    "avg_word_len_fixed",
    "question_flag",
    "log_context_len",
    # Input token count (strongest predictor)
    "log_input_tokens",
    # Conversational context
    "log_turn_pos",
    "is_first_turn",
    # Token density
    "token_density",
    # Prompt length buckets
    "prompt_short_flag",
    "prompt_medium_flag",
    "prompt_long_flag",
    # Original interaction terms
    "interaction_q_tokens",
    "log_turn_x_tokens",
    # ENHANCED: New interaction terms
    "q_context_interaction",
    "turn_text_interaction",
    "density_length_interaction",
    # ENHANCED: Non-linear features
    "text_len_squared",
    "input_tokens_squared",
    "turn_pos_input_tokens",
    "context_input_ratio",
]

df.cache()
total = df.count()
print(f"\n   Total rows after clipping: {total:,}")
print(f"   Output feature count: {len(OUTPUT_FEATURE_COLS)}")

# ---------------------------------------------------------------------------
# 6. Train / Test split
# ---------------------------------------------------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"   Train: {train_df.count():,}  |  Test: {test_df.count():,}")

# ---------------------------------------------------------------------------
# 7. Helpers
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
# 8. Train & Evaluate OUTPUT model with hyperparameter tuning
# ---------------------------------------------------------------------------
print(f"\n{'='*70}")
print(f"  OPTIMIZED OUTPUT TOKEN MODEL TRAINING")
print(f"  TARGET: log_output_tokens | FEATURES: {len(OUTPUT_FEATURE_COLS)}")
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

# --- Optimized GBT with better hyperparameters ---
# Increased iterations, deeper trees, but with regularization
gbt = GBTRegressor(
    labelCol="log_output_tokens",
    featuresCol="features",
    maxIter=150,           # Increased from 100 for better convergence
    maxDepth=10,           # Increased from 8 for better feature interactions
    stepSize=0.03,         # Reduced from 0.05 for more stability
    subsamplingRate=0.9,   # Increased from 0.8 for better samples
    minInstancesPerNode=3, # Reduced from 5 for finer splits
    minInfoGain=0.0,       # Ensure all splits are meaningful
    seed=42,
)

print("  [GBT] Fitting GBTRegressor with optimized hyperparameters ...")
model_gbt = Pipeline(stages=[assembler, scaler, gbt]).fit(train_df)
preds_gbt = model_gbt.transform(test_df)

rmse_gbt  = _evaluator("log_output_tokens", "rmse").evaluate(preds_gbt)
r2_gbt    = _evaluator("log_output_tokens", "r2").evaluate(preds_gbt)
mae_gbt   = _evaluator("log_output_tokens", "mae").evaluate(preds_gbt)
orig_rmse = _orig_rmse(preds_gbt, "log_output_tokens")

print(f"\n  [METRICS] log-RMSE={rmse_gbt:.4f}  |  R2={r2_gbt:.4f}  |  MAE={mae_gbt:.4f}")
print(f"            Original-scale RMSE={orig_rmse:.1f} tokens")

# Sample predictions
print(f"\n  [SAMPLE] Actual vs Predicted (original token scale):")
preds_gbt.withColumn("predicted_tokens", F.round(F.expm1("prediction"), 0)) \
          .withColumn("actual_tokens",    F.round(F.expm1("log_output_tokens"), 0)) \
          .select("actual_tokens", "predicted_tokens", "log_text_len") \
          .limit(10).show(truncate=False)

# Feature importance
print(f"\n  [FEATURE IMPORTANCE]")
gbt_model = model_gbt.stages[-1]
feature_importance = list(zip(OUTPUT_FEATURE_COLS, gbt_model.featureImportances))
feature_importance.sort(key=lambda x: x[1], reverse=True)
for fname, importance in feature_importance[:15]:
    print(f"    {fname:.<40} {importance:.4f}")

# Save the model
save_path = f"{MODEL_DIR}/output_token_model"
print(f"\n  [SAVE] Saving optimized model to {save_path} ...")
model_gbt.write().overwrite().save(save_path)

# ---------------------------------------------------------------------------
# 10. Final summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("  OPTIMIZED OUTPUT MODEL - FINAL METRICS")
print("=" * 70)
print(f"  R2 Score:              {r2_gbt:.4f}")
print(f"  Log-scale RMSE:        {rmse_gbt:.4f}")
print(f"  MAE:                   {mae_gbt:.4f}")
print(f"  Original-scale RMSE:   {orig_rmse:.1f} tokens")
print(f"  Model saved:           {save_path}")
print("=" * 70)

df.unpersist()
spark.stop()
print("\n[DONE] Output model training completed successfully!")
