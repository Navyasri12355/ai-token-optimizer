"""
Distributed Spark MLlib training pipeline for token prediction models.

Pipeline:
  1. Read preprocessed Parquet (output of spark/preprocess.py)
  2. Assemble feature vectors with VectorAssembler
  3. Train RandomForestRegressor + GradientBoostedTreeRegressor via Spark MLlib
  4. Evaluate with RegressionEvaluator
  5. Save PipelineModel artifacts to spark/models/

No pandas, no sklearn — 100% Spark / MLlib.
"""

import os
import glob
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import (
    RandomForestRegressor,
    GBTRegressor,
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ---------------------------------------------------------------------------
# 1.  Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TokenOptimizerMLlib")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2.  Load preprocessed Parquet
# ---------------------------------------------------------------------------
PARQUET_DIR = "data/processed.parquet"
MODEL_DIR   = "ml/models"

# Pre-flight check: ensure the parquet directory has actual part-files
_parquet_files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))
if not os.path.isdir(PARQUET_DIR) or not _parquet_files:
    print(f"\n❌  ERROR: No parquet files found in '{PARQUET_DIR}'.")
    print("   The preprocessing step has not completed successfully.")
    print("   Please run:  python spark/preprocess.py")
    print("   Then re-run: python ml/train_mllib.py")
    spark.stop()
    sys.exit(1)

print(f"📂  Loading data from {PARQUET_DIR} ({len(_parquet_files)} part-file(s)) ...")
df = spark.read.parquet(PARQUET_DIR)

# Cast feature columns to double for MLlib
FEATURE_COLS = [
    "context_len",
    "text_len",
    "num_words",
    "avg_word_len",
    "question_flag",
]

for col in FEATURE_COLS:
    df = df.withColumn(col, F.col(col).cast("double"))

df.cache()
total = df.count()
print(f"   Total rows: {total:,}")
df.printSchema()

# ---------------------------------------------------------------------------
# 3.  Train / Test split (distributed random split)
# ---------------------------------------------------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"   Train: {train_df.count():,}  |  Test: {test_df.count():,}")

# ---------------------------------------------------------------------------
# Helper: build, train, evaluate, and save one target model
# ---------------------------------------------------------------------------

def _evaluator(label_col: str, metric: str) -> RegressionEvaluator:
    return RegressionEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName=metric,
    )


def train_and_evaluate(
    train_data,
    test_data,
    label_col: str,
    model_name: str,
):
    """
    Trains a RandomForest and a GBT model for *label_col*.
    Evaluates both; saves the better one (by RMSE) to MODEL_DIR.

    Returns: (best_pipeline_model, metrics_dict)
    """
    print(f"\n{'='*55}")
    print(f"  TARGET: {label_col.upper()}  [{model_name}]")
    print(f"{'='*55}")

    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    # ---- Random Forest ----
    rf = RandomForestRegressor(
        labelCol=label_col,
        featuresCol="features",
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=2,
        featureSubsetStrategy="auto",
        seed=42,
    )
    pipeline_rf = Pipeline(stages=[assembler, scaler, rf])
    print("  🌲  Fitting RandomForest ...")
    model_rf = pipeline_rf.fit(train_data)
    preds_rf  = model_rf.transform(test_data)

    rmse_rf = _evaluator(label_col, "rmse").evaluate(preds_rf)
    mae_rf  = _evaluator(label_col, "mae").evaluate(preds_rf)
    r2_rf   = _evaluator(label_col, "r2").evaluate(preds_rf)
    print(f"     RF  → RMSE={rmse_rf:.4f}  MAE={mae_rf:.4f}  R²={r2_rf:.4f}")

    # ---- Gradient Boosted Trees ----
    gbt = GBTRegressor(
        labelCol=label_col,
        featuresCol="features",
        maxIter=50,
        maxDepth=6,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=42,
    )
    pipeline_gbt = Pipeline(stages=[assembler, scaler, gbt])
    print("  🚀  Fitting GBTRegressor ...")
    model_gbt = pipeline_gbt.fit(train_data)
    preds_gbt  = model_gbt.transform(test_data)

    rmse_gbt = _evaluator(label_col, "rmse").evaluate(preds_gbt)
    mae_gbt  = _evaluator(label_col, "mae").evaluate(preds_gbt)
    r2_gbt   = _evaluator(label_col, "r2").evaluate(preds_gbt)
    print(f"     GBT → RMSE={rmse_gbt:.4f}  MAE={mae_gbt:.4f}  R²={r2_gbt:.4f}")

    # ---- Pick best by RMSE ----
    if rmse_rf <= rmse_gbt:
        best_model  = model_rf
        best_preds  = preds_rf
        winner      = "RandomForest"
        best_metrics = {"rmse": rmse_rf, "mae": mae_rf, "r2": r2_rf}
    else:
        best_model  = model_gbt
        best_preds  = preds_gbt
        winner      = "GBT"
        best_metrics = {"rmse": rmse_gbt, "mae": mae_gbt, "r2": r2_gbt}

    print(f"\n  ✅  Winner: {winner}")
    print(f"      RMSE={best_metrics['rmse']:.4f}  "
          f"MAE={best_metrics['mae']:.4f}  "
          f"R²={best_metrics['r2']:.4f}")

    # ---- Show sample predictions ----
    print(f"\n  📊  Sample predictions ({label_col}):")
    best_preds.select(
        "context_len", "text_len", label_col, "prediction"
    ).limit(5).show(truncate=False)

    # ---- Save model ----
    save_path = f"{MODEL_DIR}/{model_name}"
    print(f"  💾  Saving to {save_path} ...")
    best_model.write().overwrite().save(save_path)

    return best_model, best_metrics


# ---------------------------------------------------------------------------
# 4.  Train models for INPUT and OUTPUT token prediction
# ---------------------------------------------------------------------------
model_input, metrics_in = train_and_evaluate(
    train_df, test_df,
    label_col="input_tokens",
    model_name="input_token_model",
)

model_output, metrics_out = train_and_evaluate(
    train_df, test_df,
    label_col="output_tokens",
    model_name="output_token_model",
)

# ---------------------------------------------------------------------------
# 5.  Final summary
# ---------------------------------------------------------------------------
print("\n" + "="*55)
print("  TRAINING SUMMARY")
print("="*55)
print(f"  INPUT  model  → RMSE={metrics_in['rmse']:.4f}  "
      f"MAE={metrics_in['mae']:.4f}  R²={metrics_in['r2']:.4f}")
print(f"  OUTPUT model  → RMSE={metrics_out['rmse']:.4f}  "
      f"MAE={metrics_out['mae']:.4f}  R²={metrics_out['r2']:.4f}")
print(f"\n  Models saved under: {MODEL_DIR}/")
print("="*55)

df.unpersist()
spark.stop()
print("\n✅  Training completed successfully!")
