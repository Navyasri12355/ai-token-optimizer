"""
spark/train_model.py
====================
Spark MLlib model training for token-count prediction and optimisation.

Pipeline
--------
1. Load processed Parquet (from preprocess.py)
2. Feature engineering with VectorAssembler
3. Train Ridge Regression (LinearRegression with elasticNetParam=0.0)
4. Evaluate – if Ridge R² < 0.70, auto-select Gradient-Boosted Trees (GBT)
5. Cross-validate best model
6. Save best model + evaluation metrics
7. Log all metrics to Elasticsearch / local JSON
"""

import sys
import os
import json
import time
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import (
    LinearRegression,
    GBTRegressor,
    RandomForestRegressor,
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from monitoring.elk_logger import get_elk_logger
    logger = get_elk_logger("train_model")
except Exception:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("train_model")

# ── Paths ──────────────────────────────────────────────────────────────────────
PROCESSED_PARQUET = str(ROOT / "spark" / "output" / "processed")
SAMPLE_PARQUET    = str(ROOT / "spark" / "output" / "sample")
MODELS_DIR        = str(ROOT / "spark" / "models")
EVAL_PATH         = str(ROOT / "spark" / "output" / "evaluation.json")

# ── Feature columns used for prediction ───────────────────────────────────────
FEATURE_COLS = [
    "char_count",
    "word_count",
    "sentence_count",
    "avg_word_length",
    "conversation_turn_count",
    "turn_index",
    "has_code_block_int",   # cast from bool
]
TARGET_COL   = "token_count"        # predict raw token count
OPT_TARGET   = "optimized_token_count"  # also train optimised model

RIDGE_R2_THRESHOLD = 0.70  # auto-switch to GBT if below this


def _cast_features(df):
    """Cast boolean → int and ensure all feature cols are numeric."""
    return df.withColumn("has_code_block_int",
                         F.col("has_code_block").cast("int"))


def _build_pipeline(regressor, feature_cols=FEATURE_COLS, target=TARGET_COL):
    """Assemble → Scale → Regressor pipeline."""
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
    regressor = regressor.setLabelCol(target).setFeaturesCol("features")
    return Pipeline(stages=[assembler, scaler, regressor])


def _evaluate(model, test_df, target=TARGET_COL):
    """Return dict with rmse, mae, r2."""
    preds = model.transform(test_df)
    ev = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    return {
        "rmse": ev.setMetricName("rmse").evaluate(preds),
        "mae":  ev.setMetricName("mae").evaluate(preds),
        "r2":   ev.setMetricName("r2").evaluate(preds),
    }


def train(
    use_sample: bool = False,
    parquet_path: str = None,
    models_dir: str = MODELS_DIR,
    eval_path: str = EVAL_PATH,
    cv_folds: int = 3,
):
    """
    Train token-count prediction models.

    Returns:
        dict  evaluation results keyed by model name
    """
    t0 = time.time()
    parquet_path = parquet_path or (SAMPLE_PARQUET if use_sample else PROCESSED_PARQUET)

    logger.info("🚀 Starting model training")
    logger.info(f"   parquet : {parquet_path}")
    logger.info(f"   models  : {models_dir}")

    from spark.spark_session import get_spark
    spark = get_spark(app_name="TokenOptimizerTraining", shuffle_partitions=8)

    # ── Load data ──────────────────────────────────────────────────────────────
    logger.info("📂 Loading processed Parquet …")
    df = spark.read.parquet(parquet_path)
    df = _cast_features(df)

    # Drop rows with null in feature / label cols
    all_cols = FEATURE_COLS + [TARGET_COL, OPT_TARGET]
    df = df.dropna(subset=all_cols)

    total = df.count()
    logger.info(f"   Loaded {total:,} turns for training")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()

    os.makedirs(models_dir, exist_ok=True)
    results = {}

    # ══════════════════════════════════════════════════════════════════════════
    # MODEL A – Ridge Regression (token_count prediction)
    # ══════════════════════════════════════════════════════════════════════════
    logger.info("─" * 50)
    logger.info("🔵 Training Ridge Regression (raw token count) …")
    ridge = LinearRegression(
        regParam=0.1,
        elasticNetParam=0.0,   # 0.0 = pure L2 = Ridge
        maxIter=100,
        solver="normal",
    )
    ridge_pipe = _build_pipeline(ridge, target=TARGET_COL)
    t1 = time.time()
    ridge_model = ridge_pipe.fit(train_df)
    ridge_train_time = time.time() - t1

    ridge_metrics = _evaluate(ridge_model, test_df, TARGET_COL)
    ridge_metrics["training_time_s"] = round(ridge_train_time, 2)
    ridge_metrics["model_type"] = "RidgeRegression"
    ridge_metrics["target"] = TARGET_COL
    results["ridge_token_count"] = ridge_metrics

    logger.info(f"   RMSE={ridge_metrics['rmse']:.3f}  "
                f"MAE={ridge_metrics['mae']:.3f}  "
                f"R²={ridge_metrics['r2']:.4f}  "
                f"({ridge_train_time:.1f}s)")

    # Save Ridge model
    ridge_path = os.path.join(models_dir, "ridge_token_count")
    ridge_model.write().overwrite().save(ridge_path)
    logger.info(f"   💾 Ridge model saved → {ridge_path}")

    # ══════════════════════════════════════════════════════════════════════════
    # MODEL B – GBT Regressor (auto-selected if Ridge R² < threshold)
    # ══════════════════════════════════════════════════════════════════════════
    best_model_name = "ridge_token_count"
    best_r2         = ridge_metrics["r2"]

    if ridge_metrics["r2"] < RIDGE_R2_THRESHOLD:
        logger.info(f"⚠️  Ridge R²={ridge_metrics['r2']:.4f} < {RIDGE_R2_THRESHOLD}. "
                    "Switching to GBT …")
        gbt = GBTRegressor(maxIter=50, maxDepth=5, stepSize=0.1)
        gbt_pipe = _build_pipeline(gbt, target=TARGET_COL)
        t2 = time.time()
        gbt_model = gbt_pipe.fit(train_df)
        gbt_train_time = time.time() - t2

        gbt_metrics = _evaluate(gbt_model, test_df, TARGET_COL)
        gbt_metrics["training_time_s"] = round(gbt_train_time, 2)
        gbt_metrics["model_type"] = "GBTRegressor"
        gbt_metrics["target"] = TARGET_COL
        results["gbt_token_count"] = gbt_metrics

        logger.info(f"   GBT  RMSE={gbt_metrics['rmse']:.3f}  "
                    f"MAE={gbt_metrics['mae']:.3f}  "
                    f"R²={gbt_metrics['r2']:.4f}  "
                    f"({gbt_train_time:.1f}s)")

        gbt_path = os.path.join(models_dir, "gbt_token_count")
        gbt_model.write().overwrite().save(gbt_path)
        logger.info(f"   💾 GBT model saved → {gbt_path}")

        if gbt_metrics["r2"] > best_r2:
            best_model_name = "gbt_token_count"
            best_r2 = gbt_metrics["r2"]
    else:
        logger.info(f"✅ Ridge R²={ridge_metrics['r2']:.4f} ≥ {RIDGE_R2_THRESHOLD}. "
                    "Ridge is selected as primary model.")

    # ══════════════════════════════════════════════════════════════════════════
    # MODEL C – Ridge for OPTIMISED token count (savings prediction)
    # ══════════════════════════════════════════════════════════════════════════
    logger.info("─" * 50)
    logger.info("🟣 Training Ridge Regression (optimised token count) …")
    ridge_opt = LinearRegression(
        regParam=0.1, elasticNetParam=0.0, maxIter=100, solver="normal"
    )
    ridge_opt_pipe = _build_pipeline(ridge_opt, target=OPT_TARGET)
    t3 = time.time()
    ridge_opt_model = ridge_opt_pipe.fit(train_df)
    ridge_opt_time  = time.time() - t3

    ridge_opt_metrics = _evaluate(ridge_opt_model, test_df, OPT_TARGET)
    ridge_opt_metrics["training_time_s"] = round(ridge_opt_time, 2)
    ridge_opt_metrics["model_type"] = "RidgeRegression"
    ridge_opt_metrics["target"] = OPT_TARGET
    results["ridge_opt_token_count"] = ridge_opt_metrics

    logger.info(f"   RMSE={ridge_opt_metrics['rmse']:.3f}  "
                f"MAE={ridge_opt_metrics['mae']:.3f}  "
                f"R²={ridge_opt_metrics['r2']:.4f}")

    opt_path = os.path.join(models_dir, "ridge_opt_token_count")
    ridge_opt_model.write().overwrite().save(opt_path)
    logger.info(f"   💾 Optimised-token Ridge saved → {opt_path}")

    # ══════════════════════════════════════════════════════════════════════════
    # MODEL D – Cross-validated best model (3-fold CV on Ridge)
    # ══════════════════════════════════════════════════════════════════════════
    logger.info("─" * 50)
    logger.info(f"🔁 Cross-validating Ridge ({cv_folds} folds) …")
    cv_ridge = LinearRegression(elasticNetParam=0.0, maxIter=100, solver="normal")
    cv_pipe   = _build_pipeline(cv_ridge, target=TARGET_COL)
    param_grid = (
        ParamGridBuilder()
        .addGrid(cv_ridge.regParam, [0.01, 0.1, 1.0])
        .build()
    )
    evaluator = RegressionEvaluator(
        labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
    )
    cv = CrossValidator(
        estimator=cv_pipe,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=cv_folds,
        parallelism=2,
    )
    t4 = time.time()
    cv_model = cv.fit(train_df)
    cv_time  = time.time() - t4

    cv_metrics = _evaluate(cv_model.bestModel, test_df, TARGET_COL)
    cv_metrics["training_time_s"] = round(cv_time, 2)
    cv_metrics["model_type"] = "CrossValidated_RidgeRegression"
    cv_metrics["target"] = TARGET_COL
    cv_metrics["best_regParam"] = float(
        cv_model.bestModel.stages[-1]._java_obj.getRegParam()
    )
    results["cv_ridge_token_count"] = cv_metrics

    logger.info(f"   CV   RMSE={cv_metrics['rmse']:.3f}  "
                f"MAE={cv_metrics['mae']:.3f}  "
                f"R²={cv_metrics['r2']:.4f}  "
                f"bestRegParam={cv_metrics['best_regParam']}  "
                f"({cv_time:.1f}s)")

    cv_path = os.path.join(models_dir, "cv_ridge_token_count")
    cv_model.bestModel.write().overwrite().save(cv_path)
    logger.info(f"   💾 CV Ridge model saved → {cv_path}")

    # ══════════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════════
    elapsed = time.time() - t0
    summary = {
        "best_model": best_model_name,
        "best_r2": best_r2,
        "total_training_time_s": round(elapsed, 2),
        "dataset_size": total,
        "models": results,
    }

    os.makedirs(os.path.dirname(eval_path), exist_ok=True)
    with open(eval_path, "w") as fh:
        json.dump(summary, fh, indent=2)
    logger.info(f"📄 Evaluation saved → {eval_path}")

    logger.info("=" * 60)
    logger.info(f"🏆 Best model : {best_model_name}  (R²={best_r2:.4f})")
    logger.info(f"⏱️  Total time  : {elapsed:.1f}s")
    logger.info("=" * 60)

    # Push to ELK if available
    try:
        from monitoring.metrics import MetricsCollector
        mc = MetricsCollector()
        for name, m in results.items():
            mc.record_training_metrics(
                model_name=name,
                mae=m["mae"],
                rmse=m["rmse"],
                r2=m["r2"],
                training_time=m["training_time_s"],
                dataset_size=total,
            )
        logger.info("📡 Metrics pushed to Elasticsearch")
    except Exception as e:
        logger.warning("Could not push to Elasticsearch: %s", str(e))

    spark.stop()
    return summary


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Train Spark MLlib models")
    ap.add_argument("--sample",  action="store_true",
                    help="Use 10k-row sample instead of full dataset")
    ap.add_argument("--parquet", default=None,
                    help="Override processed Parquet path")
    ap.add_argument("--cv-folds", type=int, default=3)
    args = ap.parse_args()

    train(
        use_sample=args.sample,
        parquet_path=args.parquet,
        cv_folds=args.cv_folds,
    )
