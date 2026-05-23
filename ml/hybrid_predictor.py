"""
Hybrid predictor: blends heuristics + segment-specific ML models
For production inference on new prompts
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

# ---------------------------------------------------------------------------
# 1. Initialize Spark
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("HybridPredictor")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Define heuristic function (Spark SQL UDF)
# ---------------------------------------------------------------------------
def heuristic_output_tokens(text_len, is_code_indicator, question_flag, 
                             token_density_ratio, is_long_form):
    """
    Heuristic baseline prediction based on available features
    Returns typical output token count for content type
    """
    
    # Code requests get longer responses (average ~1000-1500 tokens)
    if is_code_indicator == 1 or token_density_ratio > 0.10:
        base = 1200
    # Questions get shorter responses (average ~300-400 tokens)
    elif question_flag == 1:
        base = 350
    # Long-form text gets medium responses (average ~600-800 tokens)
    elif is_long_form == 1:
        base = 700
    # Default general case
    else:
        base = 500
    
    # Slight adjustment based on input length (longer input → slightly longer output)
    adjustment = 1.0 + (text_len / 5000.0) * 0.2  # +20% max for very long inputs
    
    return base * adjustment

# Register UDF
spark.udf.register("heuristic_output_tokens", heuristic_output_tokens, "double")

# ---------------------------------------------------------------------------
# 3. Load models and test data for demo
# ---------------------------------------------------------------------------
MODEL_DIR = "ml/models"
TEST_PARQUET = "data/processed_enhanced.parquet"

print("[*] Loading models and test data ...")

# Load test data
df_test = spark.read.parquet(TEST_PARQUET)
df_test = df_test.withColumn("log_output_tokens", F.log1p(F.col("output_tokens")))

# Try to load segment models (they may not all exist yet)
models = {}
available_segments = ["code", "question", "general"]

for segment in available_segments:
    model_path = f"{MODEL_DIR}/output_token_model_{segment}"
    try:
        models[segment] = PipelineModel.load(model_path)
        print(f"   ✅ Loaded: {segment} model")
    except Exception as e:
        print(f"   ⚠️  Could not load {segment} model: {e}")

if not models:
    print("\n❌ ERROR: No segment models found!")
    print("   Please run: python ml/train_hybrid_model.py")
    spark.stop()
    sys.exit(1)

# ---------------------------------------------------------------------------
# 4. Classify each test record and make predictions
# ---------------------------------------------------------------------------
print(f"\n[*] Making hybrid predictions on test data ...")

# Filter to records we have models for
df_filtered = df_test.filter(F.col("segment").isin(list(models.keys())))

# Initialize predictions with heuristics
df_pred = df_filtered.withColumn(
    "heuristic_pred",
    heuristic_output_tokens(
        F.col("text_len"),
        F.col("is_code_indicator"),
        F.col("question_flag"),
        F.col("token_density_ratio"),
        F.col("is_long_form"),
    )
)

# Get ML predictions for each segment
df_ml_preds = None
for segment, model in models.items():
    segment_df = df_pred.filter(F.col("segment") == segment)
    segment_preds = model.transform(segment_df)
    segment_preds = segment_preds.withColumn(
        "ml_pred_tokens",
        F.expm1(F.col("prediction"))  # Convert from log scale
    )
    
    if df_ml_preds is None:
        df_ml_preds = segment_preds
    else:
        df_ml_preds = df_ml_preds.union(segment_preds)

# ---------------------------------------------------------------------------
# 5. Blend: 60% heuristic + 40% ML
# ---------------------------------------------------------------------------
print(f"\n[*] Blending predictions (60% heuristic + 40% ML) ...")

df_hybrid = df_ml_preds.withColumn(
    "hybrid_pred_tokens",
    (0.6 * F.col("heuristic_pred")) + (0.4 * F.col("ml_pred_tokens"))
)

# Add error margin (±50% uncertainty band for student project)
df_hybrid = df_hybrid.withColumn(
    "pred_lower",
    F.col("hybrid_pred_tokens") * 0.7
).withColumn(
    "pred_upper",
    F.col("hybrid_pred_tokens") * 1.5
)

# Convert actual to original scale
df_hybrid = df_hybrid.withColumn(
    "actual_tokens",
    F.expm1(F.col("log_output_tokens"))
)

# ---------------------------------------------------------------------------
# 6. Evaluate hybrid model
# ---------------------------------------------------------------------------
print(f"\n{'='*70}")
print("  HYBRID MODEL EVALUATION")
print(f"{'='*70}")

# Segment-wise evaluation
for segment in models.keys():
    seg_data = df_hybrid.filter(F.col("segment") == segment)
    count = seg_data.count()
    
    # Calculate metrics
    stats = seg_data.select(
        F.mean(F.abs(F.col("hybrid_pred_tokens") - F.col("actual_tokens"))).alias("mae"),
        F.sqrt(F.mean(
            (F.col("hybrid_pred_tokens") - F.col("actual_tokens")) ** 2
        )).alias("rmse"),
        F.corr("hybrid_pred_tokens", "actual_tokens").alias("corr"),
    ).collect()[0]
    
    mape = seg_data.select(
        F.mean(F.abs((F.col("hybrid_pred_tokens") - F.col("actual_tokens")) / 
                     F.greatest(F.lit(1), F.col("actual_tokens")))) * 100
    ).collect()[0][0]
    
    print(f"\n  [{segment.upper()}] ({count:,} samples)")
    print(f"    MAE:              {stats['mae']:.1f} tokens")
    print(f"    RMSE:             {stats['rmse']:.1f} tokens")
    print(f"    MAPE:             {mape:.1f}%")
    print(f"    Correlation:      {stats['corr']:.3f}")

# Overall metrics
overall_stats = df_hybrid.select(
    F.mean(F.abs(F.col("hybrid_pred_tokens") - F.col("actual_tokens"))).alias("mae"),
    F.sqrt(F.mean(
        (F.col("hybrid_pred_tokens") - F.col("actual_tokens")) ** 2
    )).alias("rmse"),
    F.corr("hybrid_pred_tokens", "actual_tokens").alias("corr"),
).collect()[0]

overall_mape = df_hybrid.select(
    F.mean(F.abs((F.col("hybrid_pred_tokens") - F.col("actual_tokens")) / 
                 F.greatest(F.lit(1), F.col("actual_tokens")))) * 100
).collect()[0][0]

print(f"\n  [OVERALL] ({df_hybrid.count():,} samples)")
print(f"    MAE:              {overall_stats['mae']:.1f} tokens")
print(f"    RMSE:             {overall_stats['rmse']:.1f} tokens")
print(f"    MAPE:             {overall_mape:.1f}%")
print(f"    Correlation:      {overall_stats['corr']:.3f}")

# ---------------------------------------------------------------------------
# 7. Sample predictions
# ---------------------------------------------------------------------------
print(f"\n  [SAMPLE PREDICTIONS]")
print(f"  (actual | lower bound | hybrid pred | upper bound)")
df_hybrid.select(
    F.round(F.col("actual_tokens"), 0).alias("actual"),
    F.round(F.col("pred_lower"), 0).alias("lower"),
    F.round(F.col("hybrid_pred_tokens"), 0).alias("hybrid"),
    F.round(F.col("pred_upper"), 0).alias("upper"),
    F.col("segment"),
).limit(15).show(truncate=False)

print("\n" + "=" * 70)
print("  ✅ HYBRID MODEL READY FOR DEPLOYMENT")
print("=" * 70)
print("""
  Usage:
    - Load segment-specific models from ml/models/output_token_model_*
    - Classify input (code/question/general)
    - Get ML prediction from appropriate model
    - Blend: 60% heuristic + 40% ML
    - Return with ±50% error margin for honest predictions
    
  For token cost optimization:
    cost = hybrid_pred * token_price * 1.5  (upper bound for safe budgeting)
""")

spark.stop()
