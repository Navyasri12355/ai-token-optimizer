"""
Enhanced preprocessing with code detection and content segmentation features
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import glob
import os
import json
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
RAW_JSON      = PROJECT_ROOT / "data" / "raw.json"
CHUNKS_DIR    = PROJECT_ROOT / "data" / "_chunks_tmp"
OUT_DIR       = str(PROJECT_ROOT / "data" / "processed_enhanced.parquet")
PROGRESS_FILE = PROJECT_ROOT / "data" / "_enhance_progress.json"

print("=" * 70)
print("  Enhanced Preprocessor (adds code detection & content features)")
print("=" * 70)

# ---------------------------------------------------------------------------
# Initialize Spark
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("EnhancedPreprocessor")
    .config("spark.driver.memory",           "2g")
    .config("spark.executor.memory",         "2g")
    .config("spark.sql.shuffle.partitions",  "4")
    .config("spark.default.parallelism",     "4")
    .config("spark.memory.fraction",         "0.6")
    .config("spark.memory.storageFraction",  "0.3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Load original processed parquet
# ---------------------------------------------------------------------------
ORIGINAL_PARQUET = str(PROJECT_ROOT / "data" / "processed.parquet")

if not os.path.isdir(ORIGINAL_PARQUET):
    print(f"\n❌ ERROR: {ORIGINAL_PARQUET} not found")
    print("   Run: python ml/train_output_model_improved.py first")
    spark.stop()
    sys.exit(1)

print(f"\n[*] Loading original processed parquet ...")
df = spark.read.parquet(ORIGINAL_PARQUET)

print(f"   Rows: {df.count():,}")
print(f"   Columns: {', '.join(df.columns)}")

# ---------------------------------------------------------------------------
# Add ENHANCED features: using available columns only
# (prompt text is not available, use proxy features)
# ---------------------------------------------------------------------------
print(f"\n[*] Adding enhanced features ...")

# --- Code/technical content proxies (no prompt text available) ---
# Approximation: low text_len + high input_tokens = dense code
# Normal prose: high text_len + moderate input_tokens
df = df.withColumn(
    "token_density_ratio",
    (F.col("input_tokens") / F.greatest(F.lit(1.0), F.col("text_len"))).cast("double")
)

# High bracket density in original text suggests code
# Estimate: if word_count is much lower than text_len, it's probably code/markup
df = df.withColumn(
    "word_text_ratio",
    (F.col("num_words") / F.greatest(F.lit(1.0), F.col("text_len"))).cast("double")
)

# Code requests tend to have specific patterns in turn structure
# First turns with moderate tokens, then follow-ups are also technical
df = df.withColumn(
    "is_code_indicator",
    F.when(
        (F.col("token_density_ratio") > 0.08) |  # Dense: typical of code
        (F.col("word_text_ratio") < 0.15),       # Sparse: markup/code
        1
    ).otherwise(0)
    .cast("double")
)

# --- Content type classification ---
# Long-form indicator (abstract/essay like)
df = df.withColumn(
    "is_long_form",
    F.when(F.col("input_tokens") > 150, 1).otherwise(0).cast("double")
)

# Conversation depth influence (later turns might behave differently)
df = df.withColumn(
    "is_deep_conversation",
    F.when(F.col("turn_pos") > 5, 1).otherwise(0).cast("double")
)

# --- Content segments ---
# Segment: CODE (indicators suggest technical content)
# Segment: QUESTION (question flag set)
# Segment: GENERAL (everything else)
df = df.withColumn(
    "segment",
    F.when(F.col("is_code_indicator") == 1, "code")
     .when(F.col("question_flag") == 1, "question")
     .otherwise("general")
)

print(f"\n[*] Feature summary:")
print(f"   token_density_ratio: input_tokens / text_len")
print(f"   word_text_ratio: num_words / text_len (sparse = code)")
print(f"   is_code_indicator: estimated technical content")
print(f"   is_long_form: input_tokens > 150")
print(f"   is_deep_conversation: turn_pos > 5")
print(f"   segment: [code | question | general]")

# ---------------------------------------------------------------------------
# Check distribution
# ---------------------------------------------------------------------------
print(f"\n[*] Segment distribution:")
segment_counts = df.groupBy("segment").count().collect()
for row in segment_counts:
    pct = (row['count'] / df.count()) * 100
    print(f"   {row['segment']:.<15} {row['count']:>10,}  ({pct:>5.1f}%)")

# ---------------------------------------------------------------------------
# Save enhanced parquet
# ---------------------------------------------------------------------------
print(f"\n[*] Saving enhanced parquet to {OUT_DIR} ...")
df.write.mode("overwrite").parquet(OUT_DIR)

# Show schema
print(f"\n[*] Final schema:")
for col in sorted(df.columns):
    print(f"   • {col}")

print("\n" + "=" * 70)
print("  ✅ ENHANCEMENT COMPLETE")
print("=" * 70)
print(f"   Output: {OUT_DIR}")
print(f"   Total rows: {df.count():,}")

spark.stop()
