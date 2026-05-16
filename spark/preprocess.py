"""
Distributed preprocessing pipeline using PySpark.
Reads raw.json, extracts human→gpt conversation pairs,
computes all features with Spark UDFs, and writes a
distributed Parquet dataset — no pandas or local Python loops.
"""

import sys
from pathlib import Path

# Force UTF-8 encoding for stdout/stderr to prevent "Invalid argument" on Windows
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# Project root is one level above this file (spark/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    IntegerType, DoubleType, LongType
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# 1.  Spark Session — increased memory + off-heap to handle 6.7 GB JSON
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TokenOptimizerPreprocess")
    .config("spark.driver.memory", "10g")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.maxResultSize", "4g")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "4g")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2.  Paths
# ---------------------------------------------------------------------------
RAW_JSON = str(PROJECT_ROOT / "data" / "raw.json")
OUT_DIR  = str(PROJECT_ROOT / "data" / "processed.parquet")

print(f"📂  Loading {RAW_JSON} ...")

# ---------------------------------------------------------------------------
# 3.  Load raw.json — multiline JSON array, one object per row
# ---------------------------------------------------------------------------
raw_df = (
    spark.read
    .option("multiline", "true")
    .json(RAW_JSON)
)

raw_count = raw_df.count()
print(f"   Raw records loaded: {raw_count:,}")
if raw_count == 0:
    print("❌  No records found in raw.json — check the file path and JSON structure.")
    spark.stop()
    sys.exit(1)

# ---------------------------------------------------------------------------
# 4.  Explode conversations into (human_prompt, gpt_response) pairs
#
#     Uses window LAG() instead of a self-join:
#       - one pass over the data, no shuffle join
#       - keeps pairs where current turn is "gpt" and previous is "human"
# ---------------------------------------------------------------------------
turns_df = raw_df.select(
    "id",
    F.posexplode("conversations").alias("pos", "turn")
).select(
    "id",
    "pos",
    F.col("turn.from").alias("sender"),
    F.col("turn.value").alias("value"),
)

# Window over each conversation, ordered by position
w = Window.partitionBy("id").orderBy("pos")

pairs_df = (
    turns_df
    .withColumn("prev_sender", F.lag("sender", 1).over(w))
    .withColumn("prev_value",  F.lag("value",  1).over(w))
    # Keep only gpt turns where the immediately prior turn was human
    .filter((F.col("sender") == "gpt") & (F.col("prev_sender") == "human"))
    .select(
        F.col("id").alias("conv_id"),
        F.col("pos").alias("turn_pos"),
        F.col("prev_value").alias("prompt"),
        F.col("value").alias("response"),
    )
)

print("⚙️   Computing features (distributed UDFs) ...")

# ---------------------------------------------------------------------------
# 5.  Feature UDFs (all run inside Spark executors)
# ---------------------------------------------------------------------------

def _count_tokens(text: str) -> int:
    """Return GPT-3.5-turbo token count for *text*."""
    if not text:
        return 0
    import tiktoken  # imported inside UDF so workers load it lazily
    enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
    return len(enc.encode(text))

def _num_words(text: str) -> int:
    if not text:
        return 0
    return len(text.split())

def _avg_word_len(text: str) -> float:
    if not text:
        return 0.0
    words = text.split()
    if not words:
        return 0.0
    return float(sum(len(w) for w in words)) / float(len(words))

def _question_flag(text: str) -> int:
    if not text:
        return 0
    lower = text.lower()
    return int(any(q in lower for q in ("what", "why", "how", "explain", "describe")))

udf_count_tokens  = F.udf(_count_tokens,  IntegerType())
udf_num_words     = F.udf(_num_words,     IntegerType())
udf_avg_word_len  = F.udf(_avg_word_len,  DoubleType())
udf_question_flag = F.udf(_question_flag, IntegerType())

# ---------------------------------------------------------------------------
# 6.  Apply UDFs — all distributed across Spark partitions
# ---------------------------------------------------------------------------
features_df = (
    pairs_df
    .withColumn("text_len",      F.length("prompt").cast(LongType()))
    .withColumn("context_len",   F.length("prompt").cast(LongType()))
    .withColumn("num_words",     udf_num_words("prompt"))
    .withColumn("avg_word_len",  udf_avg_word_len("prompt"))
    .withColumn("question_flag", udf_question_flag("prompt"))
    .withColumn("input_tokens",  udf_count_tokens("prompt"))
    .withColumn("output_tokens", udf_count_tokens("response"))
    .select(
        "conv_id",
        "turn_pos",
        "text_len",
        "context_len",
        "num_words",
        "avg_word_len",
        "question_flag",
        "input_tokens",
        "output_tokens",
    )
    # Drop rows where tokenisation returned 0 (empty strings)
    .filter((F.col("input_tokens") > 0) & (F.col("output_tokens") > 0))
)

# ---------------------------------------------------------------------------
# 7.  Write as Parquet (native Spark format, columnar, compressed)
# ---------------------------------------------------------------------------
print(f"💾  Writing processed data to {OUT_DIR} ...")
features_df.write.mode("overwrite").parquet(OUT_DIR)

total = spark.read.parquet(OUT_DIR).count()
print(f"✅  Preprocessed {total:,} rows → {OUT_DIR}")

spark.stop()