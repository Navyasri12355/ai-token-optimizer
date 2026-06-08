"""
spark/preprocess.py
====================
PySpark-based preprocessing pipeline for raw.json conversational data.

Schema of raw.json:
  [ { "id": str,
      "conversations": [ {"from": "human"|"gpt", "value": str}, ... ] },
    ... ]

Produces a processed Parquet dataset with per-turn features:
  - record_id, turn_index, role ("human" / "gpt")
  - raw_text
  - char_count, word_count, token_count (whitespace approximation)
  - sentence_count, avg_word_length
  - has_code_block (bool)
  - optimized_text, optimized_token_count, token_savings, savings_pct
  - conversation_turn_count (total turns in that conversation)
"""

import re
import sys
import time
import logging
import os
from pathlib import Path

# ── PySpark imports ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    IntegerType, FloatType, BooleanType, LongType
)

# ── Project root on sys.path ───────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ELK logger (graceful fallback if ES is not running)
try:
    from monitoring.elk_logger import get_elk_logger
    logger = get_elk_logger("preprocess")
except Exception:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("preprocess")

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_JSON_PATH     = str(ROOT / "data" / "raw.json")
OUTPUT_PARQUET    = str(ROOT / "spark" / "output" / "processed")
SAMPLE_PARQUET    = str(ROOT / "spark" / "output" / "sample")
STATS_PATH        = str(ROOT / "spark" / "output" / "stats.json")

# ── Optimisation helpers ──────────────────────────────────────────────────────
# NOTE: All constants are defined INSIDE the UDF closures so PySpark can
# serialise (pickle) the functions correctly on Windows.

# These are also exported for unit tests (test_pipeline.py imports them).
_KEEP = {"not","no","how","what","why","who","when","where",
         "explain","define","compare","list","give","examples"}
_FILLERS = [
    "please", "could you", "would you mind", "kindly",
    "i would like you to", "can you", "tell me", "help me understand"
]


def _build_stops():
    """Build stopwords set – called inside each UDF to avoid pickle issues."""
    keep = {"not","no","how","what","why","who","when","where",
            "explain","define","compare","list","give","examples"}
    try:
        import nltk as _nltk
        try:
            _nltk.data.find("corpora/stopwords")
        except LookupError:
            _nltk.download("stopwords", quiet=True)
        from nltk.corpus import stopwords as _sw
        return set(_sw.words("english")) - keep
    except Exception:
        return set()


# Module-level cache (used only by unit tests / non-Spark code)
try:
    _STOPS = _build_stops()
except Exception:
    _STOPS = set()


# ── UDF functions – self-contained closures (no module-level mutable state) ───
def _optimize_text(text):
    """Lightweight prompt optimiser – safe for PySpark pickle."""
    if not text:
        return text
    _fillers = [
        "please", "could you", "would you mind", "kindly",
        "i would like you to", "can you", "tell me", "help me understand"
    ]
    _keep = {"not","no","how","what","why","who","when","where",
             "explain","define","compare","list","give","examples"}
    import re as _re
    t = text.lower()
    for f in _fillers:
        t = t.replace(f, "")
    t = _re.sub(r"[^\w\s]", "", t)
    words = t.split()
    # Use a small embedded stopword list to avoid pickling nltk
    _basic_stops = {
        "i","me","my","myself","we","our","ours","ourselves","you","your",
        "yours","he","him","his","she","her","hers","it","its","they",
        "them","their","this","that","these","those","am","is","are","was",
        "were","be","been","being","have","has","had","do","does","did",
        "a","an","the","and","but","if","or","as","at","by","for","in",
        "of","on","to","up","with","about","into","through","then","just",
        "so","than","too","very","some","same","such","also","each",
    } - _keep
    words = [w for w in words if w not in _basic_stops]
    seen, uniq = set(), []
    for w in words:
        if w not in seen:
            uniq.append(w); seen.add(w)
    result = " ".join(uniq)
    return result if result.strip() else text


def _approx_tokens(text):
    if not text:
        return 0
    return max(1, len(text.split()))


def _sentence_count(text):
    import re as _re
    return max(1, len(_re.split(r"[.!?]+", text)))


def _avg_word_len(text):
    words = text.split()
    if not words:
        return 0.0
    return float(sum(len(w) for w in words)) / len(words)


def _has_code(text):
    return "```" in text


# ── Register as Spark UDFs ─ ONLY for non-Spark code / unit tests ───────────
# The preprocessing pipeline itself now uses only Spark SQL native expressions
# (regexp_replace, split, size, length, etc.) so no Python UDF subprocess is
# needed, which avoids the Windows socket error in PySpark 3.x.
# These wrappers are kept so test_pipeline.py can still exercise the logic.


# ── Main pipeline ──────────────────────────────────────────────────────────────
def run_preprocessing(
    sample_size: int = None,
    output_parquet: str = OUTPUT_PARQUET,
    sample_parquet: str = SAMPLE_PARQUET,
    stats_path: str = STATS_PATH,
    raw_json: str = RAW_JSON_PATH,
    jsonl_path: str = None,
):
    """
    Full preprocessing pipeline.

    Args:
        sample_size:    If set, take only this many top-level records (for testing).
        output_parquet: Destination for full processed dataset.
        sample_parquet: Destination for a 10k-row sample used by the trainer.
        stats_path:     JSON file for summary statistics.
        raw_json:       Path to raw.json (multiline array — fallback).
        jsonl_path:     Path to raw.jsonl (preferred — avoids OOM on large files).
    """
    t0 = time.time()

    # ── Auto-detect JSONL ────────────────────────────────────────────────────────
    # Prefer JSONL because Spark can split it across partitions without OOM.
    # Fallback to raw.json with multiline=True only if JSONL not available.
    _jsonl = jsonl_path or str(Path(raw_json).with_suffix(".jsonl"))
    use_jsonl = os.path.exists(_jsonl)

    input_file = _jsonl if use_jsonl else raw_json
    input_mode = "JSONL (partitioned)" if use_jsonl else "JSON array (multiline)"

    logger.info("Starting PySpark preprocessing pipeline")
    logger.info(f"   input     : {input_file}")
    logger.info(f"   format    : {input_mode}")
    logger.info(f"   output    : {output_parquet}")
    if not use_jsonl:
        logger.warning(
            "JSONL not found - falling back to multiline JSON. "
            "This may OOM on large files. Run first: "
            "python spark/convert_to_jsonl.py"
        )

    # ── Build Spark session ──────────────────────────────────────────────────────
    from spark.spark_session import get_spark
    spark = get_spark(
        app_name="TokenOptimizerPreprocessing",
        driver_memory="8g",
        shuffle_partitions=4,
        extra_configs={
            # Allow Spark to spill to disk when heap is full
            "spark.memory.fraction":           "0.7",
            "spark.memory.storageFraction":    "0.3",
            # Disable whole-stage codegen (reduces per-task heap pressure)
            "spark.sql.codegen.wholeStage":    "false",
            # Write Parquet without pre-shuffle repartition
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128 MB per partition
            # Checkpoint dir so Spark can spill shuffle data to disk
            "spark.local.dir": str(ROOT / "spark" / "tmp"),
        }
    )
    # Set checkpoint dir for disk spill
    spark.sparkContext.setCheckpointDir(str(ROOT / "spark" / "tmp" / "checkpoints"))
    logger.info("SparkSession created")

    # ── Load data ─────────────────────────────────────────────────────────────
    logger.info(f"Loading input data ({input_mode}) ...")

    raw_schema = StructType([
        StructField("id",            StringType(), True),
        StructField("conversations", ArrayType(
            StructType([
                StructField("from",  StringType(), True),
                StructField("value", StringType(), True),
            ])
        ), True),
    ])

    if use_jsonl:
        # JSONL: Spark splits at newlines → proper parallelism, no OOM
        df_raw = (
            spark.read
            .schema(raw_schema)
            .json(_jsonl)          # no multiline option needed
        )
    else:
        # Fallback: entire file in one partition — OOM risk on large files
        df_raw = (
            spark.read
            .option("multiline", "true")
            .schema(raw_schema)
            .json(raw_json)
        )

    if sample_size:
        df_raw = df_raw.limit(sample_size)
        logger.info(f"   Sample mode: capped at {sample_size:,} conversations")

    total_convs = df_raw.count()
    logger.info(f"   Loaded {total_convs:,} conversation records")

    # ── Explode turns ──────────────────────────────────────────────────────────
    # Add turn index before explode
    df_indexed = df_raw.select(
        F.col("id").alias("record_id"),
        F.posexplode("conversations").alias("turn_index", "turn"),
        F.size("conversations").alias("conversation_turn_count"),
    )

    # ── Feature extraction using ONLY Spark SQL native expressions ───────────
    # (No Python UDF subprocess → avoids Windows socket crash in PySpark 3.x)
    logger.info("Extracting features ...")

    # Filler phrases to strip (applied via regexp_replace chains)
    _FILLER_RE = "(?i)(please |could you |would you mind |kindly |i would like you to |can you |tell me |help me understand )"

    df_feats = (
        df_indexed
        .withColumn("role",     F.col("turn.from"))
        .withColumn("raw_text", F.coalesce(F.col("turn.value"), F.lit("")))
        .drop("turn")
        # — Basic text stats (all native Spark) —
        .withColumn("char_count",   F.length("raw_text"))
        .withColumn("word_count",   F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
        # Approx tokens = word_count (whitespace split)
        .withColumn("token_count",  F.size(F.split(F.trim(F.col("raw_text")), r"\s+")))
        # Sentence count: number of sentence-ending punctuation groups
        .withColumn("sentence_count",
                    F.greatest(
                        F.lit(1),
                        F.size(F.split(F.col("raw_text"), r"[.!?]+")) - F.lit(1)
                    ))
        # Avg word length
        .withColumn("avg_word_length",
                    F.col("char_count") / F.greatest(F.col("word_count"), F.lit(1)))
        # Has code block
        .withColumn("has_code_block",
                    F.col("raw_text").contains("```"))
        # — Optimised text: lowercase + remove fillers + collapse whitespace —
        .withColumn("_opt1", F.lower(F.col("raw_text")))
        .withColumn("_opt2", F.regexp_replace(F.col("_opt1"), _FILLER_RE, " "))
        .withColumn("_opt3", F.regexp_replace(F.col("_opt2"), r"[^\w\s]", ""))
        .withColumn("optimized_text", F.trim(F.regexp_replace(F.col("_opt3"), r"\s+", " ")))
        .drop("_opt1", "_opt2", "_opt3")
        # Optimised token count
        .withColumn("optimized_token_count",
                    F.size(F.split(F.trim(F.col("optimized_text")), r"\s+")))
        # Savings
        .withColumn("token_savings",
                    F.col("token_count") - F.col("optimized_token_count"))
        .withColumn("savings_pct",
                    F.when(F.col("token_count") > 0,
                           (F.col("token_savings") / F.col("token_count") * 100.0))
                    .otherwise(F.lit(0.0)))
    )

    # ── Filter out empty / null turns ─────────────────────────────────────────
    df_clean = (
        df_feats
        .filter(F.col("raw_text").isNotNull())
        .filter(F.length(F.trim(F.col("raw_text"))) > 0)
        .filter(F.col("token_count") > 0)
    )

    # ── Role-specific features ─────────────────────────────────────────────────
    df_final = df_clean.withColumn(
        "is_human", F.when(F.col("role") == "human", True).otherwise(False)
    )

    # ── Persist processed dataset ──────────────────────────────────────────────
    logger.info(f"Writing Parquet -> {output_parquet}")
    os.makedirs(output_parquet, exist_ok=True)
    (
        df_final
        # coalesce reduces partition count WITHOUT a full shuffle
        # Use 4 partitions for full run, 1 for sample
        .coalesce(1 if sample_size else 4)
        .write
        .mode("overwrite")
        .parquet(output_parquet)
    )

    # ── Persist a 10 k-row sample for fast ML iteration ───────────────────────
    logger.info(f"💾 Writing sample Parquet → {sample_parquet}")
    (
        df_final
        .limit(10_000)
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(sample_parquet)
    )

    # ── Summary statistics ────────────────────────────────────────────────────
    logger.info("📊 Computing summary statistics …")
    stats = (
        df_final.agg(
            F.count("*").alias("total_turns"),
            F.countDistinct("record_id").alias("total_conversations"),
            F.mean("token_count").alias("avg_token_count"),
            F.mean("optimized_token_count").alias("avg_opt_token_count"),
            F.mean("savings_pct").alias("avg_savings_pct"),
            F.sum("token_savings").alias("total_tokens_saved"),
            F.sum("token_count").alias("total_tokens"),
            F.mean("word_count").alias("avg_word_count"),
            F.mean("char_count").alias("avg_char_count"),
        ).collect()[0]
    )

    import json
    stats_dict = {k: (float(v) if v is not None else None)
                  for k, v in stats.asDict().items()}
    stats_dict["elapsed_seconds"] = round(time.time() - t0, 2)
    stats_dict["raw_json_path"]   = raw_json
    stats_dict["output_parquet"]  = output_parquet

    os.makedirs(os.path.dirname(stats_path), exist_ok=True)
    with open(stats_path, "w") as fh:
        json.dump(stats_dict, fh, indent=2)

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"✅ Preprocessing complete in {elapsed:.1f}s")
    logger.info(f"   Total conversations : {int(stats['total_conversations']):,}")
    logger.info(f"   Total turns         : {int(stats['total_turns']):,}")
    logger.info(f"   Avg token count     : {stats['avg_token_count']:.1f}")
    logger.info(f"   Avg savings         : {stats['avg_savings_pct']:.1f}%")
    logger.info(f"   Stats saved to      : {stats_path}")
    logger.info("=" * 60)

    spark.stop()
    return stats_dict


# ── CLI entry-point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="PySpark preprocessing for raw.json")
    ap.add_argument("--sample",  type=int, default=None,
                    help="Limit to N conversations (omit for full run)")
    ap.add_argument("--raw-json", default=RAW_JSON_PATH)
    ap.add_argument("--output",   default=OUTPUT_PARQUET)
    args = ap.parse_args()

    run_preprocessing(
        sample_size   = args.sample,
        raw_json      = args.raw_json,
        output_parquet= args.output,
    )
