"""
Memory-efficient chunked preprocessing pipeline.

Strategy
--------
1. Stream-parse raw.json with ijson and write chunk JSON files (skipped
   automatically if chunk files already exist in _chunks_tmp/).
2. For every chunk file, run feature engineering with a low-memory Spark
   session using ONLY native Spark SQL expressions — zero Python UDFs,
   zero Python worker processes, zero socket crashes.
3. APPEND each chunk's output to data/processed.parquet.
4. Delete each temp chunk file after its Spark job succeeds.

Token-count approximation
-------------------------
GPT-3.5-turbo averages ~1 token per 4 characters.  We use:
    input_tokens  = ceil(length(prompt)   / 4)
    output_tokens = ceil(length(response) / 4)
This avoids running tiktoken inside Spark executors (which crashes on
Windows due to the Python worker socket issue).

Peak RAM per chunk: ~4-5 GB (2 g driver + 2 g executor + OS overhead).

Usage
-----
    python spark/preprocess_chunked.py               # 4 000 records / chunk
    python spark/preprocess_chunked.py --chunk 2000  # smaller → less RAM
    python spark/preprocess_chunked.py --no-resume   # fresh start
"""

import sys
import os
import json
import shutil
import argparse
import glob as _glob
from pathlib import Path

# Force UTF-8 on Windows
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
RAW_JSON      = PROJECT_ROOT / "data" / "raw.json"
CHUNKS_DIR    = PROJECT_ROOT / "data" / "_chunks_tmp"
OUT_DIR       = str(PROJECT_ROOT / "data" / "processed.parquet")
PROGRESS_FILE = PROJECT_ROOT / "data" / "_chunk_progress.json"

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Chunked Spark preprocessor (no UDFs)")
parser.add_argument("--chunk", type=int, default=4000,
                    help="Records per chunk when splitting (default: 4000)")
parser.add_argument("--no-resume", action="store_true",
                    help="Wipe progress and processed.parquet; restart from scratch")
args = parser.parse_args()

CHUNK_SIZE = args.chunk
RESUME     = not args.no_resume

print("=" * 60)
print(f"  Chunked Preprocessor  (chunk_size={CHUNK_SIZE})")
print("=" * 60)

# ---------------------------------------------------------------------------
# Load / initialise resume state
# ---------------------------------------------------------------------------
progress: dict = {}
if RESUME and PROGRESS_FILE.exists():
    with open(PROGRESS_FILE, "r") as f:
        progress = json.load(f)
    done_chunks = {k for k, v in progress.items() if v == "done"}
    print(f"   Resuming — {len(done_chunks)} chunk(s) already processed.")
elif not RESUME:
    if Path(OUT_DIR).exists():
        shutil.rmtree(OUT_DIR)
        print("   🗑️   Cleared existing processed.parquet for fresh run.")
    if CHUNKS_DIR.exists():
        shutil.rmtree(CHUNKS_DIR)
        print("   🗑️   Cleared existing chunk files for fresh run.")

# ---------------------------------------------------------------------------
# 1.  Split phase — skipped if chunk files already exist
# ---------------------------------------------------------------------------
CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
existing_chunks = sorted(CHUNKS_DIR.glob("chunk_*.json"))

if existing_chunks:
    # Chunk files from a previous run are intact — reuse them
    chunk_files = existing_chunks
    print(f"\n♻️   Found {len(chunk_files)} existing chunk file(s) — skipping split step.")
else:
    # Need to stream-split raw.json
    try:
        import ijson
    except ImportError:
        print("❌  ijson not found. Install it:  pip install ijson")
        sys.exit(1)

    print(f"\n📄  Splitting {RAW_JSON.name} ({RAW_JSON.stat().st_size / 1e9:.2f} GB) …")

    chunk_files = []
    chunk_id    = 0
    current     = []

    with open(RAW_JSON, "rb") as fh:
        for record in ijson.items(fh, "item"):
            current.append(record)
            if len(current) >= CHUNK_SIZE:
                chunk_path = CHUNKS_DIR / f"chunk_{chunk_id:04d}.json"
                chunk_files.append(chunk_path)
                with open(chunk_path, "w", encoding="utf-8") as out:
                    json.dump(current, out, ensure_ascii=False)
                chunk_id += 1
                current = []
                print(f"   chunk_{chunk_id-1:04d}.json  written", flush=True)

    if current:
        chunk_path = CHUNKS_DIR / f"chunk_{chunk_id:04d}.json"
        chunk_files.append(chunk_path)
        with open(chunk_path, "w", encoding="utf-8") as out:
            json.dump(current, out, ensure_ascii=False)
        print(f"   chunk_{chunk_id:04d}.json  written ({len(current)} remainder)")

    print(f"\n✅  Split complete: {len(chunk_files)} chunk file(s)\n")

total_chunks = len(chunk_files)

# ---------------------------------------------------------------------------
# 2.  Spark Session — conservative memory, NO Python UDF workers needed
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("TokenOptimizerChunked")
    # ---- Memory budget ≈ 4-5 GB peak ----------------------------
    .config("spark.driver.memory",           "2g")
    .config("spark.executor.memory",         "2g")
    .config("spark.driver.maxResultSize",    "1g")
    .config("spark.memory.offHeap.enabled",  "true")
    .config("spark.memory.offHeap.size",     "512m")
    # ---- Reduce task overhead ------------------------------------
    .config("spark.sql.shuffle.partitions",  "4")
    .config("spark.default.parallelism",     "4")
    # ---- Spill aggressively to disk (safety valve) ---------------
    .config("spark.memory.fraction",         "0.6")
    .config("spark.memory.storageFraction",  "0.3")
    # ---- Disable Python UDF worker (not needed) ------------------
    .config("spark.python.worker.reuse",     "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 3.  Process each chunk — 100 % native Spark SQL, zero Python UDFs
# ---------------------------------------------------------------------------
def process_chunk(chunk_path: Path, chunk_label: str) -> int:
    """
    Reads one chunk JSON → extracts (prompt, response) pairs →
    computes features with native Spark expressions → APPENDs to Parquet.

    Feature derivations (all native Spark, no Python workers):
      text_len      = length in characters of the prompt
      context_len   = same as text_len (alias kept for model compatibility)
      num_words     = size(split(prompt, '\\s+'))
      avg_word_len  = (length(prompt) - num_spaces) / num_words  ≈ avg chars/word
      question_flag = 1 if prompt contains 'what|why|how|explain|describe'
      input_tokens  = ceil(length(prompt)   / 4)   # GPT ≈ 4 chars/token
      output_tokens = ceil(length(response) / 4)
    """
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(str(chunk_path))
    )

    raw_count = raw_df.count()
    if raw_count == 0:
        print(f"   [{chunk_label}] ⚠️  No records — skipping.")
        return 0

    # --- Explode turns ---
    turns_df = raw_df.select(
        "id",
        F.posexplode("conversations").alias("pos", "turn")
    ).select(
        "id", "pos",
        F.col("turn.from").alias("sender"),
        F.col("turn.value").alias("value"),
    )

    w = Window.partitionBy("id").orderBy("pos")

    pairs_df = (
        turns_df
        .withColumn("prev_sender", F.lag("sender", 1).over(w))
        .withColumn("prev_value",  F.lag("value",  1).over(w))
        .filter((F.col("sender") == "gpt") & (F.col("prev_sender") == "human"))
        .select(
            F.col("id").alias("conv_id"),
            F.col("pos").alias("turn_pos"),
            F.col("prev_value").alias("prompt"),
            F.col("value").alias("response"),
        )
        # Drop nulls that would break length() calls
        .filter(F.col("prompt").isNotNull() & F.col("response").isNotNull())
    )

    # --- Feature engineering (native Spark only) ---
    # Intermediate helpers
    p_len      = F.length("prompt")                           # char count
    p_spaces   = F.length(F.regexp_replace("prompt", r"\S", ""))  # space count
    p_words    = F.greatest(F.lit(1), p_len - p_spaces)      # avoid div-by-0

    features_df = (
        pairs_df
        .withColumn("text_len",
                    p_len.cast(LongType()))
        .withColumn("context_len",
                    p_len.cast(LongType()))
        .withColumn("num_words",
                    (p_len - p_spaces).cast(LongType()))
        .withColumn("avg_word_len",
                    # avg chars per word ≈ non-space chars / word count
                    (F.length(F.regexp_replace("prompt", r"\s", ""))
                     / p_words).cast("double"))
        .withColumn("question_flag",
                    F.when(
                        F.lower("prompt").rlike(
                            r"(what|why|how|explain|describe)"
                        ), F.lit(1)
                    ).otherwise(F.lit(0)).cast(LongType()))
        .withColumn("input_tokens",
                    F.ceil(p_len / F.lit(4.0)).cast(LongType()))
        .withColumn("output_tokens",
                    F.ceil(F.length("response") / F.lit(4.0)).cast(LongType()))
        .select(
            "conv_id", "turn_pos",
            "text_len", "context_len", "num_words",
            "avg_word_len", "question_flag",
            "input_tokens", "output_tokens",
        )
        .filter((F.col("input_tokens") > 0) & (F.col("output_tokens") > 0))
    )

    features_df.write.mode("append").parquet(OUT_DIR)

    written = features_df.count()
    spark.catalog.clearCache()
    return written

# ---------------------------------------------------------------------------
# 4.  Main loop
# ---------------------------------------------------------------------------
total_rows = 0
for idx, chunk_path in enumerate(chunk_files):
    label = f"{idx+1}/{total_chunks}  {chunk_path.name}"

    if RESUME and progress.get(chunk_path.name) == "done":
        print(f"   [{label}] ⏭️   Already done — skipping.")
        continue

    print(f"\n🔄  [{label}] …", flush=True)
    try:
        rows = process_chunk(chunk_path, label)
        total_rows += rows
        print(f"   [{label}] ✅  {rows:,} rows written", flush=True)

        progress[chunk_path.name] = "done"
        with open(PROGRESS_FILE, "w") as pf:
            json.dump(progress, pf, indent=2)

        chunk_path.unlink()   # free disk space immediately

    except Exception as exc:
        print(f"\n   [{label}] ❌  ERROR: {exc}")
        print("   Progress saved. Re-run to resume from this chunk.")
        spark.stop()
        sys.exit(1)

# ---------------------------------------------------------------------------
# 5.  Cleanup & final report
# ---------------------------------------------------------------------------
remaining = list(CHUNKS_DIR.glob("chunk_*.json"))
if not remaining:
    shutil.rmtree(CHUNKS_DIR, ignore_errors=True)
    PROGRESS_FILE.unlink(missing_ok=True)
    print("\n🧹  Cleaned up temporary chunk files.")

pq_files = _glob.glob(os.path.join(OUT_DIR, "*.parquet"))
print("\n" + "=" * 60)
print("  PREPROCESSING COMPLETE")
print("=" * 60)
print(f"  Chunks processed : {total_chunks}")
print(f"  Rows written     : {total_rows:,}")
print(f"  Part-files       : {len(pq_files)}")
print(f"  Output dir       : {OUT_DIR}")
print("=" * 60)

spark.stop()
print("\n✅  Done! You can now run:  python ml/train_mllib.py")
