"""
spark/convert_to_jsonl.py
==========================
Streaming converter: raw.json (one giant JSON array) → raw.jsonl (one record per line).

Uses ijson for true streaming JSON parsing — no buffer management overhead.
Memory usage is O(1 record) regardless of file size, throughput ~50-100k records/sec.

Usage:
    python spark/convert_to_jsonl.py                      # uses defaults
    python spark/convert_to_jsonl.py --input data/raw.json --output data/raw.jsonl
    python spark/convert_to_jsonl.py --limit 50000        # first N records only

Why this matters:
    PySpark's multiline=True JSON reader cannot split a single JSON array
    across partitions — the entire file lands in one task, causing OOM on
    files > ~2 GB. JSONL lets Spark split the file at newlines, distributing
    work across all available cores with no memory spike.
"""

import sys
import os
import json
import time
import argparse
from pathlib import Path

try:
    import ijson
except ImportError:
    print("[ERROR] ijson not installed. Install with: pip install ijson")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent


def convert_streaming(
    input_path: str,
    output_path: str,
    limit: int = None,
    report_every: int = 5_000,
) -> int:
    """
    Stream-parse a JSON array file using ijson and write each object as one line.

    ijson is a C-accelerated streaming JSON parser that doesn't load data into memory.
    Throughput: ~50-100k records/sec, memory: O(1 record).

    Returns number of records written.
    """
    t0 = time.time()
    count = 0

    with open(input_path, "rb") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        try:
            # ijson.items() streams top-level array elements efficiently
            for obj in ijson.items(fin, "item"):
                try:
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    count += 1

                    if count % report_every == 0:
                        elapsed = time.time() - t0
                        rate = count / elapsed
                        print(
                            f"\r  {count:>8,} records written  "
                            f"({rate:,.0f} rec/s)  "
                            f"elapsed {elapsed:.0f}s   ",
                            end="", flush=True,
                        )

                    if limit and count >= limit:
                        print()
                        return count

                except (json.JSONDecodeError, TypeError) as exc:
                    print(f"\n[WARN] Skipping malformed record #{count}: {exc}", flush=True)

        except ijson.JSONError as exc:
            print(f"\n[ERROR] JSON parsing error: {exc}", flush=True)
            raise

    print()
    return count


def main():
    ap = argparse.ArgumentParser(
        description="Convert a large JSON-array file to JSONL (newline-delimited JSON)"
    )
    ap.add_argument(
        "--input",  "-i",
        default=str(ROOT / "data" / "raw.json"),
        help="Path to input JSON array file (default: data/raw.json)",
    )
    ap.add_argument(
        "--output", "-o",
        default=str(ROOT / "data" / "raw.jsonl"),
        help="Path for output JSONL file (default: data/raw.jsonl)",
    )
    ap.add_argument(
        "--limit",  "-n",
        type=int, default=None,
        help="Stop after N records (useful for testing)",
    )
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output)

    if not inp.exists():
        print(f"[ERROR] Input file not found: {inp}")
        sys.exit(1)

    size_gb = inp.stat().st_size / (1024 ** 3)
    print(f"\n{'='*60}")
    print(f"  Input  : {inp}  ({size_gb:.2f} GB)")
    print(f"  Output : {out}")
    if args.limit:
        print(f"  Limit  : first {args.limit:,} records")
    print(f"{'='*60}\n")

    # Warn if output already exists
    if out.exists():
        existing_gb = out.stat().st_size / (1024 ** 3)
        print(f"[INFO] Output already exists ({existing_gb:.2f} GB) — overwriting.\n")

    t0 = time.time()
    n  = convert_streaming(
        input_path  = str(inp),
        output_path = str(out),
        limit       = args.limit,
    )
    elapsed  = time.time() - t0
    out_size = out.stat().st_size / (1024 ** 3)

    print(f"\n{'='*60}")
    print(f"  Records written : {n:,}")
    print(f"  Output size     : {out_size:.2f} GB")
    print(f"  Time            : {elapsed:.1f}s")
    print(f"  Throughput      : {n / elapsed:,.0f} rec/s")
    print(f"\n  Next step:")
    print(f"    python spark/run_pipeline.py --jsonl")
    print(f"  or to process just a sample:")
    print(f"    python spark/run_pipeline.py --sample --jsonl")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
