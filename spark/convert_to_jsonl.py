"""
spark/convert_to_jsonl.py
==========================
Streaming converter: raw.json (one giant JSON array) → raw.jsonl (one record per line).

Uses a brace-depth tracker so it NEVER loads the full file into RAM.
Memory usage is O(1 record) regardless of file size.

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

ROOT = Path(__file__).resolve().parent.parent


def convert_streaming(
    input_path: str,
    output_path: str,
    limit: int = None,
    chunk_size: int = 2 * 1024 * 1024,   # 2 MB read chunks
    report_every: int = 5_000,
) -> int:
    """
    Stream-parse a JSON array file and write each top-level object as one line.

    Works character-by-character using a brace/string-escape state machine —
    no external dependencies, memory usage stays flat.

    Returns number of records written.
    """
    t0 = time.time()
    count = 0

    with open(input_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        # State machine variables
        depth        = 0      # brace nesting depth
        in_string    = False  # are we inside a JSON string?
        escape_next  = False  # next char is escaped
        obj_start    = None   # index in `buf` where current object started
        buf          = ""     # rolling string buffer

        while True:
            chunk = fin.read(chunk_size)
            if not chunk:
                break
            buf += chunk

            i = 0
            while i < len(buf):
                c = buf[i]

                if escape_next:
                    escape_next = False

                elif in_string:
                    if c == "\\":
                        escape_next = True
                    elif c == '"':
                        in_string = False

                else:
                    if c == '"':
                        in_string = True

                    elif c == "{":
                        if depth == 0:
                            obj_start = i        # mark start of new top-level object
                        depth += 1

                    elif c == "}":
                        depth -= 1
                        if depth == 0 and obj_start is not None:
                            raw_obj = buf[obj_start : i + 1]
                            try:
                                obj = json.loads(raw_obj)
                                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                                count += 1
                                if count % report_every == 0:
                                    elapsed = time.time() - t0
                                    rate    = count / elapsed
                                    print(
                                        f"\r  {count:>8,} records written  "
                                        f"({rate:,.0f} rec/s)  "
                                        f"elapsed {elapsed:.0f}s   ",
                                        end="", flush=True,
                                    )
                                if limit and count >= limit:
                                    print()
                                    return count
                            except json.JSONDecodeError as exc:
                                print(f"\n[WARN] Skipping malformed record at "
                                      f"pos ~{i}: {exc}", flush=True)

                            # Trim the buffer to just after this object
                            buf = buf[i + 1:]
                            i   = -1          # loop will increment to 0
                            obj_start = None

                i += 1

            # Keep only the unprocessed tail in buf to avoid unbounded growth
            if obj_start is not None:
                buf = buf[obj_start:]
                obj_start = 0
            else:
                buf = ""

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
    ap.add_argument(
        "--chunk-mb",
        type=int, default=2,
        help="Read chunk size in MB (default: 2)",
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
        chunk_size  = args.chunk_mb * 1024 * 1024,
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
