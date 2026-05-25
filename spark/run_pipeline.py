"""
spark/run_pipeline.py
======================
Master orchestrator for the full token-optimizer Spark pipeline.

Stages (all optional via --skip-* flags):
  1. preprocess   – load raw.json, extract features, save Parquet
  2. train        – train Ridge/GBT models with Spark MLlib
  3. cost         – compute cost estimates and savings
  4. dashboards   – push dashboards to Kibana (requires ELK running)

Quick start (sample mode for testing):
  python spark/run_pipeline.py --sample

Full run:
  python spark/run_pipeline.py

Skip specific stages:
  python spark/run_pipeline.py --skip-train --skip-dashboards
"""

import sys
import os
import json
import time
import argparse
import logging
import traceback
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from spark.elk_logger import get_elk_logger, push_event
    logger = get_elk_logger("pipeline")
except Exception:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("pipeline")
    def push_event(*args, **kwargs): pass


PIPELINE_REPORT = str(ROOT / "spark" / "output" / "pipeline_report.json")


def _banner(title: str):
    width = 62
    logger.info("╔" + "═" * width + "╗")
    logger.info("║  " + title.ljust(width - 2) + "║")
    logger.info("╚" + "═" * width + "╝")


def _stage(name: str, fn, *args, **kwargs):
    """Run a pipeline stage, timing it and logging success/failure."""
    _banner(f"STAGE: {name}")
    t0 = time.time()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.time() - t0
        logger.info(f"✅ {name} completed in {elapsed:.1f}s")
        push_event(f"stage_{name.lower().replace(' ', '_')}_done",
                   {"status": "success", "elapsed_s": elapsed})
        return result, elapsed, None
    except Exception as exc:
        elapsed = time.time() - t0
        tb = traceback.format_exc()
        logger.error(f"{name} FAILED after {elapsed:.1f}s")
        logger.error(f"  Error type : {type(exc).__name__}")
        logger.error(f"  Error msg  : {exc}")
        logger.error("  Traceback:\n" + tb)
        push_event(f"stage_{name.lower().replace(' ', '_')}_failed",
                   {"status": "error", "error": str(exc), "elapsed_s": elapsed})
        return None, elapsed, str(exc)



def run_pipeline(args):
    """Execute the full pipeline."""
    t_start = time.time()
    run_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
    report   = {"run_id": run_id, "args": vars(args), "stages": {}}

    _banner(f"AI Token Optimizer – Spark Pipeline  [{run_id}]")
    logger.info(f"   mode      : {'SAMPLE' if args.sample else 'FULL'}")
    logger.info(f"   raw_json  : {args.raw_json}")
    logger.info(f"   models    : {args.models_dir}")
    logger.info(f"   kibana    : {args.kibana}")

    push_event("pipeline_started", {"run_id": run_id, "sample": args.sample})

    raw_jsonl = str(Path(args.raw_json).with_suffix(".jsonl"))

    # ── Stage 0: Convert JSON → JSONL (one-time, fixes OOM) ───────────────────
    if not args.skip_preprocess and not os.path.exists(raw_jsonl) and getattr(args, "auto_convert", False):
        _banner("Converting raw.json -> raw.jsonl")
        logger.info("raw.jsonl not found. Running one-time converter ...")
        logger.info(f"   Input  : {args.raw_json}  (~6.8 GB)")
        logger.info(f"   Output : {raw_jsonl}")
        logger.info("   This may take 10-20 minutes. Run it separately with:")
        logger.info("   python spark/convert_to_jsonl.py")
        from spark.convert_to_jsonl import convert_streaming
        limit = args.sample_size if args.sample else None
        n = convert_streaming(args.raw_json, raw_jsonl, limit=limit)
        logger.info(f"   Converted {n:,} records to JSONL")
        report["stages"]["convert"] = {"records": n}
    elif not os.path.exists(raw_jsonl) and not args.skip_preprocess:
        logger.warning(
            "raw.jsonl not found - using multiline JSON (may OOM on large files). "
            "Fix: run  python spark/convert_to_jsonl.py  first, then re-run pipeline."
        )

    # ── Stage 1: Preprocessing ─────────────────────────────────────────────────
    if not args.skip_preprocess:
        from spark.preprocess import run_preprocessing
        result, elapsed, err = _stage(
            "Preprocessing",
            run_preprocessing,
            sample_size=args.sample_size if args.sample else None,
            raw_json=args.raw_json,
            jsonl_path=raw_jsonl if os.path.exists(raw_jsonl) else None,
        )
        report["stages"]["preprocess"] = {
            "elapsed_s": elapsed, "error": err, "result": result
        }
        if err and not args.continue_on_error:
            logger.error("Preprocessing failed. Run with --continue-on-error to skip.")
            logger.error("Full error above. Common causes:")
            logger.error("  - OOM on large file: run  python spark/convert_to_jsonl.py  first")
            logger.error("  - Or use: python spark/run_pipeline.py --auto-convert")
            logger.error("  - raw.json not found: check --raw-json path")
            sys.exit(1)
    else:
        logger.info("Skipping preprocessing")


    # ── Stage 2: Model Training ────────────────────────────────────────────────
    if not args.skip_train:
        from spark.train_model import train
        result, elapsed, err = _stage(
            "Model Training",
            train,
            use_sample=args.sample,
            models_dir=args.models_dir,
            cv_folds=args.cv_folds,
        )
        report["stages"]["train"] = {
            "elapsed_s": elapsed, "error": err, "result": result
        }
        if err and not args.continue_on_error:
            logger.error("Pipeline aborted")
            sys.exit(1)
    else:
        logger.info("⏭️  Skipping training")

    # ── Stage 3: Cost Analysis ─────────────────────────────────────────────────
    if not args.skip_cost:
        from spark.cost_analysis import run_cost_analysis
        result, elapsed, err = _stage(
            "Cost Analysis",
            run_cost_analysis,
            use_sample=args.sample,
            model=args.pricing_model,
        )
        report["stages"]["cost"] = {
            "elapsed_s": elapsed, "error": err, "result": result
        }
    else:
        logger.info("⏭️  Skipping cost analysis")

    # ── Stage 4: Kibana Dashboards ─────────────────────────────────────────────
    if not args.skip_dashboards:
        from spark.kibana_dashboards import setup_all_dashboards, wait_for_kibana
        if wait_for_kibana(args.kibana, retries=5):
            result, elapsed, err = _stage(
                "Kibana Dashboards",
                setup_all_dashboards,
                kibana_url=args.kibana,
            )
            report["stages"]["dashboards"] = {
                "elapsed_s": elapsed, "error": err
            }
        else:
            logger.warning("⚠️  Kibana not available – skipping dashboard setup")
            report["stages"]["dashboards"] = {
                "elapsed_s": 0, "error": "Kibana not reachable"
            }
    else:
        logger.info("⏭️  Skipping Kibana dashboards")

    # ── Final report ───────────────────────────────────────────────────────────
    total_elapsed = time.time() - t_start
    report["total_elapsed_s"] = round(total_elapsed, 2)
    report["status"] = "success" if all(
        s.get("error") is None
        for s in report["stages"].values()
    ) else "partial"

    os.makedirs(os.path.dirname(PIPELINE_REPORT), exist_ok=True)
    with open(PIPELINE_REPORT, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    push_event("pipeline_complete", {
        "run_id": run_id,
        "total_elapsed_s": total_elapsed,
        "status": report["status"],
    })

    _banner("Pipeline Complete")
    logger.info(f"   Status      : {report['status'].upper()}")
    logger.info(f"   Total time  : {total_elapsed:.1f}s")
    logger.info(f"   Report      : {PIPELINE_REPORT}")
    logger.info(f"   Kibana      : {args.kibana}/app/dashboards")
    logger.info("")


# ── CLI ────────────────────────────────────────────────────────────────────────
def _build_parser():
    ap = argparse.ArgumentParser(
        description="AI Token Optimizer – Spark Pipeline Orchestrator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--sample", action="store_true",
                    help="Use sample instead of full dataset (fast for testing)")
    ap.add_argument("--sample-size", type=int, default=50_000,
                    help="Conversations to use in sample mode")
    ap.add_argument("--raw-json",
                    default=str(ROOT / "data" / "raw.json"))
    ap.add_argument("--jsonl", action="store_true",
                    help="Prefer JSONL input if raw.jsonl already exists")
    ap.add_argument("--auto-convert", action="store_true",
                    help="Auto-convert raw.json → raw.jsonl before preprocessing "
                         "(one-time, fixes OOM on large files)")
    ap.add_argument("--models-dir",
                    default=str(ROOT / "spark" / "models"))
    ap.add_argument("--kibana",
                    default=os.environ.get("KIBANA_URL", "http://localhost:5601"))
    ap.add_argument("--pricing-model", default="gpt-4o",
                    choices=["gpt-4o", "gpt-4-turbo", "gpt-3.5-turbo",
                             "claude-3-opus", "claude-3-sonnet", "claude-3-haiku"])
    ap.add_argument("--cv-folds", type=int, default=3)
    ap.add_argument("--skip-preprocess",  action="store_true")
    ap.add_argument("--skip-train",       action="store_true")
    ap.add_argument("--skip-cost",        action="store_true")
    ap.add_argument("--skip-dashboards",  action="store_true")
    ap.add_argument("--continue-on-error", action="store_true",
                    help="Continue pipeline even if a stage fails")
    return ap


if __name__ == "__main__":
    parser = _build_parser()
    run_pipeline(parser.parse_args())
