"""
spark/cost_analysis.py
=======================
Estimates API cost of conversations before and after optimisation,
computes savings, and logs/visualises the results.

Supported model pricing (per 1 000 tokens, USD):
  gpt-4o          input $0.005   output $0.015
  gpt-4-turbo     input $0.010   output $0.030
  gpt-3.5-turbo   input $0.0015  output $0.002
  claude-3-opus   input $0.015   output $0.075
  claude-3-sonnet input $0.003   output $0.015
  claude-3-haiku  input $0.00025 output $0.00125

Usage:
  python spark/cost_analysis.py                          # full dataset
  python spark/cost_analysis.py --sample                 # use 10k sample
  python spark/cost_analysis.py --model gpt-4-turbo
"""

import sys
import os
import json
import time
import logging
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from spark.elk_logger import get_elk_logger
    logger = get_elk_logger("cost_analysis")
except Exception:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("cost_analysis")

# ── Paths ──────────────────────────────────────────────────────────────────────
PROCESSED_PARQUET = str(ROOT / "spark" / "output" / "processed")
SAMPLE_PARQUET    = str(ROOT / "spark" / "output" / "sample")
COST_REPORT_PATH  = str(ROOT / "spark" / "output" / "cost_report.json")
PLOT_DIR          = str(ROOT / "spark" / "output" / "plots")

# ── Pricing table (per 1 000 tokens, USD) ─────────────────────────────────────
PRICING: Dict[str, Dict[str, float]] = {
    "gpt-4o":           {"input": 0.005,   "output": 0.015},
    "gpt-4-turbo":      {"input": 0.010,   "output": 0.030},
    "gpt-3.5-turbo":    {"input": 0.0015,  "output": 0.002},
    "claude-3-opus":    {"input": 0.015,   "output": 0.075},
    "claude-3-sonnet":  {"input": 0.003,   "output": 0.015},
    "claude-3-haiku":   {"input": 0.00025, "output": 0.00125},
}

DEFAULT_MODEL = "gpt-4o"


def _cost_usd(token_count: int, rate_per_1k: float) -> float:
    """Simple cost calculation."""
    return (token_count / 1000.0) * rate_per_1k


def run_cost_analysis(
    use_sample: bool = False,
    parquet_path: str = None,
    model: str = DEFAULT_MODEL,
    cost_report_path: str = COST_REPORT_PATH,
    plot_dir: str = PLOT_DIR,
    push_to_elk: bool = True,
):
    """
    Compute cost estimates and savings per conversation/turn.

    Args:
        use_sample: Use 10k-row sample.
        parquet_path: Override processed parquet path.
        model: Pricing model key (see PRICING dict).
        cost_report_path: Output JSON report path.
        plot_dir: Directory to save matplotlib charts.
        push_to_elk: Push summary metrics to Elasticsearch.

    Returns:
        dict  full cost report
    """
    if model not in PRICING:
        raise ValueError(f"Unknown model '{model}'. Choose from: {list(PRICING)}")

    t0 = time.time()
    parquet_path = parquet_path or (SAMPLE_PARQUET if use_sample else PROCESSED_PARQUET)
    prices = PRICING[model]

    logger.info("💰 Starting cost analysis")
    logger.info(f"   model    : {model}")
    logger.info(f"   parquet  : {parquet_path}")
    logger.info(f"   in_rate  : ${prices['input']}/1k tokens")
    logger.info(f"   out_rate : ${prices['output']}/1k tokens")

    from spark.spark_session import get_spark
    spark = get_spark(app_name="TokenOptimizerCostAnalysis", shuffle_partitions=4)

    # ── Load processed data ────────────────────────────────────────────────────
    df = spark.read.parquet(parquet_path)

    # ── Assign input / output roles ────────────────────────────────────────────
    # "human" turns = API input tokens; "gpt" turns = output tokens
    in_rate  = prices["input"]
    out_rate = prices["output"]

    df = (
        df
        .withColumn("cost_raw_usd",
            F.when(F.col("role") == "human",
                   F.col("token_count")           / 1000.0 * in_rate)
            .otherwise(
                   F.col("token_count")           / 1000.0 * out_rate))
        .withColumn("cost_opt_usd",
            F.when(F.col("role") == "human",
                   F.col("optimized_token_count") / 1000.0 * in_rate)
            .otherwise(
                   F.col("optimized_token_count") / 1000.0 * out_rate))
        .withColumn("cost_saved_usd",
                    F.col("cost_raw_usd") - F.col("cost_opt_usd"))
    )

    # ── Aggregate per conversation ─────────────────────────────────────────────
    conv_costs = (
        df.groupBy("record_id")
        .agg(
            F.sum("cost_raw_usd").alias("conv_cost_raw"),
            F.sum("cost_opt_usd").alias("conv_cost_opt"),
            F.sum("cost_saved_usd").alias("conv_cost_saved"),
            F.sum("token_count").alias("conv_tokens_raw"),
            F.sum("optimized_token_count").alias("conv_tokens_opt"),
            F.count("*").alias("conv_turns"),
        )
        .withColumn("conv_savings_pct",
                    F.when(F.col("conv_cost_raw") > 0,
                           F.col("conv_cost_saved") / F.col("conv_cost_raw") * 100.0)
                    .otherwise(F.lit(0.0)))
    )
    conv_costs.cache()

    # ── Global totals ──────────────────────────────────────────────────────────
    totals = (
        conv_costs.agg(
            F.count("*").alias("total_conversations"),
            F.sum("conv_cost_raw").alias("total_cost_raw_usd"),
            F.sum("conv_cost_opt").alias("total_cost_opt_usd"),
            F.sum("conv_cost_saved").alias("total_cost_saved_usd"),
            F.sum("conv_tokens_raw").alias("total_tokens_raw"),
            F.sum("conv_tokens_opt").alias("total_tokens_opt"),
            F.mean("conv_cost_raw").alias("avg_cost_raw_usd"),
            F.mean("conv_cost_opt").alias("avg_cost_opt_usd"),
            F.mean("conv_savings_pct").alias("avg_savings_pct"),
            F.max("conv_cost_saved").alias("max_savings_single_conv_usd"),
        ).collect()[0]
    )

    # ── Per-model comparison ───────────────────────────────────────────────────
    model_comparison = {}
    for m, p in PRICING.items():
        ir, or_ = p["input"], p["output"]
        tc_raw = int(totals["total_tokens_raw"] or 0)
        tc_opt = int(totals["total_tokens_opt"] or 0)
        # rough 60/40 human/gpt split assumption for total comparison
        raw_cost  = tc_raw * 0.6 / 1000 * ir + tc_raw * 0.4 / 1000 * or_
        opt_cost  = tc_opt * 0.6 / 1000 * ir + tc_opt * 0.4 / 1000 * or_
        model_comparison[m] = {
            "raw_cost_usd": round(raw_cost, 4),
            "opt_cost_usd": round(opt_cost, 4),
            "saved_usd":    round(raw_cost - opt_cost, 4),
            "savings_pct":  round((raw_cost - opt_cost) / raw_cost * 100, 2)
                            if raw_cost > 0 else 0.0,
        }

    elapsed = time.time() - t0

    # ── Build report dict ──────────────────────────────────────────────────────
    report = {
        "model": model,
        "pricing": prices,
        "elapsed_seconds": round(elapsed, 2),
        "totals": {k: (round(float(v), 6) if v is not None else None)
                   for k, v in totals.asDict().items()},
        "all_model_comparison": model_comparison,
    }

    os.makedirs(os.path.dirname(cost_report_path), exist_ok=True)
    with open(cost_report_path, "w") as fh:
        json.dump(report, fh, indent=2)

    # ── Console summary ────────────────────────────────────────────────────────
    t = report["totals"]
    logger.info("=" * 60)
    logger.info(f"💰 Cost Analysis – {model}")
    logger.info(f"   Conversations    : {int(t.get('total_conversations',0)):,}")
    logger.info(f"   Raw tokens       : {int(t.get('total_tokens_raw',0)):,}")
    logger.info(f"   Optimised tokens : {int(t.get('total_tokens_opt',0)):,}")
    logger.info(f"   Raw cost         : ${t.get('total_cost_raw_usd',0):.4f}")
    logger.info(f"   Optimised cost   : ${t.get('total_cost_opt_usd',0):.4f}")
    logger.info(f"   💵 SAVED         : ${t.get('total_cost_saved_usd',0):.4f}  "
                f"({t.get('avg_savings_pct',0):.1f}% avg)")
    logger.info(f"   Report saved     : {cost_report_path}")
    logger.info("=" * 60)

    # ── Visualisations ─────────────────────────────────────────────────────────
    try:
        _make_plots(report, conv_costs.toPandas(), plot_dir, model)
    except Exception as e:
        logger.warning(f"⚠️  Could not generate plots: {e}")

    # ── Push to ELK ───────────────────────────────────────────────────────────
    if push_to_elk:
        try:
            from metrics import MetricsCollector
            mc = MetricsCollector()
            mc.record_data_processing(
                operation="cost_analysis",
                rows_processed=int(t.get("total_conversations", 0)),
                processing_time_seconds=elapsed,
                model=model,
                total_cost_raw_usd=t.get("total_cost_raw_usd", 0),
                total_cost_opt_usd=t.get("total_cost_opt_usd", 0),
                total_savings_usd=t.get("total_cost_saved_usd", 0),
                avg_savings_pct=t.get("avg_savings_pct", 0),
            )
            logger.info("📡 Cost metrics pushed to Elasticsearch")
        except Exception as e:
            logger.warning(f"⚠️  Elasticsearch push failed: {e}")

    spark.stop()
    return report


# ── Matplotlib visualisations ─────────────────────────────────────────────────
def _make_plots(report: dict, conv_df, plot_dir: str, model: str):
    """Generate cost-analysis charts and save to plot_dir."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    import numpy as np

    os.makedirs(plot_dir, exist_ok=True)
    plt.style.use("dark_background")
    ACCENT  = "#7c3aed"
    ACCENT2 = "#06b6d4"
    RED     = "#ef4444"

    # 1️⃣ Bar chart – Raw vs Optimised cost per model ──────────────────────────
    fig, ax = plt.subplots(figsize=(12, 5))
    comp = report["all_model_comparison"]
    names  = list(comp.keys())
    raw_c  = [comp[m]["raw_cost_usd"] for m in names]
    opt_c  = [comp[m]["opt_cost_usd"] for m in names]
    x      = np.arange(len(names))
    w      = 0.35
    bars1  = ax.bar(x - w/2, raw_c, w, label="Raw Cost",       color=RED,     alpha=0.85)
    bars2  = ax.bar(x + w/2, opt_c, w, label="Optimised Cost", color=ACCENT2, alpha=0.85)
    ax.set_xticks(x); ax.set_xticklabels(names, rotation=20, ha="right")
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter("$%.3f"))
    ax.set_title("Estimated API Cost – Raw vs Optimised (all models)", fontsize=14)
    ax.legend()
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(os.path.join(plot_dir, "cost_comparison_models.png"), dpi=140)
    plt.close(fig)
    logger.info("   📊 Saved cost_comparison_models.png")

    # 2️⃣ Histogram – savings % per conversation ───────────────────────────────
    if "conv_savings_pct" in conv_df.columns:
        fig, ax = plt.subplots(figsize=(10, 4))
        data = conv_df["conv_savings_pct"].dropna()
        ax.hist(data, bins=50, color=ACCENT, edgecolor="none", alpha=0.85)
        ax.axvline(data.mean(), color="yellow", linestyle="--",
                   linewidth=1.5, label=f"Mean {data.mean():.1f}%")
        ax.set_xlabel("Token Savings (%)")
        ax.set_ylabel("Number of Conversations")
        ax.set_title("Distribution of Token Savings per Conversation")
        ax.legend()
        ax.grid(alpha=0.2)
        fig.tight_layout()
        fig.savefig(os.path.join(plot_dir, "savings_distribution.png"), dpi=140)
        plt.close(fig)
        logger.info("   📊 Saved savings_distribution.png")

    # 3️⃣ Pie – savings share ──────────────────────────────────────────────────
    t = report["totals"]
    raw_usd  = t.get("total_cost_raw_usd", 1) or 1
    opt_usd  = t.get("total_cost_opt_usd", 0)
    saved    = max(0.0, raw_usd - opt_usd)
    fig, ax  = plt.subplots(figsize=(6, 6))
    ax.pie(
        [opt_usd, saved],
        labels=["Remaining Cost", "Cost Saved"],
        colors=[ACCENT, "#22c55e"],
        autopct="%1.1f%%",
        startangle=140,
        textprops={"color": "white"},
    )
    ax.set_title(f"Cost Savings — {model}", fontsize=14)
    fig.tight_layout()
    fig.savefig(os.path.join(plot_dir, f"savings_pie_{model.replace('-','_')}.png"),
                dpi=140)
    plt.close(fig)
    logger.info(f"   📊 Saved savings_pie_{model}.png")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Token cost analysis")
    ap.add_argument("--sample",  action="store_true")
    ap.add_argument("--model",   default=DEFAULT_MODEL,
                    choices=list(PRICING.keys()))
    ap.add_argument("--parquet", default=None)
    args = ap.parse_args()

    run_cost_analysis(
        use_sample=args.sample,
        parquet_path=args.parquet,
        model=args.model,
    )
