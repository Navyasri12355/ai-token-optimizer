"""
Quick analysis of output_tokens distribution
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import glob
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TokenDistributionAnalysis")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

PARQUET_DIR = "data/processed.parquet"
print(f"[*] Loading parquet from {PARQUET_DIR} ...")
df = spark.read.parquet(PARQUET_DIR)

# Cast output_tokens
df = df.withColumn("output_tokens", F.col("output_tokens").cast("double"))

# Compute statistics
stats = df.agg(
    F.avg("output_tokens").alias("mean"),
    F.percentile_approx("output_tokens", 0.5).alias("median"),
    F.stddev("output_tokens").alias("stddev"),
    F.min("output_tokens").alias("min"),
    F.max("output_tokens").alias("max"),
    F.percentile_approx("output_tokens", 0.25).alias("p25"),
    F.percentile_approx("output_tokens", 0.75).alias("p75"),
).collect()[0]

print("\n" + "="*60)
print("  OUTPUT TOKENS DISTRIBUTION")
print("="*60)
print(f"  Mean:                {stats['mean']:,.1f} tokens")
print(f"  Median:              {stats['median']:,.1f} tokens")
print(f"  Std Dev:             {stats['stddev']:,.1f} tokens")
print(f"  Min:                 {stats['min']:,.1f} tokens")
print(f"  Max:                 {stats['max']:,.1f} tokens")
print(f"  25th percentile:     {stats['p25']:,.1f} tokens")
print(f"  75th percentile:     {stats['p75']:,.1f} tokens")
print("="*60)

# Calculate error relative to mean
mean_val = stats['mean']
rmse_val = 271.0
error_pct = (rmse_val / mean_val) * 100

print(f"\n  RMSE of 271 tokens as % of mean:")
print(f"    Error: {error_pct:.1f}%")
if error_pct < 15:
    print(f"    ✅ ACCEPTABLE for inference")
elif error_pct < 25:
    print(f"    ⚠️  BORDERLINE - use with caution")
else:
    print(f"    ❌ POOR - needs improvement")

spark.stop()
