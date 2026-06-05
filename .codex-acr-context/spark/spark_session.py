"""
spark/spark_session.py
=======================
Shared SparkSession factory.

Ensures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to the same Python
interpreter that launched the current process, fixing the common Windows issue
where PySpark workers use a system Python (e.g. C:\\Python313) instead of the
active conda/venv interpreter.
"""

import os
import sys
from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "TokenOptimizer",
    driver_memory: str = "4g",
    shuffle_partitions: int = 8,
    extra_configs: dict = None,
) -> SparkSession:
    """
    Create or retrieve a SparkSession with correct Python interpreter paths.

    Args:
        app_name: Spark application name.
        driver_memory: Driver heap size (e.g. '4g').
        shuffle_partitions: spark.sql.shuffle.partitions.
        extra_configs: Additional Spark config key/value pairs.

    Returns:
        Active SparkSession.
    """
    # Always point Spark workers at the same Python that runs this script
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON",        python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory",              driver_memory)
        .config("spark.driver.maxResultSize",       "2g")
        .config("spark.sql.shuffle.partitions",     str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled",       "true")
        .config("spark.pyspark.python",             python_exec)
        .config("spark.pyspark.driver.python",      python_exec)
        .config("spark.ui.showConsoleProgress",     "false")
        # G1GC is better than default GC for large heaps — reduces OOM risk
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:G1HeapRegionSize=32m "
                "-XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35")
    )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
