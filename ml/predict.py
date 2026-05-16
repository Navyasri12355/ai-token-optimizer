"""
Spark MLlib prediction utilities for token estimation.

Loads PipelineModel artifacts produced by spark/train_mllib.py and
provides both single-prompt and batch prediction.  No sklearn/pickle.
"""

from __future__ import annotations

import os
from typing import Dict

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, IntegerType


# Feature columns must match those used during training
FEATURE_COLS = [
    "context_len",
    "text_len",
    "num_words",
    "avg_word_len",
    "question_flag",
]

FEATURE_SCHEMA = StructType([
    StructField("context_len",   DoubleType(), False),
    StructField("text_len",      DoubleType(), False),
    StructField("num_words",     DoubleType(), False),
    StructField("avg_word_len",  DoubleType(), False),
    StructField("question_flag", DoubleType(), False),
])


class TokenPredictor:
    """Spark MLlib-based token predictor.

    Parameters
    ----------
    model_dir : str
        Directory containing the PipelineModel sub-folders produced by
        spark/train_mllib.py (default: ``spark/models``).
    """

    def __init__(self, model_dir: str = "spark/models"):
        self.spark = (
            SparkSession.builder
            .appName("TokenPrediction")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        self.model_dir = model_dir

        input_path  = os.path.join(model_dir, "input_token_model")
        output_path = os.path.join(model_dir, "output_token_model")

        if not os.path.exists(input_path) or not os.path.exists(output_path):
            raise FileNotFoundError(
                f"Model artifacts not found under '{model_dir}'. "
                "Run spark/train_mllib.py first."
            )

        self.model_input  = PipelineModel.load(input_path)
        self.model_output = PipelineModel.load(output_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_spark(self, data: pd.DataFrame):
        """Convert a pandas DataFrame to a Spark DataFrame with correct types."""
        casted = data.copy()
        for col in FEATURE_COLS:
            casted[col] = casted[col].astype(float)
        return self.spark.createDataFrame(casted, schema=FEATURE_SCHEMA)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def predict(self, data: pd.DataFrame) -> Dict[str, int]:
        """Predict token counts for a *single* row.

        Parameters
        ----------
        data : pd.DataFrame
            One-row DataFrame with columns matching ``FEATURE_COLS``.

        Returns
        -------
        dict
            ``{"input_tokens": int, "output_tokens": int}``
        """
        spark_df = self._to_spark(data)

        input_val  = (
            self.model_input.transform(spark_df)
            .select("prediction")
            .collect()[0][0]
        )
        output_val = (
            self.model_output.transform(spark_df)
            .select("prediction")
            .collect()[0][0]
        )

        return {
            "input_tokens":  int(round(input_val)),
            "output_tokens": int(round(output_val)),
        }

    def batch_predict(self, data: pd.DataFrame) -> pd.DataFrame:
        """Batch prediction for multiple rows.

        Parameters
        ----------
        data : pd.DataFrame
            Multi-row DataFrame with feature columns.

        Returns
        -------
        pd.DataFrame
            Original feature columns plus ``predicted_input_tokens`` and
            ``predicted_output_tokens``.
        """
        spark_df = self._to_spark(data)

        # Add a monotonically increasing row id for joining
        spark_df = spark_df.withColumn("_row_id", F.monotonically_increasing_id())

        preds_in  = (
            self.model_input.transform(spark_df)
            .select("_row_id", F.col("prediction").alias("predicted_input_tokens"))
        )
        preds_out = (
            self.model_output.transform(spark_df)
            .select("_row_id", F.col("prediction").alias("predicted_output_tokens"))
        )

        result = (
            spark_df
            .join(preds_in,  on="_row_id")
            .join(preds_out, on="_row_id")
            .drop("_row_id", "raw_features", "features")
        )

        return result.toPandas()

    def close(self):
        """Stop the Spark session."""
        self.spark.stop()


# ---------------------------------------------------------------------------
# Quick smoke-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    predictor = TokenPredictor()

    sample = pd.DataFrame({
        "context_len":   [150.0],
        "text_len":      [150.0],
        "num_words":     [25.0],
        "avg_word_len":  [6.0],
        "question_flag": [1.0],
    })

    result = predictor.predict(sample)
    print(f"Predicted input  tokens : {result['input_tokens']}")
    print(f"Predicted output tokens : {result['output_tokens']}")

    predictor.close()
