from __future__ import annotations

"""
Spark-free token predictor.

Reads the Ridge regression coefficients directly from the saved Parquet files
(no Spark / Java runtime needed).  The pipeline is:
  VectorAssembler → StandardScaler → LinearRegression

Features used (must match training order):
  char_count, word_count, sentence_count, avg_word_length,
  conversation_turn_count, turn_index, has_code_block_int
"""

import glob
from pathlib import Path

import numpy as np
import pandas as pd


FEATURE_COLS = [
    "char_count",
    "word_count",
    "sentence_count",
    "avg_word_length",
    "conversation_turn_count",
    "turn_index",
    "has_code_block_int",
]


class TokenPredictor:
    """Pure numpy Ridge predictor — no Spark or Java required."""

    def __init__(self, model_dir: str = "spark/models") -> None:
        self.model_dir = Path(model_dir)
        model_path = self._resolve_model_path()
        stages = model_path / "stages"

        # ── StandardScaler ────────────────────────────────────────────────
        scaler_parquet = self._first_parquet(stages, "1_StandardScaler*")
        scaler_row = pd.read_parquet(scaler_parquet).iloc[0]
        self._scaler_mean = np.array(scaler_row["mean"]["values"], dtype=float)
        std_raw = np.array(scaler_row["std"]["values"], dtype=float)
        # Avoid divide-by-zero for zero-variance features
        self._scaler_std = np.where(std_raw == 0, 1.0, std_raw)

        # ── LinearRegression ──────────────────────────────────────────────
        lr_parquet = self._first_parquet(stages, "2_LinearRegression*")
        lr_row = pd.read_parquet(lr_parquet).iloc[0]
        self._intercept = float(lr_row["intercept"])
        self._coef = np.array(lr_row["coefficients"]["values"], dtype=float)

    # ─────────────────────────────────────────────────────────────────────
    def _resolve_model_path(self) -> Path:
        for name in ("ridge_opt_token_count", "ridge_token_count", "cv_ridge_token_count"):
            p = self.model_dir / name
            if p.exists():
                return p
        raise FileNotFoundError(f"No token model found under {self.model_dir}")

    @staticmethod
    def _first_parquet(stages_dir: Path, pattern: str) -> str:
        hits = glob.glob(str(stages_dir / pattern / "data" / "*.parquet"))
        if not hits:
            raise FileNotFoundError(f"No parquet under {stages_dir / pattern}")
        return hits[0]

    # ─────────────────────────────────────────────────────────────────────
    def predict(self, features: pd.DataFrame) -> dict[str, int]:
        """
        Accepts a DataFrame whose column names are raw text-level features
        (char_count, word_count, …) OR the legacy API features
        (text_len, num_words, …) — maps either to the trained feature vector.
        """
        row = self._map_features(features)

        # StandardScaler transform
        scaled = (row - self._scaler_mean) / self._scaler_std

        # Linear prediction
        predicted = float(np.dot(scaled, self._coef) + self._intercept)
        input_tokens = max(1, int(round(predicted)))

        return {
            "input_tokens": input_tokens,
            "output_tokens": max(1, int(round(input_tokens * 1.5))),
        }

    # ─────────────────────────────────────────────────────────────────────
    def _map_features(self, df: pd.DataFrame) -> np.ndarray:
        """Return a (7,) float array matching FEATURE_COLS order."""
        row = df.iloc[0]

        def _get(*keys, default=0.0):
            for k in keys:
                if k in row.index:
                    return float(row[k])
            return float(default)

        char_count              = _get("char_count", "text_len", "context_len")
        word_count              = _get("word_count",  "num_words")
        sentence_count          = _get("sentence_count", default=max(1, word_count / 15))
        avg_word_length         = _get("avg_word_length", "avg_word_len",
                                       default=(char_count / max(1, word_count)))
        conversation_turn_count = _get("conversation_turn_count", default=1.0)
        turn_index              = _get("turn_index", default=0.0)
        has_code_block_int      = _get("has_code_block_int", default=0.0)

        return np.array([
            char_count, word_count, sentence_count, avg_word_length,
            conversation_turn_count, turn_index, has_code_block_int,
        ], dtype=float)

