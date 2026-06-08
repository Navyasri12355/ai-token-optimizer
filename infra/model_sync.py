from __future__ import annotations

import os
import shutil
from pathlib import Path

from infra.azure_config import cfg

DEFAULT_MODEL_CACHE_DIR = os.getenv("MODEL_CACHE_DIR", "./.cache/models")
DEFAULT_MODEL_BLOB_PREFIX = os.getenv("AZURE_MODELS_BLOB_PREFIX", cfg.MODELS_BLOB)


class ModelSyncError(RuntimeError):
    pass


def ensure_models_available(local_dir: str | None = None) -> Path:
    """
    Ensure model artifacts exist locally.

    Resolution order:
    1. Use a pre-populated local directory if a complete known model exists there.
    2. Download the model tree from Azure Blob Storage into a writable cache dir.
    3. If the cache is partial/corrupt, refresh it once from Blob.
    """
    target_dir = Path(local_dir or DEFAULT_MODEL_CACHE_DIR)

    if _has_supported_model(target_dir):
        return target_dir

    _validate_azure_blob_config()

    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)

    downloaded = cfg.download_prefix(
        DEFAULT_MODEL_BLOB_PREFIX, str(target_dir), overwrite=True
    )

    if downloaded == 0 and not _has_supported_model(target_dir):
        raise ModelSyncError(
            f"No model artifacts found in Azure Blob prefix '{DEFAULT_MODEL_BLOB_PREFIX}'."
        )

    if not _has_supported_model(target_dir):
        raise ModelSyncError(
            f"Downloaded blobs into '{target_dir}', but no complete supported token model was found."
        )

    return target_dir


def _validate_azure_blob_config() -> None:
    missing = []
    if not cfg.STORAGE_ACCOUNT:
        missing.append("AZURE_STORAGE_ACCOUNT")
    if not cfg.STORAGE_KEY:
        missing.append("AZURE_STORAGE_KEY")
    if not cfg.CONTAINER:
        missing.append("AZURE_CONTAINER")

    if missing:
        raise ModelSyncError(
            "Missing Azure Blob configuration for model download: " + ", ".join(missing)
        )


def _has_supported_model(model_root: Path) -> bool:
    candidates = (
        "ridge_opt_token_count",
        "ridge_token_count",
        "cv_ridge_token_count",
    )
    return any(_is_complete_model_dir(model_root / name) for name in candidates)


def _is_complete_model_dir(model_dir: Path) -> bool:
    stages_dir = model_dir / "stages"
    if not stages_dir.exists():
        return False

    scaler_hits = list(stages_dir.glob("1_StandardScaler*/data/*.parquet"))
    lr_hits = list(stages_dir.glob("2_LinearRegression*/data/*.parquet"))
    return bool(scaler_hits and lr_hits)
