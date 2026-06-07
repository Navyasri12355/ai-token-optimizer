from __future__ import annotations

import os
from pathlib import Path

from cloud.azure_config import cfg

DEFAULT_MODEL_CACHE_DIR = os.getenv("MODEL_CACHE_DIR", "./.cache/models")
DEFAULT_MODEL_BLOB_PREFIX = os.getenv("AZURE_MODELS_BLOB_PREFIX", cfg.MODELS_BLOB)


class ModelSyncError(RuntimeError):
    pass


def ensure_models_available(local_dir: str | None = None) -> Path:
    """
    Ensure model artifacts exist locally.

    Resolution order:
    1. Use a pre-populated local directory if a known model exists there.
    2. Download the model tree from Azure Blob Storage into a writable cache dir.
    """
    target_dir = Path(local_dir or DEFAULT_MODEL_CACHE_DIR)

    if _has_supported_model(target_dir):
        return target_dir

    _validate_azure_blob_config()

    downloaded = cfg.download_prefix(
        DEFAULT_MODEL_BLOB_PREFIX, str(target_dir), overwrite=False
    )

    if downloaded == 0 and not _has_supported_model(target_dir):
        raise ModelSyncError(
            f"No model artifacts found in Azure Blob prefix '{DEFAULT_MODEL_BLOB_PREFIX}'."
        )

    if not _has_supported_model(target_dir):
        raise ModelSyncError(
            f"Downloaded blobs into '{target_dir}', but no supported token model was found."
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
    return any((model_root / name).exists() for name in candidates)
