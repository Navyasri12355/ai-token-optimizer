from __future__ import annotations

import argparse
from pathlib import Path

from cloud.azure_config import cfg


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload locally exported model artifacts to Azure Blob Storage."
    )
    parser.add_argument(
        "--source",
        default="spark/models",
        help="Local model directory to upload (default: spark/models)",
    )
    parser.add_argument(
        "--blob-prefix",
        default=cfg.MODELS_BLOB,
        help="Azure Blob prefix to upload into (default: models)",
    )
    args = parser.parse_args()

    source_dir = Path(args.source)
    if not source_dir.exists():
        raise FileNotFoundError(f"Model source directory not found: {source_dir}")

    file_count = sum(1 for p in source_dir.rglob("*") if p.is_file())
    if file_count == 0:
        raise RuntimeError(f"No model files found under: {source_dir}")

    uploaded = cfg.upload_directory(str(source_dir), args.blob_prefix, overwrite=True)

    print("=== Model upload complete ===")
    print(f"Local source : {source_dir}")
    print(f"Blob prefix  : {args.blob_prefix}")
    print(f"Files pushed : {uploaded}")
    print(
        f"Blob URL     : https://{cfg.STORAGE_ACCOUNT}.blob.core.windows.net/"
        f"{cfg.CONTAINER}/{args.blob_prefix}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
