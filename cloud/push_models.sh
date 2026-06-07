#!/usr/bin/env bash
# cloud/push_models.sh
# ====================
# Upload locally exported model artifacts to Azure Blob Storage.
#
# Usage:
#   source .env
#   bash cloud/push_models.sh [local_model_dir]

set -e

if [ -z "$AZURE_STORAGE_ACCOUNT" ] || [ -z "$AZURE_STORAGE_KEY" ] || [ -z "$AZURE_CONTAINER" ]; then
  echo "ERROR: Azure storage settings are missing. Run: source .env"
  exit 1
fi

MODEL_DIR="${1:-spark/models}"
BLOB_PREFIX="${AZURE_MODELS_BLOB_PREFIX:-models}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

echo "=== Pushing model artifacts to Azure Blob ==="
echo "   Local dir   : $MODEL_DIR"
echo "   Blob prefix : $BLOB_PREFIX"
echo ""

"$PYTHON_BIN" cloud/push_models.py --source "$MODEL_DIR" --blob-prefix "$BLOB_PREFIX"
