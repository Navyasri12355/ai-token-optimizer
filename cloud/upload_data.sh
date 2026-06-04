#!/usr/bin/env bash
# cloud/upload_data.sh
# =====================
# Upload raw.jsonl to Azure Blob Storage.
# Run after azure_setup.sh and after converting raw.json → raw.jsonl.
#
# Usage:
#   source .env
#   bash cloud/upload_data.sh

set -e

if [ -z "$AZURE_STORAGE_ACCOUNT" ]; then
  echo "ERROR: AZURE_STORAGE_ACCOUNT not set. Run: source .env"
  exit 1
fi

JSONL_PATH="data/raw.jsonl"

if [ ! -f "$JSONL_PATH" ]; then
  echo "raw.jsonl not found. Converting raw.json first..."
  python spark/convert_to_jsonl.py
fi

FILE_SIZE=$(du -sh "$JSONL_PATH" | cut -f1)
echo "Uploading $JSONL_PATH ($FILE_SIZE) to Azure Blob..."
echo "  Account   : $AZURE_STORAGE_ACCOUNT"
echo "  Container : $AZURE_CONTAINER"
echo "  Blob path : data/raw.jsonl"
echo ""
echo "This may take a while on a slow connection..."

az storage blob upload \
  --account-name "$AZURE_STORAGE_ACCOUNT" \
  --account-key  "$AZURE_STORAGE_KEY" \
  --container-name "$AZURE_CONTAINER" \
  --name "data/raw.jsonl" \
  --file "$JSONL_PATH" \
  --overwrite true

echo ""
echo "Upload complete!"
echo "Blob URL: https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_CONTAINER/data/raw.jsonl"
