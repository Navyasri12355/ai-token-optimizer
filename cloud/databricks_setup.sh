#!/usr/bin/env bash
# cloud/databricks_setup.sh
# ==========================
# Creates Azure Databricks workspace (idempotent), uploads the pipeline
# script via REST API, and submits a one-time Job Cluster run.
#
# Uses curl + Azure AD token directly — no databricks-cli needed.
#
# Prerequisites:
#   - az login done
#   - source .env
#
# Usage:
#   source .env
#   bash cloud/databricks_setup.sh [--sample]

set -e

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-ai-token-optimizer-rg}"
LOCATION="${AZURE_LOCATION:-centralindia}"
WORKSPACE_NAME="ai-token-optimizer-databricks"
PIPELINE_ARGS="${@}"   # e.g. --sample
export PIPELINE_ARGS
PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

echo "=== AI Token Optimizer — Databricks Job Submit ==="

# ── 1. Register provider (idempotent) ─────────────────────────────────────────
az provider register --namespace Microsoft.Databricks --wait --output none 2>/dev/null || true

# ── 2. Create / confirm workspace ─────────────────────────────────────────────
echo "[1/4] Databricks workspace: $WORKSPACE_NAME"
if az databricks workspace show --name "$WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "   Already exists — skipping creation."
else
  echo "   Creating workspace (Premium SKU)..."
  az databricks workspace create \
    --name "$WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku premium \
    --output none
fi

WORKSPACE_URL=$(az databricks workspace show \
  --name "$WORKSPACE_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "workspaceUrl" \
  --output tsv)
echo "   URL: https://$WORKSPACE_URL"

# ── 3. Get Azure AD token (REST API auth) ─────────────────────────────────────
echo "[2/4] Getting Azure AD token..."
AAD_TOKEN=$(az account get-access-token \
  --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" \
  --query accessToken \
  --output tsv)

CURRENT_USER=$(az account show --query user.name --output tsv)
echo "   User: $CURRENT_USER"
WORKSPACE_SCRIPT_PATH="/Users/$CURRENT_USER/run_pipeline_databricks"

# Helper: call Databricks REST API with curl
dbx_api() {
  local method="$1"
  local path="$2"
  curl -s -X "$method" \
    "https://$WORKSPACE_URL/api/$path" \
    -H "Authorization: Bearer $AAD_TOKEN" \
    -H "Content-Type: application/json"
}

dbx_api_file() {
  local method="$1"
  local path="$2"
  local payload_file="$3"
  curl -s -X "$method" \
    "https://$WORKSPACE_URL/api/$path" \
    -H "Authorization: Bearer $AAD_TOKEN" \
    -H "Content-Type: application/json" \
    -d "@$payload_file"
}

# ── 4. Upload pipeline script via DBFS REST API ───────────────────────────────
echo "[3/4] Uploading pipeline script to workspace files..."

UPLOAD_PAYLOAD_FILE=$(mktemp)
"$PYTHON_BIN" - "$CURRENT_USER" "$UPLOAD_PAYLOAD_FILE" <<'PY'
import base64
import json
import sys

current_user = sys.argv[1]
payload_path = sys.argv[2]
workspace_script_path = '/' + 'Users/' + current_user + '/run_pipeline_databricks'
with open('cloud/run_pipeline_databricks.py', 'rb') as f:
    content = base64.b64encode(b'# Databricks notebook source\n' + f.read()).decode()
payload = {
    'path': workspace_script_path,
    'format': 'SOURCE',
    'language': 'PYTHON',
    'overwrite': True,
    'content': content,
}
with open(payload_path, 'w', encoding='utf-8') as f:
    json.dump(payload, f)
PY

UPLOAD_RESP=$(dbx_api_file POST "2.0/workspace/import" "$UPLOAD_PAYLOAD_FILE")
rm -f "$UPLOAD_PAYLOAD_FILE"

# Check for error in response
UPLOAD_ERR=$(echo "$UPLOAD_RESP" | "$PYTHON_BIN" -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    print(d.get('error_code', '') + ': ' + d.get('message', ''))
except:
    print('')
" 2>/dev/null)

if [ -n "$UPLOAD_ERR" ] && [ "$UPLOAD_ERR" != ": " ]; then
  echo "   ERROR uploading script: $UPLOAD_ERR"
  exit 1
fi
echo "   Uploaded → $WORKSPACE_SCRIPT_PATH"

# ── 5. Submit one-time run with a Job Cluster ──────────────────────────────────
echo "[4/4] Submitting pipeline job (Job Cluster)..."

RUN_PAYLOAD_FILE=$(mktemp)
"$PYTHON_BIN" - "$CURRENT_USER" "$RUN_PAYLOAD_FILE" <<'PY'
import json
import os
import sys

current_user = sys.argv[1]
payload_path = sys.argv[2]
workspace_script_path = '/' + 'Users/' + current_user + '/run_pipeline_databricks'
es_host = os.environ.get('ES_HOST', 'http://ai-token-optimizer-elk.centralindia.azurecontainer.io:9200')
if es_host in ('localhost', 'http://localhost:9200', 'https://localhost:9200'):
    es_host = 'http://ai-token-optimizer-elk.centralindia.azurecontainer.io:9200'
payload = {
  'run_name': 'token-optimizer-pipeline',
  'new_cluster': {
    'spark_version': '14.3.x-scala2.12',
    'node_type_id': 'Standard_D4s_v3',
    'num_workers': 0,
    'data_security_mode': 'SINGLE_USER',
    'single_user_name': current_user,
    'spark_conf': {
      'spark.master': 'local[*, 4]',
      'spark.databricks.cluster.profile': 'singleNode',
      'spark.sql.shuffle.partitions': '8',
      'spark.sql.adaptive.enabled': 'true',
      'spark.driver.maxResultSize': '4g'
    },
    'custom_tags': {'ResourceClass': 'SingleNode'},
    'spark_env_vars': {
      'AZURE_STORAGE_ACCOUNT': os.environ.get('AZURE_STORAGE_ACCOUNT', ''),
      'AZURE_STORAGE_KEY':     os.environ.get('AZURE_STORAGE_KEY', ''),
      'AZURE_CONTAINER':       os.environ.get('AZURE_CONTAINER', ''),
      'ES_HOST':               es_host,
      'ES_PORT':               os.environ.get('ES_PORT', '9200'),
      'SERVICE_NAME':          'ai-token-optimizer-databricks'
    }
  },
  'notebook_task': {
    'notebook_path': workspace_script_path
  }
}
with open(payload_path, 'w', encoding='utf-8') as f:
    json.dump(payload, f)
PY

RUN_RAW=$(dbx_api_file POST "2.1/jobs/runs/submit" "$RUN_PAYLOAD_FILE")
rm -f "$RUN_PAYLOAD_FILE"

RUN_ID=$(echo "$RUN_RAW" | "$PYTHON_BIN" -c "
import sys, json
raw = sys.stdin.read().strip()
try:
    d = json.loads(raw)
    if 'run_id' in d:
        print(d['run_id'])
    else:
        print('ERROR:', d.get('error_code',''), d.get('message',''), file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print('Parse error:', e, file=sys.stderr)
    print(raw, file=sys.stderr)
    sys.exit(1)
")

echo ""
echo "=== Job submitted! ==="
echo "   Run ID    : $RUN_ID"
echo "   Monitor   : https://$WORKSPACE_URL/jobs/runs/$RUN_ID"
echo ""
echo "Watching run status (Ctrl+C to stop watching, job continues in cloud)..."
echo ""

# ── 6. Poll run status ─────────────────────────────────────────────────────────
for i in $(seq 1 120); do
  RUN_STATE_RAW=$(dbx_api GET "2.1/jobs/runs/get?run_id=$RUN_ID")

  LIFE=$(echo "$RUN_STATE_RAW" | "$PYTHON_BIN" -c "
import sys, json
try:
    print(json.loads(sys.stdin.read())['state']['life_cycle_state'])
except:
    print('UNKNOWN')
")
  RESULT=$(echo "$RUN_STATE_RAW" | "$PYTHON_BIN" -c "
import sys, json
try:
    print(json.loads(sys.stdin.read())['state'].get('result_state', ''))
except:
    print('')
")

  printf "   [%4ds] %-16s %s\n" "$((i * 15))" "$LIFE" "$RESULT"

  if [ "$LIFE" = "TERMINATED" ]; then
    echo ""
    if [ "$RESULT" = "SUCCESS" ]; then
      echo "✅ Pipeline completed successfully!"
      echo ""
      echo "Outputs:"
      echo "  wasbs://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/output/"
      echo "  wasbs://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/models/"
    else
      MSG=$(echo "$RUN_STATE_RAW" | "$PYTHON_BIN" -c "
import sys, json
try:
    print(json.loads(sys.stdin.read())['state'].get('state_message','No message'))
except:
    print('unknown')
")
      echo "❌ Pipeline failed: $MSG"
      echo "   Logs: https://$WORKSPACE_URL/#job/run/$RUN_ID"
      exit 1
    fi
    break
  fi

  if [ "$LIFE" = "INTERNAL_ERROR" ]; then
    ERR_MSG=$(dbx_api GET "2.1/jobs/runs/get?run_id=$RUN_ID" | "$PYTHON_BIN" -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    s = d.get('state', {})
    print(s.get('state_message', 'No message available'))
except:
    print('unknown')
")
    echo "❌ Internal error: $ERR_MSG"
    echo "   Logs: https://$WORKSPACE_URL/jobs/runs/$RUN_ID"
    exit 1
  fi

  sleep 15
done

echo ""
echo "Save to .env:"
echo "  DATABRICKS_WORKSPACE_URL=https://$WORKSPACE_URL"
echo "  DATABRICKS_RUN_ID=$RUN_ID"
