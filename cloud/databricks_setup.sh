#!/usr/bin/env bash
# cloud/databricks_setup.sh
# ==========================
# Creates Azure Databricks workspace, configures a cluster,
# uploads the pipeline script, and submits it as a job.
#
# Prerequisites:
#   - az login done
#   - source .env
#   - pip install databricks-cli
#
# Usage:
#   source .env
#   bash cloud/databricks_setup.sh [--sample]

set -e

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-ai-token-optimizer-rg}"
LOCATION="${AZURE_LOCATION:-centralindia}"
WORKSPACE_NAME="ai-token-optimizer-databricks"
PIPELINE_ARGS="${@}"   # e.g. --sample

echo "=== Creating Azure Databricks Workspace ==="

# 1. Register Databricks provider (once)
az provider register --namespace Microsoft.Databricks --wait --output none 2>/dev/null || true

# 2. Create Databricks workspace (Premium tier — Standard SKU is deprecated)
echo "[1/6] Creating Databricks workspace: $WORKSPACE_NAME"
az databricks workspace create \
  --name "$WORKSPACE_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku premium \
  --output none

WORKSPACE_URL=$(az databricks workspace show \
  --name "$WORKSPACE_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "workspaceUrl" \
  --output tsv)

echo "   Workspace URL: https://$WORKSPACE_URL"

# 3. Get Azure AD token for Databricks (no PAT needed)
echo "[2/6] Generating Azure AD token..."
AAD_TOKEN=$(az account get-access-token \
  --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" \
  --query accessToken \
  --output tsv)

# 4. Configure Databricks CLI
echo "[3/6] Configuring Databricks CLI..."
pip install databricks-cli --quiet
export DATABRICKS_HOST="https://$WORKSPACE_URL"
export DATABRICKS_TOKEN="$AAD_TOKEN"

# 5. Create cluster spec and start cluster
echo "[4/6] Creating Databricks cluster..."
CLUSTER_ID=$(databricks clusters create --json '{
  "cluster_name": "token-optimizer-cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 0,
  "spark_conf": {
    "spark.master": "local[*, 4]",
    "spark.databricks.cluster.profile": "singleNode",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.maxResultSize": "4g"
  },
  "custom_tags": {"ResourceClass": "SingleNode"},
  "spark_env_vars": {
    "AZURE_STORAGE_ACCOUNT": "'"$AZURE_STORAGE_ACCOUNT"'",
    "AZURE_STORAGE_KEY": "'"$AZURE_STORAGE_KEY"'",
    "AZURE_CONTAINER": "'"$AZURE_CONTAINER"'"
  },
  "autotermination_minutes": 60
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['cluster_id'])")

echo "   Cluster ID: $CLUSTER_ID"
echo "   Starting cluster (takes ~3 min)..."

# Wait for cluster to start
for i in $(seq 1 30); do
  STATE=$(databricks clusters get --cluster-id "$CLUSTER_ID" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  echo "   State: $STATE"
  if [ "$STATE" = "RUNNING" ]; then break; fi
  sleep 15
done

# 6. Upload pipeline script and submit job
echo "[5/6] Uploading pipeline script..."
databricks fs mkdirs dbfs:/pipeline
databricks fs cp cloud/run_pipeline_databricks.py dbfs:/pipeline/run_pipeline_databricks.py

echo "[6/6] Submitting pipeline job..."
RUN_ID=$(databricks runs submit --json '{
  "run_name": "token-optimizer-pipeline",
  "existing_cluster_id": "'"$CLUSTER_ID"'",
  "spark_python_task": {
    "python_file": "dbfs:/pipeline/run_pipeline_databricks.py",
    "parameters": ["'"$PIPELINE_ARGS"'"]
  }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")

echo ""
echo "=== Job submitted! ==="
echo "   Run ID    : $RUN_ID"
echo "   Workspace : https://$WORKSPACE_URL"
echo ""
echo "Monitor:"
echo "  databricks runs get --run-id $RUN_ID"
echo "  Or: https://$WORKSPACE_URL/#job/$RUN_ID"
echo ""
echo "Outputs when done:"
echo "  wasbs://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/output/"
echo "  wasbs://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/models/"
echo ""
echo "Save these to .env:"
echo "  DATABRICKS_WORKSPACE_URL=https://$WORKSPACE_URL"
echo "  DATABRICKS_CLUSTER_ID=$CLUSTER_ID"
