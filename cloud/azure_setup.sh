#!/usr/bin/env bash
# cloud/azure_setup.sh
# =====================
# One-time Azure setup — Azure-only architecture:
#   Resource Group, Storage Account, Blob Container,
#   Azure Container Registry, Databricks Workspace.
# All services run on Azure for Students ($100 credit, no card).
#
# Prerequisites:
#   1. Install Azure CLI: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli
#   2. Activate Azure for Students via GitHub Student Pack
#   3. Run: az login
#
# Usage:
#   bash cloud/azure_setup.sh

set -e

# ── Edit these ─────────────────────────────────────────────────────────────────
SUBSCRIPTION_ID="58251734-8ba7-4aad-93ca-6bd83d703fc0"   # Azure for Students (pulipatinavyasrigmail)
RESOURCE_GROUP="ai-token-optimizer-rg"
LOCATION="centralindia"                   # closest Azure region to India
STORAGE_ACCOUNT="aitokenoptimizer$RANDOM"  # globally unique (3-24 chars, lowercase)
CONTAINER="pipeline-data"
DATABRICKS_WORKSPACE="ai-token-optimizer-databricks"
ACR_NAME="aitokenoptimizeracr"
# ──────────────────────────────────────────────────────────────────────────────

# Pin to the correct subscription (avoids stale CLI cache issues)
echo "=== Setting active subscription ==="
az account set --subscription "$SUBSCRIPTION_ID"

echo "=== Azure for Students — One-Time Setup (Azure-Only) ==="
echo "   Resource Group  : $RESOURCE_GROUP"
echo "   Location        : $LOCATION"
echo "   Storage Account : $STORAGE_ACCOUNT"
echo "   Databricks      : $DATABRICKS_WORKSPACE"
echo ""

# 1. Create Resource Group
echo "[1/6] Creating resource group..."
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --output none

# 2. Create Storage Account
echo "[2/6] Creating storage account: $STORAGE_ACCOUNT"
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --output none

# 3. Get Storage Key
echo "[3/6] Retrieving storage key..."
STORAGE_KEY=$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[0].value" \
  --output tsv)

# 4. Create Blob container
echo "[4/6] Creating blob container: $CONTAINER"
az storage container create \
  --name "$CONTAINER" \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$STORAGE_KEY" \
  --output none

# 5. Create Azure Container Registry (for FastAPI Docker image)
echo "[5/6] Creating Azure Container Registry: $ACR_NAME"
az acr create \
  --name "$ACR_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --sku Basic \
  --admin-enabled true \
  --output none

ACR_PASSWORD=$(az acr credential show \
  --name "$ACR_NAME" \
  --query "passwords[0].value" \
  --output tsv)

# 6. Create Databricks workspace
echo "[6/6] Creating Azure Databricks workspace: $DATABRICKS_WORKSPACE"
az provider register --namespace Microsoft.Databricks --wait --output none 2>/dev/null || true
az databricks workspace create \
  --name "$DATABRICKS_WORKSPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku premium \
  --output none

DATABRICKS_URL=$(az databricks workspace show \
  --name "$DATABRICKS_WORKSPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --query "workspaceUrl" \
  --output tsv)

# 7. Write .env file
echo ""
echo "=== Writing .env file ==="
cat > .env << EOF
# Azure Storage
AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT
AZURE_STORAGE_KEY=$STORAGE_KEY
AZURE_CONTAINER=$CONTAINER
AZURE_RESOURCE_GROUP=$RESOURCE_GROUP
AZURE_LOCATION=$LOCATION

# Azure Container Registry
AZURE_ACR_NAME=$ACR_NAME
AZURE_ACR_PASSWORD=$ACR_PASSWORD

# Azure Container Apps (FastAPI)
AZURE_CONTAINER_APP=token-optimizer-api
AZURE_CONTAINER_ENV=token-optimizer-env
API_URL=http://127.0.0.1:8000

# Azure Databricks (Spark pipeline)
DATABRICKS_WORKSPACE_URL=https://$DATABRICKS_URL

# Elasticsearch + Kibana on Azure Container Instances
# (fill in after: bash cloud/aci_elk_setup.sh)
ES_HOST=http://localhost:9200
KIBANA_URL=http://localhost:5601
EOF

echo ""
echo "=== Setup complete! ==="
echo ""
echo "  Storage Account : $STORAGE_ACCOUNT"
echo "  Container       : $CONTAINER"
echo "  ACR             : $ACR_NAME.azurecr.io"
echo "  Databricks      : https://$DATABRICKS_URL"
echo ""
echo ".env written — source it before running other scripts."
echo ""
echo "Next steps (in order):"
echo "  1. source .env"
echo "  2. bash cloud/upload_data.sh            # upload raw.jsonl to Blob"
echo "  3. bash cloud/databricks_setup.sh       # run Spark pipeline on Databricks"
echo "  4. bash cloud/aci_elk_setup.sh          # deploy ES + Kibana on ACI"
echo "  5. bash cloud/deploy_azure.sh           # deploy FastAPI to Container Apps"
