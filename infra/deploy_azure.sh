#!/usr/bin/env bash
# cloud/deploy_azure.sh
# =====================
# Build the API Docker image → push to Azure Container Registry
# → deploy to Azure Container Apps (free tier).
#
# Uses the slim API image (`cloud/Dockerfile.api`) because inference in this
# repo is Spark-free at runtime and only needs the exported model artifacts.
#
# Azure Container Apps free tier:
#   180,000 vCPU-seconds + 360,000 GB-seconds per month — plenty for a student project.
#
# Prerequisites:
#   - az login done
#   - source .env
#   - model artifacts uploaded to Azure Blob under the `models/` prefix
#   - Docker Desktop running locally OR ACR build permissions for remote build
#
# Usage:
#   source .env
#   bash cloud/deploy_azure.sh

set -e

if [ -z "$AZURE_STORAGE_ACCOUNT" ]; then
  echo "ERROR: .env not loaded. Run: source .env"
  exit 1
fi

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-ai-token-optimizer-rg}"
LOCATION="${AZURE_LOCATION:-centralindia}"
ACR_NAME="${AZURE_ACR_NAME}"
APP_NAME="${AZURE_CONTAINER_APP:-token-optimizer-api}"
ENV_NAME="${AZURE_CONTAINER_ENV:-token-optimizer-env}"
IMAGE_REPO="token-optimizer-api"
IMAGE_TAG="${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}"
IMAGE="$ACR_NAME.azurecr.io/$IMAGE_REPO:$IMAGE_TAG"
LATEST_IMAGE="$ACR_NAME.azurecr.io/$IMAGE_REPO:latest"
DOCKERFILE="cloud/Dockerfile.api"

echo "=== Deploying FastAPI to Azure Container Apps ==="
echo "   ACR        : $ACR_NAME.azurecr.io"
echo "   App        : $APP_NAME"
echo "   Env        : $ENV_NAME"
echo "   Location   : $LOCATION"
echo "   Dockerfile : $DOCKERFILE"
echo "   Image tag  : $IMAGE_TAG"
echo ""

# 1-3. Build and publish Docker image
echo "[1/5] Publishing image to ACR..."
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  echo "   Using local Docker daemon."
  az acr login --name "$ACR_NAME"
  docker build -f "$DOCKERFILE" -t "$IMAGE" -t "$LATEST_IMAGE" .
  docker push "$IMAGE"
  docker push "$LATEST_IMAGE"
else
  echo "   Local Docker daemon unavailable; using Azure ACR remote build."
  az acr build \
    --registry "$ACR_NAME" \
    --image "$IMAGE_REPO:$IMAGE_TAG" \
    --file "$DOCKERFILE" \
    .
fi

# 4. Create Container Apps Environment (if not exists)
echo "[4/5] Creating Container Apps environment..."
az containerapp env create \
  --name "$ENV_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --output none 2>/dev/null || echo "   Environment already exists"

# 5. Deploy or update the Container App
echo "[5/5] Deploying Container App..."
ENV_VARS=(
  "AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT"
  "AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY"
  "AZURE_CONTAINER=$AZURE_CONTAINER"
  "AZURE_RESOURCE_GROUP=$RESOURCE_GROUP"
  "AZURE_LOCATION=$LOCATION"
  "AZURE_ACR_NAME=$ACR_NAME"
  "AZURE_CONTAINER_APP=$APP_NAME"
  "AZURE_CONTAINER_ENV=$ENV_NAME"
  "ES_HOST=http://ai-token-optimizer-elk.centralindia.azurecontainer.io:9200"
  "ES_PORT=9200"
  "KIBANA_URL=http://ai-token-optimizer-elk.centralindia.azurecontainer.io:5601"
  "SERVICE_NAME=ai-token-optimizer-api"
  "AZURE_MODELS_BLOB_PREFIX=models"
  "MODEL_CACHE_DIR=/tmp/ai-token-optimizer-models"
)

if az containerapp show --name "$APP_NAME" --resource-group "$RESOURCE_GROUP" >/dev/null 2>&1; then
  az containerapp update \
    --name "$APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --image "$IMAGE" \
    --set-env-vars "${ENV_VARS[@]}" \
    --output none
else
  az containerapp create \
    --name "$APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --environment "$ENV_NAME" \
    --image "$IMAGE" \
    --registry-server "$ACR_NAME.azurecr.io" \
    --registry-username "$ACR_NAME" \
    --registry-password "$AZURE_ACR_PASSWORD" \
    --target-port 8080 \
    --ingress external \
    --min-replicas 0 \
    --max-replicas 3 \
    --cpu 0.5 \
    --memory 1.0Gi \
    --env-vars "${ENV_VARS[@]}" \
    --output none
fi

# Get deployed URL
APP_URL=$(az containerapp show \
  --name "$APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.configuration.ingress.fqdn" \
  --output tsv)

echo ""
echo "=== Deployment complete! ==="
echo "   API URL  : https://$APP_URL"
echo "   Docs     : https://$APP_URL/docs"
echo "   Health   : https://$APP_URL/health"
echo ""
echo "Test:"
echo "  curl https://$APP_URL/health"
echo "  curl \"https://$APP_URL/predict?prompt=Explain+transformers\""
echo ""
echo "Save this URL for the dashboard:"
echo "  API_URL=https://$APP_URL"
