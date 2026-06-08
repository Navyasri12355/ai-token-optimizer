#!/usr/bin/env bash
# cloud/aci_elk_setup.sh
# =======================
# Deploy Elasticsearch + Kibana to Azure Container Instances.
# Replaces the Oracle Always Free VM for ELK.
#
# Cost: ~$0.10/hr from Azure credits (~$2.40/day if running 24/7)
# TIP:  Stop the container group when not using Kibana to save credits:
#         az container stop  --name elk-stack --resource-group <rg>
#         az container start --name elk-stack --resource-group <rg>
#
# Prerequisites:
#   source .env
#
# Usage:
#   bash cloud/aci_elk_setup.sh        # deploy
#   bash cloud/aci_elk_setup.sh stop   # stop to save credits
#   bash cloud/aci_elk_setup.sh start  # restart
#   bash cloud/aci_elk_setup.sh delete # delete (stops billing entirely)

set -e

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-ai-token-optimizer-rg}"
LOCATION="${AZURE_LOCATION:-centralindia}"
CONTAINER_GROUP="elk-stack"
ACTION="${1:-deploy}"

case "$ACTION" in
  stop)
    echo "Stopping ELK stack (no billing while stopped)..."
    az container stop --name "$CONTAINER_GROUP" --resource-group "$RESOURCE_GROUP"
    echo "Stopped. Restart with: bash cloud/aci_elk_setup.sh start"
    exit 0
    ;;
  start)
    echo "Starting ELK stack..."
    az container start --name "$CONTAINER_GROUP" --resource-group "$RESOURCE_GROUP"
    IP=$(az container show \
      --name "$CONTAINER_GROUP" \
      --resource-group "$RESOURCE_GROUP" \
      --query "ipAddress.ip" --output tsv)
    echo "Kibana: http://$IP:5601"
    exit 0
    ;;
  delete)
    echo "Deleting ELK stack (all billing stops)..."
    az container delete --name "$CONTAINER_GROUP" --resource-group "$RESOURCE_GROUP" --yes
    echo "Deleted."
    exit 0
    ;;
esac

# ── Deploy ─────────────────────────────────────────────────────────────────────
echo "=== Deploying ELK Stack to Azure Container Instances ==="
echo "   Resource Group : $RESOURCE_GROUP"
echo "   Location       : $LOCATION"
echo "   Cost           : ~\$0.10/hr (~\$2.40/day) from Azure credits"
echo ""

# Patch the YAML with actual resource group / location
sed "s/location: centralindia/location: $LOCATION/" cloud/aci_elk.yaml > /tmp/aci_elk_patched.yaml

az container create \
  --resource-group "$RESOURCE_GROUP" \
  --file /tmp/aci_elk_patched.yaml \
  --output none

echo "Waiting for containers to start (~60s)..."
sleep 60

# Get public IP
IP=$(az container show \
  --name "$CONTAINER_GROUP" \
  --resource-group "$RESOURCE_GROUP" \
  --query "ipAddress.ip" \
  --output tsv)

DNS="ai-token-optimizer-elk.$LOCATION.azurecontainer.io"

echo ""
echo "=== ELK Stack deployed! ==="
echo "   Elasticsearch : http://$IP:9200"
echo "   Kibana        : http://$IP:5601"
echo "   DNS (alt)     : http://$DNS:5601"
echo ""
echo "Wait ~2 min for ES to fully start, then check:"
echo "  curl http://$IP:9200/_cluster/health"
echo ""
echo "Update your .env:"
echo "  ES_HOST=http://$IP:9200"
echo "  KIBANA_URL=http://$IP:5601"
echo ""
echo "Stop when not using (saves credits):"
echo "  bash cloud/aci_elk_setup.sh stop"
