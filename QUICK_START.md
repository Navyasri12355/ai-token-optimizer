# Quick Start Guide - 4 Hour Azure Session
## Budget: ₹2,000 INR | Using Existing infra/ Scripts

## Prerequisites
- Azure CLI installed: `az login`
- Project files in current directory
- ₹2,000 Azure credit available

## Step 1: One-Time Azure Setup (First time only)

```bash
cd infra
bash azure_setup.sh
```

This creates:
- Resource Group: `ai-token-optimizer-rg`
- Storage Account: `aitokenoptimizer$RANDOM`
- Azure Container Registry: `aitokenoptimizeracr`
- Databricks Workspace: `ai-token-optimizer-databricks`

**Output:** Creates `.env` file with all credentials

```bash
source .env
```

## Step 2: Deploy Elasticsearch + Kibana

```bash
bash aci_elk_setup.sh
```

**Cost:** ~₹8/hr (~₹32 for 4 hours)

**Output:** 
- Elasticsearch: `http://IP:9200`
- Kibana: `http://IP:5601`

**Update .env:**
```bash
ES_HOST=http://IP:9200
KIBANA_URL=http://IP:5601
```

## Step 3: Deploy FastAPI Backend

```bash
bash deploy_azure.sh
```

**Cost:** Free tier (180,000 vCPU-seconds/month)

**Output:**
- API URL: `https://token-optimizer-api.azurecontainerapps.io`
- Docs: `https://token-optimizer-api.azurecontainerapps.io/docs`

## Step 4: Generate Sample Data

```bash
cd ../scripts
python generate_sample_data.py
```

**Output:** `sample_data.json` with 500 records over 24 hours

## Step 5: Ingest Data into Elasticsearch

```bash
python ingest_to_elasticsearch.py
```

**When prompted:**
- Elasticsearch URL: `http://IP:9200` (from Step 2)
- Index name: `token-optimizer-logs`
- Authentication: `n` (unless you set up auth)

## Step 6: Import Kibana Dashboard

1. Open Kibana: `http://IP:5601`
2. Go to **Stack Management** → **Saved Objects**
3. Click **Import**
4. Select: `../kibana/dashboard.ndjson`
5. Click **Import**

## Step 7: Deploy Frontend

```bash
cd ../frontend
npm install
npm run build
vercel --prod
```

**Add environment variable in Vercel:**
- `VITE_API_URL`: Your FastAPI URL from Step 3

## Step 8: Start Databricks (Optional - for ML pipeline)

```bash
cd ../infra
bash databricks_setup.sh
```

**Cost:** ~₹320-640/hr (only if needed for model training)

**Skip this step** if you just want to demonstrate the dashboard with sample data.

## Verification Checklist

✅ Azure services running
```bash
az container list --resource-group ai-token-optimizer-rg
az storage account list --resource-group ai-token-optimizer-rg
```

✅ Elasticsearch healthy
```bash
curl http://IP:9200/_cluster/health
```

✅ FastAPI running
```bash
curl https://token-optimizer-api.azurecontainerapps.io/health
```

✅ Data in Elasticsearch
```bash
curl http://IP:9200/token-optimizer-logs/_count
```

✅ Kibana dashboard visible
- Open Kibana → Dashboard → "AI Token Optimizer Dashboard"

## Cost Monitoring

```bash
# Check current costs
az consumption usage list \
  --resource-group ai-token-optimizer-rg \
  --start-date $(date -d '4 hours ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d)
```

## Shutdown Procedure (After 4 Hours)

```bash
cd infra

# Stop ELK stack (saves money)
bash aci_elk_setup.sh stop

# Scale Container Apps to zero
az containerapp update \
  --name token-optimizer-api \
  --resource-group ai-token-optimizer-rg \
  --min-replicas 0

# Stop Databricks cluster (via Databricks UI)
# Or delete entire resource group:
az group delete --name ai-token-optimizer-rg --yes
```

## Expected 4-Hour Costs

- Azure Container Apps: **Free**
- Blob Storage: **₹2-9**
- Elasticsearch (ACI): **₹32**
- Databricks (if used): **₹320-640**

**Total without Databricks: ₹34-41**
**Total with Databricks: ₹354-681**

## Troubleshooting

### Azure setup fails
```bash
# Check subscription
az account list
az account set --subscription "your-subscription-id"
```

### ELK stack won't start
```bash
# Check container logs
az container logs show --name elk-stack --resource-group ai-token-optimizer-rg
```

### FastAPI deployment fails
```bash
# Check Container Apps logs
az containerapp logs show \
  --name token-optimizer-api \
  --resource-group ai-token-optimizer-rg \
  --follow
```

### Data ingestion fails
```bash
# Verify Elasticsearch is accessible
curl http://IP:9200
curl http://IP:9200/_cluster/health
```

## Quick Reference

**After deployment:**
- Frontend: `https://your-app.vercel.app`
- Backend: `https://token-optimizer-api.azurecontainerapps.io`
- Kibana: `http://IP:5601`
- Databricks: `https://ai-token-optimizer-databricks.azuredatabricks.net`

**Key scripts:**
- `infra/azure_setup.sh` - Initial Azure setup
- `infra/aci_elk_setup.sh` - Deploy/stop ELK
- `infra/deploy_azure.sh` - Deploy FastAPI
- `infra/databricks_setup.sh` - Setup Databricks
- `scripts/generate_sample_data.py` - Generate demo data
- `scripts/ingest_to_elasticsearch.py` - Load data into ES

## Success Indicators

✅ All Azure services running
✅ Kibana dashboard shows 500 records
✅ Frontend connects to backend
✅ Model selector works with 14 models
✅ Costs stay under ₹100 (without Databricks)
