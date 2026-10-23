# Azure Startup Guide for 4-Hour Session

## Budget: ₹2,000 INR (~$24 USD)
## Session Duration: 4 Hours

## Prerequisites
- Azure account with ₹2,000 credit
- Azure CLI installed (or use Azure Portal)
- Project files ready locally

## Step 1: Start Azure Container Apps (FastAPI Backend)

### Option A: Using Azure CLI
```bash
# Log in to Azure
az login

# Set your subscription
az account set --subscription "your-subscription-id"

# Navigate to your resource group
cd path/to/ai-token-optimizer

# Build and deploy the container app
az containerapp up \
  --name token-optimizer-api \
  --resource-group token-optimizer-rg \
  --environment token-optimizer-env \
  --source ./api \
  --target-port 8000 \
  --ingress external
```

### Option B: Using Azure Portal
1. Go to Azure Portal → Container Apps
2. Click "Create" → "Container App"
3. Configure:
   - Name: `token-optimizer-api`
   - Resource Group: `token-optimizer-rg`
   - Region: East US (or your preferred region)
   - Container image: Your FastAPI Docker image
   - Environment variables: Add any required API keys
4. Click "Create"

## Step 2: Start Azure Blob Storage

```bash
# Create storage account (if not exists)
az storage account create \
  --name tokenoptimizerstorage \
  --resource-group token-optimizer-rg \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2

# Create container for logs
az storage container create \
  --name token-logs \
  --account-name tokenoptimizerstorage
```

## Step 3: Start Azure Databricks

### Using Azure Portal
1. Go to Azure Portal → Azure Databricks
2. Click "Create" → "Azure Databricks Workspace"
3. Configure:
   - Workspace name: `token-optimizer-databricks`
   - Pricing tier: Standard (for 4-hour session)
   - Region: Same as other resources
4. Click "Create"

### Start Cluster in Databricks
1. Open Databricks workspace
2. Go to "Clusters" → "Create Cluster"
3. Configure:
   - Cluster name: `token-optimizer-cluster`
   - Runtime: Latest LTS
   - Worker type: Standard_DS3_v2 (cost-effective)
   - Workers: 2 (for 4-hour session)
4. Click "Create Cluster"

## Step 4: Start Elasticsearch on Azure

### Option A: Azure Marketplace
1. Go to Azure Portal → Marketplace
2. Search "Elasticsearch"
3. Select "Elastic Cloud" or "Elasticsearch"
4. Configure:
   - Resource name: `token-optimizer-elastic`
   - Region: Same as other resources
   - Pricing: Basic (for 4-hour session)
5. Deploy

### Option B: Docker on Container Apps
```bash
# Deploy Elasticsearch as container app
az containerapp up \
  --name token-optimizer-elastic \
  --resource-group token-optimizer-rg \
  --environment token-optimizer-env \
  --image docker.elastic.co/elasticsearch/elasticsearch:8.11.0 \
  --env-vars ELASTIC_PASSWORD=yourpassword \
  --target-port 9200
```

## Step 5: Generate and Load Sample Data

### Generate Sample Data
```bash
cd scripts
python generate_sample_data.py
```

This creates `sample_data.json` with 500 records over 24 hours.

### Load into Elasticsearch
```bash
# Using Elasticsearch bulk API
curl -X POST "https://your-elastic-endpoint:9200/token-optimizer-logs/_bulk" \
  -H "Content-Type: application/json" \
  -u elastic:yourpassword \
  --data-binary @sample_data.json
```

Or run the script with Elasticsearch URL:
```bash
python generate_sample_data.py
# Enter your Elasticsearch URL when prompted
```

## Step 6: Import Kibana Dashboard

1. Open Kibana (usually at `https://your-elastic-endpoint:5601`)
2. Go to Stack Management → Saved Objects
3. Click "Import"
4. Select `kibana/dashboard.ndjson`
5. Click "Import"
6. Go to Dashboard → "AI Token Optimizer Dashboard"

## Step 7: Deploy Frontend to Vercel

```bash
cd frontend
npm install
npm run build
vercel --prod
```

Or use Vercel dashboard:
1. Connect your GitHub repository
2. Import `frontend` directory
3. Add environment variable: `VITE_API_URL` (your Azure Container Apps URL)
4. Deploy

## Step 8: Test the Full Stack

1. Open your Vercel frontend URL
2. Enter a test prompt
3. Select a model from dropdown
4. Click "Analyze"
5. Verify results appear in dashboard
6. Check Kibana dashboard for data visualization

## Cost Monitoring

### Set Budget Alerts
```bash
# Create budget alert
az consumption budget create \
  --name token-optimizer-budget \
  --resource-group token-optimizer-rg \
  --amount 2000 \
  --currency INR \
  --notification-email your-email@example.com
```

### Monitor Costs in Real-Time
```bash
# Check current costs
az consumption usage list \
  --resource-group token-optimizer-rg \
  --start-date 2024-01-01 \
  --end-date 2024-01-02
```

## Shutdown Procedure (After 4 Hours)

### Stop All Services
```bash
# Stop Databricks cluster
# (Via Databricks UI: Clusters → Select cluster → Terminate)

# Stop Container Apps
az containerapp update \
  --name token-optimizer-api \
  --resource-group token-optimizer-rg \
  --scale-to-zero

# Stop Elasticsearch
# (Via Azure Portal or delete resource)

# Delete temporary resources if not needed
az group delete \
  --name token-optimizer-rg \
  --yes
```

## Estimated 4-Hour Costs

- Container Apps: ₹20-80
- Blob Storage: ₹2-9  
- Databricks: ₹80-160
- Elasticsearch: ₹20-40
- **Total: ₹122-289**

With ₹2,000 budget, you have plenty of margin.

## Troubleshooting

### Container Apps not starting
```bash
az containerapp logs show \
  --name token-optimizer-api \
  --resource-group token-optimizer-rg \
  --follow
```

### Elasticsearch connection issues
- Check security group rules allow port 9200
- Verify username/password credentials
- Check Elasticsearch health: `curl -u elastic:password https://endpoint:9200/_cluster/health`

### Databricks cluster issues
- Check cluster logs in Databricks UI
- Verify Spark MLlib model paths are correct
- Ensure required libraries are installed

## Quick Reference URLs

After deployment, you'll have:
- Frontend: `https://your-app.vercel.app`
- Backend API: `https://token-optimizer-api.azurecontainerapps.io`
- Kibana: `https://your-elastic-endpoint:5601`
- Databricks: `https://your-databricks-instance.azuredatabricks.net`

## Success Indicators

✅ Frontend loads and shows sample data
✅ Model selector works with all 14 models
✅ Cost calculations update based on model selection
✅ Kibana dashboard shows visualizations
✅ Data appears in Elasticsearch
✅ Total cost stays under ₹300 for 4 hours
