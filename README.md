# AI Token Optimizer

AI Token Optimizer is a cloud-deployed system for predicting LLM token usage, estimating request cost, and optimizing prompts to reduce token consumption. The project uses a Streamlit frontend, a FastAPI backend, Spark/Databricks for preprocessing and training, Azure Blob Storage for datasets and model artifacts, and Kibana/Elasticsearch for real monitoring dashboards.

## Live deployment

- **Frontend:** https://ai-token-optimizer.streamlit.app/
- **Backend API:** https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io
- **API docs:** https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io/docs
- **Kibana:** http://ai-token-optimizer-elk.centralindia.azurecontainer.io:5601

## Features

- Predicts input, output, and total token usage for a prompt.
- Estimates request cost using token pricing assumptions.
- Optimizes prompts to reduce unnecessary wording.
- Runs Spark-based preprocessing and model training on Azure Databricks.
- Stores raw data, processed data, and trained Spark model artifacts in Azure Blob Storage.
- Loads trained model artifacts from Azure Blob at API runtime.
- Tracks real API inference, training, data-processing, and cost-analysis metrics in Elasticsearch for Kibana dashboards.

## Architecture

```text
Streamlit Cloud frontend
        |
        | HTTPS
        v
Azure Container Apps FastAPI backend
        |
        | downloads model artifacts
        v
Azure Blob Storage
        |
        | stores raw data, processed parquet, model outputs
        v
Azure Databricks Spark pipeline

FastAPI + Databricks
        |
        | real logs and metrics
        v
Elasticsearch + Kibana on Azure Container Instances
```

## Cloud resources

| Component | Service | Purpose |
|---|---|---|
| Frontend | Streamlit Cloud | User interface for prompt analysis |
| Backend | Azure Container Apps | FastAPI prediction and optimization service |
| Training pipeline | Azure Databricks | Spark preprocessing, training, and cost analysis |
| Storage | Azure Blob Storage | Raw data, processed parquet, reports, model artifacts |
| Monitoring | Elasticsearch + Kibana on Azure Container Instances | Real logs, inference metrics, model metrics, pipeline statistics |
| Container registry | Azure Container Registry | Stores the backend Docker image |

## Azure storage layout

Primary storage is Azure Blob Storage.

Storage account used during deployment:

```text
aitokenoptimizer19474
```

Container:

```text
pipeline-data
```

Important blob paths:

```text
data/raw.jsonl
output/processed/
output/sample/
output/stats_spark/
models/
```

The backend API downloads trained model files from:

```text
models/
```

into a temporary runtime cache before serving predictions.

## Monitoring data in Kibana

The project sends real runtime and pipeline data to Elasticsearch.

Kibana data views:

```text
metrics-*
token-optimizer-events-*
token-optimizer-logs-*
```

### `metrics-*`

Contains real model and inference metrics, including:

- `event_type: prediction`
- `event_type: model_training`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `estimated_cost`
- `token_savings_percent`
- `prediction_time_ms`
- `model_name`
- `r2`
- `rmse`
- `mae`
- `dataset_size`

### `token-optimizer-events-*`

Contains real Databricks pipeline events, including:

- `pipeline_run_started`
- `data_processing`
- `training_summary`
- `cost_analysis`
- `pipeline_run_completed`
- `rows_processed`
- `total_conversations`
- `avg_token_count`
- `avg_savings_pct`
- `throughput_rows_per_sec`
- `best_model`
- `best_r2`
- `total_cost_saved_usd`

### `token-optimizer-logs-*`

Contains backend request and health-check logs, including:

- `api_request`
- `api_health_check`
- `endpoint`
- `method`
- `status_code`
- `response_time_ms`

## Local development

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

For the full local pipeline:

```bash
pip install -r requirements.txt
```

For the lightweight API runtime:

```bash
pip install -r requirements-api.txt
```

### 3. Run the API locally

```bash
uvicorn api.main:app --reload
```

By default, the API runs at:

```text
http://127.0.0.1:8000
```

### 4. Run the Streamlit dashboard locally

```bash
streamlit run dashboard/app.py
```

Set the backend URL if needed:

```bash
export API_URL=http://127.0.0.1:8000
```

On Windows PowerShell:

```powershell
$env:API_URL="http://127.0.0.1:8000"
```

## Streamlit Cloud configuration

The deployed Streamlit frontend connects to the Azure backend using the `API_URL` secret.

In Streamlit Cloud → App settings → Secrets, set:

```toml
API_URL = "https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io"
```

## Azure deployment workflow

Run commands from the project root.

### 1. Provision Azure resources

```bash
bash cloud/azure_setup.sh
```

This creates:

- resource group
- storage account
- blob container
- Azure Container Registry
- Azure Databricks workspace
- `.env` with deployment settings

### 2. Load environment variables

```bash
source .env
```

### 3. Upload raw data

```bash
bash cloud/upload_data.sh
```

### 4. Run the Databricks pipeline

For a sampled real-data run:

```bash
bash cloud/databricks_setup.sh --sample
```

For a full run:

```bash
bash cloud/databricks_setup.sh
```

The pipeline writes processed data, reports, and model artifacts to Azure Blob Storage.

### 5. Deploy Elasticsearch and Kibana

```bash
bash cloud/aci_elk_setup.sh
```

Kibana is available at:

```text
http://ai-token-optimizer-elk.centralindia.azurecontainer.io:5601
```

### 6. Deploy the backend API

```bash
bash cloud/deploy_azure.sh
```

The script builds the API Docker image, pushes it to Azure Container Registry, and deploys it to Azure Container Apps.

## API endpoints

### Health check

```bash
curl https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io/health
```

Expected response:

```json
{
  "status": "ok",
  "source": "azure-blob-cache"
}
```

### Prediction

```bash
curl -X POST "https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io/predict?prompt=Explain+transformers"
```

Example response:

```json
{
  "input_tokens": 2,
  "output_tokens": 3,
  "total_tokens": 5,
  "estimated_cost": 0.000009,
  "optimized_prompt": "explain transformers",
  "token_savings_percent": 33.33,
  "compression_percent": 0.0
}
```

## Useful Kibana filters

Use these in Kibana Discover or Lens.

### Real inference metrics

```kql
event_type : "prediction"
```

Data view:

```text
metrics-*
```

### Real model training metrics

```kql
event_type : "model_training"
```

Data view:

```text
metrics-*
```

### Real data processing stats

```kql
event_type : "data_processing"
```

Data view:

```text
token-optimizer-events-*
```

### Real cost analysis

```kql
event_type : "cost_analysis"
```

Data view:

```text
token-optimizer-events-*
```

## Suggested Kibana visualizations

- Average `r2` by `model_name.keyword`
- Average `rmse` by `model_name.keyword`
- Average `prediction_time_ms` over time
- Average `estimated_cost` over time
- Sum of `rows_processed`
- Sum of `total_cost_saved_usd`
- Count of API requests by `endpoint.keyword`

## Project structure

```text
api/                    FastAPI backend
cloud/                  Azure deployment, Databricks, model sync, monitoring utilities
dashboard/              Streamlit frontend
data/                   Dataset loading and local data files
spark/                  Spark preprocessing, training, prediction utilities
ml/                     Additional model experiments
requirements.txt        Full project dependencies
requirements-api.txt    Lightweight API runtime dependencies
docker-compose.yml      Local ELK/support services
```

## Current deployment status

The project has been deployed and verified with:

- Streamlit frontend running on Streamlit Cloud.
- FastAPI backend running on Azure Container Apps.
- Databricks pipeline completed successfully and wrote models to Azure Blob Storage.
- API loaded models from Azure Blob and returned successful predictions.
- Elasticsearch/Kibana received real API inference, model training, data-processing, and cost-analysis metrics.
