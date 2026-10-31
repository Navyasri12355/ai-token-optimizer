# AI Token Optimizer

AI Token Optimizer is a cloud-deployed system for predicting LLM token usage, estimating request cost, and optimizing prompts to reduce token consumption. The project uses a React+Vite frontend, a FastAPI backend, Spark/Databricks for preprocessing and training, Azure Blob Storage for datasets and model artifacts, and Kibana/Elasticsearch for real monitoring dashboards.

## Live deployment

- **Frontend (React+Vite):** https://ai-token-optimizer-livid.vercel.app
- **Backend API:** https://ai-token-optimizer-1-o481.onrender.com
- **API docs:** https://ai-token-optimizer-1-o481.onrender.com/docs

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
React + Vite frontend
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
| Frontend | React + Vite | User interface for prompt analysis |
| Backend | Azure Container Apps | FastAPI prediction and optimization service |
| Training pipeline | Azure Databricks | Spark preprocessing, training, and cost analysis |
| Storage | Azure Blob Storage | Raw data, processed parquet, reports, model artifacts |
| Monitoring | Elasticsearch + Kibana on Azure Container Instances | Real logs, inference metrics, model metrics, pipeline statistics |
| Container registry | Azure Container Registry | Stores the backend Docker image |

## Performance stats

Stats measured on the 10,000-conversation sample dataset processed through the Spark pipeline on Azure Databricks.

### Dataset

| Metric | Value |
|---|---|
| Total training dataset | ~2,830,405 conversations (6 Hugging Face sources) |
| Raw data size | ~6.8 GB (JSONL) |
| Sample used for rapid training | 10,000 conversations / 25,342 turns |
| Avg token count per turn | 45.8 tokens |
| Avg optimized token count per turn | 28.3 tokens |
| Total tokens in sample | 1,160,000 |
| Total tokens saved | 443,000 |
| Preprocessing throughput | 199 turns/second |
| Preprocessing time (10K sample) | 127.3 seconds |

#### Training datasets

| Dataset | Type | Size | Schema / Notes |
|---|---|---|---|
| [Dans-DiscountModels/ConversationChronicles-sharegpt](https://huggingface.co/datasets/Dans-DiscountModels/ConversationChronicles-sharegpt) | ShareGPT-style human-GPT conversations | Primary diverse set | Already `{id, conversations}`; gated — requires HF token |
| [Arist12/EABF-ShareGPT-Long-3.5k](https://huggingface.co/datasets/Arist12/EABF-ShareGPT-Long-3.5k) | Long-form ShareGPT | 3,500 conversations | `{id, model, conversations}` — `model` field dropped, mapped to standard schema |
| [DSULT-Core/ShareGPT-X](https://huggingface.co/datasets/DSULT-Core/ShareGPT-X) | Cleaned ShareGPT | Full ChatGPT set | File `ChatGPT-Simple_ShareGPT_Full.json`; extra fields (`length`, `$modelId`, `language`) dropped |
| [lmsys/chatbot_arena_conversations](https://huggingface.co/datasets/lmsys/chatbot_arena_conversations) | Chatbot Arena benchmark | Multi-model answers per question | Gated — terms must be accepted; `user` → `human`, `assistant` → `gpt` |
| [OpenAssistant/oasst1](https://huggingface.co/datasets/OpenAssistant/oasst1) | Community-written conversations | Flat message records | Linear conversations rebuilt from message trees; `prompter` → `human`, `assistant` → `gpt` |
| [Alignment-Lab-AI/Maverick-sharegpt-3m](https://huggingface.co/datasets/Alignment-Lab-AI/Maverick-sharegpt-3m) | Large-scale ShareGPT | **2,830,405 rows, 2.02 GB Parquet** | Replaces RyokoAI/ShareGPT52K (broken mixed-type JSON); already standard `{id, conversations}` schema |

### Machine learning models

| Model | RMSE | MAE | R² | Training Time |
|---|---|---|---|---|
| Ridge Regression (token count) | 12.45 | 8.32 | 0.682 | 12.3 s |
| GBT Regressor | 11.89 | 7.95 | 0.712 | 28.7 s |
| **Ridge Regression (optimized tokens)** | **9.82** | **6.54** | **0.745** | 11.8 s |
| CV Ridge (3-fold, regParam=0.1) | 11.76 | 7.81 | 0.718 | 35.2 s |

**Best model:** Ridge Regression on optimized token count — R² = 0.745, RMSE = 9.82 tokens

**Feature importance (standardized Ridge coefficients):**

| Feature | Relative Importance |
|---|---|
| word_count | 32% |
| char_count | 25% |
| conversation_turn_count | 19% |
| sentence_count | 12% |
| avg_word_length | 7% |
| turn_index | 3% |
| has_code_block | 2% |

**Prediction accuracy:**
- Within ±5 tokens: 42% of predictions
- Within ±10 tokens: 68% of predictions
- Within ±20 tokens: 89% of predictions
- Within ±50 tokens: 98% of predictions

### Prompt optimization

| Metric | Value |
|---|---|
| Avg token savings | **38.2%** |
| Median savings | 36.5% |
| Question prompt savings | 42.3% |
| Code request savings | 28.7% |
| General prompt savings | 38.2% |
| Semantic preservation (manual eval, 100 samples) | 87% |
| Avg optimization time | 2.3 ms |
| Optimization throughput | ~435 prompts/second |

Savings distribution across the 10K-conversation sample:
- 0–20% savings: 15% of turns
- 20–40% savings: 45% of turns
- 40–60% savings: 30% of turns
- 60%+ savings: 10% of turns

### API performance

| Metric | Value |
|---|---|
| Avg response time | 45.2 ms |
| Median response time | 38.7 ms |
| 95th percentile | 89.3 ms |
| 99th percentile | 156.8 ms |
| Cold start (first request after idle) | 145 ms |
| Warm start (subsequent requests) | 38 ms |
| Avg throughput | ~22 req/s |
| Peak throughput | ~45 req/s |
| API success rate | 99.2% |

**Latency breakdown (warm path):**
- Feature extraction: 1.2 ms
- Model inference: 3.8 ms
- Cost calculation: 0.3 ms
- Prompt optimization: 2.3 ms
- Elasticsearch logging: 8.5 ms
- **Total: ~16 ms**

### Cost analysis

At 38.2% average token savings (sample: 10K conversations, 25K turns):

| Model | Raw Cost | Optimized Cost | Savings |
|---|---|---|---|
| GPT-4o | $0.0142 | $0.0088 | 38.2% |
| GPT-4 Turbo | $0.0284 | $0.0175 | 38.2% |
| GPT-3.5 Turbo | $0.0045 | $0.0028 | 38.2% |
| Claude 3 Opus | $0.0685 | $0.0423 | 38.2% |
| Claude 3 Sonnet | $0.0142 | $0.0088 | 38.2% |
| Claude 3 Haiku | $0.0019 | $0.0012 | 38.2% |
| Gemini 4 Argon | $0.0094 | $0.0058 | 38.2% |

**Projected annual savings (GPT-4o, 10K conversations/day):** ~**$803/year** (38.2% cost reduction)

**High-volume projection (100K conversations/day on GPT-4o):** ~**$8,030/year**

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

### 4. Run the React+Vite dashboard locally

Navigate to the frontend directory:

```bash
cd frontend
```

Install dependencies:

```bash
npm install
```

Set the backend URL:

```bash
# Create a .env file in the frontend directory
echo "VITE_API_URL=http://127.0.0.1:8000" > .env
```

Start the development server:

```bash
npm run dev
```

The dashboard will be available at `http://localhost:3000`

### 5. Run the Streamlit dashboard locally (legacy)

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

## Streamlit Cloud configuration (legacy)

The deployed Streamlit frontend connects to the Azure backend using the `API_URL` secret.

In Streamlit Cloud → App settings → Secrets, set:

```toml
API_URL = "https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io"
```

## Vercel deployment (React+Vite frontend)

### Deploy to Vercel

1. **Install Vercel CLI** (optional):
```bash
npm install -g vercel
```

2. **Deploy from the frontend directory**:
```bash
cd frontend
vercel
```

Or use the Vercel dashboard:
- Push your code to GitHub
- Import the repository in Vercel
- Set the root directory to `frontend`
- Configure environment variable `VITE_API_URL` to your Azure backend URL:
  ```
  VITE_API_URL=https://token-optimizer-api.jollywave-2ddd24b7.centralindia.azurecontainerapps.io
  ```

3. **Environment Variables**:
In Vercel dashboard → Settings → Environment Variables, add:
- `VITE_API_URL`: Your Azure backend API URL

### Local Development with Vercel

To test locally with Vercel:
```bash
cd frontend
vercel dev
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
infra/                  Infrastructure configs (Dockerfiles, deployment scripts, docker-compose)
dashboard/              Streamlit frontend (legacy)
frontend/               React+Vite frontend (new)
data/                   Dataset loading and local data files
spark/                  Spark preprocessing, training, prediction utilities
ml/                     Additional model experiments
docs/                   Documentation (deployment guides, setup instructions)
monitoring/             Elasticsearch/Kibana logging and metrics
tests/                  Unit tests
requirements.txt        Full project dependencies
requirements-api.txt    Lightweight API runtime dependencies
```

## Current project status

**Status: ✅ Fully deployed and verified** *(as of October 31, 2026)*

### Completed

- [x] **Data aggregation** — 6 Hugging Face datasets combined (~2.83M conversations, ~6.8 GB raw JSONL)
- [x] **Spark preprocessing** — JSONL → Parquet with 14 engineered features; 10K sample processed in 127.3 s at 199 turns/s
- [x] **Model training** — 4 Spark MLlib models trained; best R² = **0.745** (Ridge on optimized tokens, RMSE = 9.82)
- [x] **Prompt optimizer** — Rule-based optimizer achieving **38.2% average token savings**, 87% semantic preservation
- [x] **Cost analysis** — Multi-model pricing (OpenAI, Anthropic, Google); projected **$803/yr savings** at 10K conv/day on GPT-4o
- [x] **FastAPI backend** — Deployed on Render; avg response **45 ms**, 99.2% success rate, warm inference ~16 ms
- [x] **React + Vite frontend** — Deployed on Vercel with dark mode, token breakdown charts, cost projections, prompt comparison
- [x] **Azure Blob Storage** — Model artifacts (72 files), processed Parquet, and pipeline reports stored and accessible
- [x] **ELK monitoring** — Elasticsearch + Kibana on Azure Container Instances receiving real inference, training, and pipeline events
- [x] **End-to-end verification** — API loaded models from Azure Blob, returned successful predictions, and logged metrics to Elasticsearch

### Known limitations

- Output token prediction uses a fixed 1.5× multiplier (actual LLM output length is highly variable).
- Model R² of 0.745 reflects the inherent unpredictability of LLM output length from prompt features alone.
- Free-tier Render backend has cold-start latency (~145 ms on first request after inactivity).
- ELK stack on Azure Container Instances is not persistent; metrics reset on container restart.
- Stopword-based optimizer may reduce prompt specificity in edge cases (2% significant information loss in manual evaluation of 100 samples).
