# Render Deployment Guide

## Overview

This guide explains how to deploy the AI Token Optimizer API to Render for token prediction inference.

## Why Render?

- **Free tier available** for web services
- **No Spark/Java needed** - Pure Python inference (models are only 63KB)
- **Fast & lightweight** - Cold start < 2 seconds
- **HTTPS included** - Automatic SSL certificates
- **Easy deployment** - Git-based deployment

## Prerequisites

1. A Render account (free at [render.com](https://render.com))
2. A GitHub repository with this code
3. Models bundled in `models/` directory (already present)

## Deployment Steps

### 1. Update `.gitignore`

Ensure models are tracked (remove `models/` from `.gitignore`):

```bash
# Remove models/ from .gitignore if present
git add models/
git commit -m "Add model artifacts for Render deployment"
```

### 2. Push to GitHub

```bash
git push origin main
```

### 3. Deploy on Render

#### Option A: Using render.yaml (Recommended)

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New +" → "Web Service"
3. Connect your GitHub repository
4. Render will auto-detect `infra/render.yaml` configuration
5. Deploy

#### Option B: Manual Configuration

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New +" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Name**: `ai-token-optimizer-api`
   - **Region**: Choose nearest region
   - **Branch**: `main`
   - **Runtime**: Docker
   - **Dockerfile**: `infra/Dockerfile.render`
   - **Instance Type**: Free (or Starter for production)
5. Add Environment Variables (optional):
   - `MONGODB_URI` (if you want request logging)
   - `AZURE_STORAGE_ACCOUNT` (if you want Azure sync)
   - `AZURE_STORAGE_KEY` (if you want Azure sync)
   - `AZURE_CONTAINER` = `pipeline-data`
6. Click "Create Web Service"

### 4. Verify Deployment

Once deployed, check the health endpoint:

```bash
curl https://your-app-name.onrender.com/health
```

Expected response:
```json
{
  "status": "ok",
  "models": "models",
  "source": "bundled"
}
```

### 5. Test Prediction

```bash
curl -X POST "https://your-app-name.onrender.com/predict?prompt=Explain+neural+networks"
```

## Architecture

### Container Image

- **Base**: `python:3.11-slim` (~120MB)
- **Dependencies**: FastAPI, pandas, numpy, pyarrow, tiktoken (~50MB)
- **Models**: 63KB (bundled)
- **Total image size**: ~170MB

### Model Loading

The API checks for models in this order:
1. **Bundled models** (`./models/`) - Used on Render
2. **Azure Blob Storage** - Fallback if not bundled

### Inference Process

1. Request comes in → FastAPI
2. Extract text features (char count, word count, etc.)
3. Load model parameters from Parquet files
4. Apply StandardScaler transformation
5. Predict tokens using LinearRegression coefficients
6. Return prediction + optimized prompt

## Performance

- **Cold start**: ~2-3 seconds (Render free tier)
- **Request latency**: ~50-100ms after warm
- **Throughput**: ~10-20 requests/second (free tier)
- **Memory usage**: ~100-200MB

## Environment Variables

### Required (None)

The API works out-of-the-box with bundled models.

### Optional

| Variable | Purpose | Required |
|----------|---------|----------|
| `MONGODB_URI` | Request logging to MongoDB | No |
| `AZURE_STORAGE_ACCOUNT` | Azure Blob sync | No |
| `AZURE_STORAGE_KEY` | Azure Blob sync | No |
| `AZURE_CONTAINER` | Azure Blob container | No |
| `ES_HOST` | Elasticsearch logging | No |
| `KIBANA_URL` | Kibana dashboard | No |

## Troubleshooting

### Models not found

If you see "No model artifacts found" error:
1. Ensure `models/` directory is in your Git repository
2. Check that Parquet files exist: `models/*/stages/*/*.parquet`
3. Verify Dockerfile copies models: `COPY models/ ./models/`

### Port errors

Render uses dynamic ports. The Dockerfile sets `PORT=10000` but Render may override this. The app uses the `PORT` environment variable automatically.

### Memory limits

Free tier has 512MB RAM. If you see OOM errors:
- Upgrade to Starter tier (1GB RAM)
- Remove optional dependencies (pymongo, elasticsearch)

### Slow cold starts

Render free tier has limited CPU. Options:
- Upgrade to Starter tier
- Use a smaller base image (alpine)
- Pre-load models on startup

## Cost

- **Free tier**: $0/month (750 hours/month, 512MB RAM)
- **Starter tier**: $7/month (1GB RAM, better performance)
- **Standard tier**: $25/month (2GB RAM, production-ready)

## Monitoring

Render provides:
- **Logs**: Real-time log viewer
- **Metrics**: CPU, memory, response time
- **Alerts**: Email notifications on failures
- **Deployments**: Version history and rollback

## Scaling

To handle more traffic:
1. Upgrade instance type (Starter/Standard)
2. Enable autoscaling (requires Standard tier)
3. Add CDN for static assets (if you add frontend)

## Next Steps

1. Deploy frontend (React+Vite) to Vercel
2. Configure CORS to allow your frontend domain
3. Set up monitoring with Render metrics
4. Add rate limiting for production use
