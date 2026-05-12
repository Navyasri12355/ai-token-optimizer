# Elasticsearch + Kibana Setup & Usage

## Overview

This project integrates **Elasticsearch** and **Kibana** for:
- 📊 **Centralized Logging**: Collect logs from all components
- 📈 **Real-time Metrics**: Track model performance and predictions
- 🔍 **Data Exploration**: Search and analyze logs interactively
- 📉 **Visualizations**: Create dashboards for monitoring

## Architecture

```
┌─────────────────────────────────────┐
│  Application (token-optimizer)      │
│  - Training                         │
│  - Predictions                      │
│  - API Requests                     │
└────────────┬────────────────────────┘
             │
             ├─→ logging_config.py ─────┐
             ├─→ metrics.py ────────────┤
             └─→ Logstash ──────────────┤
                                        │
                                        ▼
                          ┌──────────────────────┐
                          │  Elasticsearch       │
                          │  (Port 9200)         │
                          │  - Indexing          │
                          │  - Search            │
                          │  - Analytics         │
                          └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  Kibana              │
                          │  (Port 5601)         │
                          │  - Dashboards        │
                          │  - Visualizations    │
                          │  - Monitoring        │
                          └──────────────────────┘
```

## Quick Start

### 1. Start Services with Docker Compose

```bash
docker-compose up -d
```

This starts:
- **Elasticsearch** on `http://localhost:9200`
- **Kibana** on `http://localhost:5601`
- **Logstash** on `http://localhost:5000` and `http://localhost:9600`

Verify services are running:
```bash
docker ps
```

### 2. Update Python Requirements

Add these packages to `requirements.txt`:
```
elasticsearch>=8.0.0
python-json-logger>=2.0.0
requests>=2.28.0
```

Install them:
```bash
pip install -r requirements.txt
```

### 3. Initialize Logging in Your Code

```python
from logging_config import setup_logging, get_training_logger
from metrics import get_metrics_collector

# Setup logging
logger = setup_logging(es_host="localhost:9200")
logger.info("Application started")

# Setup metrics
metrics = get_metrics_collector()
```

### 4. Setup Kibana Dashboards

```bash
python kibana_setup.py
```

Then open Kibana: http://localhost:5601

## Usage Examples

### Training with Metrics

```python
from metrics import get_metrics_collector
from logging_config import get_training_logger
import time

metrics = get_metrics_collector()
logger = get_training_logger()

start_time = time.time()

# Train your model...
model.fit(X_train, y_train)

# Record metrics
training_time = time.time() - start_time
metrics.record_training_metrics(
    model_name="input_rf_model",
    mae=2.45,
    rmse=3.12,
    r2=0.92,
    training_time=training_time,
    dataset_size=len(X_train),
    num_trees=100,
    max_depth=10
)

logger.info(f"Training completed in {training_time:.2f} seconds")
```

### Recording Predictions

```python
from metrics import get_metrics_collector
import time

metrics = get_metrics_collector()

start_time = time.time()
prediction = model.predict(features)
prediction_time = (time.time() - start_time) * 1000  # ms

metrics.record_prediction_metrics(
    model_name="input_rf_model",
    input_tokens=int(prediction[0]),
    output_tokens=int(prediction[1]),
    prediction_time_ms=prediction_time
)
```

### Logging Events

```python
from logging_config import setup_logging

logger = setup_logging()

# Different log levels
logger.debug("Detailed debug information")
logger.info("General information")
logger.warning("Warning - something unexpected")
logger.error("Error - something went wrong")
logger.critical("Critical - application failing")
```

### Recording Data Processing Metrics

```python
from metrics import get_metrics_collector
import time

metrics = get_metrics_collector()

start = time.time()
# Process your data...
rows_processed = 100000
processing_time = time.time() - start

metrics.record_data_processing(
    operation="feature_engineering",
    rows_processed=rows_processed,
    processing_time_seconds=processing_time
)
```

## Kibana Features

### Discover Tab
- Search and filter logs
- View individual documents
- Analyze data trends

### Visualizations
Create custom charts:
- Line graphs (time-series metrics)
- Bar charts (model comparisons)
- Metrics (KPIs like avg accuracy)
- Tables (raw data)

### Dashboards
Monitor multiple visualizations together:
- **Training Dashboard**: Model metrics over time
- **Prediction Dashboard**: Inference performance
- **System Health**: Application logs and errors

### Alerts (Optional)
Set up alerts for:
- High error rates
- Slow predictions
- Training failures
- Resource exhaustion

## Index Structure

### Logs Index
```json
{
  "timestamp": "2026-05-11T10:30:00Z",
  "level": "INFO",
  "logger": "token_optimizer.training",
  "message": "Training completed",
  "module": "train_mllib",
  "function": "train_model",
  "line": 42
}
```

### Metrics Index
```json
{
  "timestamp": "2026-05-11T10:30:00Z",
  "event_type": "training",
  "model_name": "input_rf_model",
  "metrics": {
    "mae": 2.45,
    "rmse": 3.12,
    "r2": 0.92,
    "training_time_seconds": 45.3,
    "dataset_size": 80000
  }
}
```

## Troubleshooting

### Elasticsearch Connection Error
```
⚠️ Could not connect to Elasticsearch
```

**Solution:**
```bash
# Check if ES is running
docker ps | grep elasticsearch

# Restart services
docker-compose restart elasticsearch
```

### Kibana Not Accessible
```
ERR_CONNECTION_REFUSED on localhost:5601
```

**Solution:**
```bash
# Wait for services to fully start (30-60 seconds)
docker logs kibana

# If it still fails, restart Kibana
docker-compose restart kibana
```

### No Data in Kibana
1. Check Elasticsearch has data:
   ```bash
   curl http://localhost:9200/_cat/indices
   ```

2. Create index pattern matching your index names:
   - Pattern: `token-optimizer-logs-*`
   - Time field: `@timestamp`

3. Verify logs are being sent:
   ```python
   logger = setup_logging()
   logger.info("Test message")
   ```

### High Memory Usage
Elasticsearch can use significant memory. Adjust in `docker-compose.yml`:
```yaml
environment:
  - "ES_JAVA_OPTS=-Xms256m -Xmx256m"  # Reduce from 512m
```

## Production Considerations

### Security
- Enable authentication in Elasticsearch
- Use HTTPS for Kibana
- Set up role-based access control (RBAC)

### Data Retention
- Set up index lifecycle policies
- Archive old indices
- Delete data older than X days

### Performance
- Configure appropriate shard counts
- Use rollover indices for time-series data
- Monitor disk usage

### Backup & Recovery
- Regular Elasticsearch backups
- Test restore procedures
- Document recovery processes

## API Endpoints

### Elasticsearch
- **Health**: `http://localhost:9200/_cluster/health`
- **Indices**: `http://localhost:9200/_cat/indices`
- **Search**: `http://localhost:9200/<index>/_search`

### Kibana
- **Home**: `http://localhost:5601`
- **Discover**: `http://localhost:5601/app/discover`
- **Dashboards**: `http://localhost:5601/app/dashboards`

## Files Overview

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Container orchestration |
| `logging_config.py` | Application logging setup |
| `metrics.py` | Metrics collection and tracking |
| `kibana_setup.py` | Dashboard initialization |
| `logs/logstash.conf` | Logstash pipeline configuration |

## Next Steps

1. ✅ Start Docker services
2. ✅ Initialize logging in your code
3. ✅ Run `kibana_setup.py` to create dashboards
4. ✅ View metrics at `http://localhost:5601`
5. ✅ Create custom visualizations

## References

- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)
- [Logstash Pipeline](https://www.elastic.co/guide/en/logstash/current/pipeline.html)

---

**Version**: 1.0  
**Last Updated**: 2026-05-11
