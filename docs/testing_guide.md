# Testing Guide - MLlib & Elasticsearch+Kibana

## Overview

This guide explains how to run comprehensive tests for:
1. **Spark MLlib** - Machine Learning models for token prediction
2. **Elasticsearch & Kibana** - Centralized logging and visualization

## Prerequisites

### For MLlib Testing
- Python 3.7+
- Spark installed and configured
- Required packages: `pyspark`, `pandas`, `numpy`

### For Elasticsearch & Kibana Testing
- Docker installed and running
- Docker Compose installed
- Python 3.7+

## Installation

### Install Dependencies

```bash
pip install -r requirements.txt
pip install elasticsearch python-json-logger
```

For PySpark setup on Windows, ensure `SPARK_HOME` is set in environment variables.

## Running Tests

### Test 1: Spark MLlib

The MLlib test suite trains and evaluates machine learning models for token prediction.

#### Run the Test

```bash
python test_mllib.py
```

#### What It Tests

1. **Data Creation**: Creates sample training data if not available
2. **Data Loading**: Loads and validates processed data
3. **Random Forest (Input Tokens)**: Trains RF model to predict input token counts
4. **Random Forest (Output Tokens)**: Trains RF model to predict output token counts
5. **Gradient Boosted Trees**: Trains GBT model for combined token prediction
6. **Feature Importance**: Analyzes which features most influence predictions

#### Expected Output

```
============================================================
🧪 SPARK MLLIB TEST SUITE
============================================================

📊 Creating Sample Data...
✅ Sample data created: 1000 samples
   Features: context_len, text_len, num_words, avg_word_len, question_flag
   Targets: input_tokens, output_tokens

📂 Loading Data...
✅ Data loaded: 1000 rows, 7 columns
✅ All required columns present

...

============================================================
✅ TEST SUMMARY
============================================================
✅ PASSED - data_creation
✅ PASSED - data_loading
✅ PASSED - rf_input_tokens
✅ PASSED - rf_output_tokens
✅ PASSED - gbt_model
✅ PASSED - feature_importance

📈 Overall: 6/6 tests passed

🎉 All MLlib tests passed!
```

#### Model Details

**Random Forest Configuration:**
- Number of Trees: 50
- Max Depth: 10
- Min Instances Per Node: 1
- Seed: 42

**Gradient Boosted Trees Configuration:**
- Max Iterations: 100
- Max Depth: 5
- Seed: 42

**Evaluation Metrics:**
- RMSE (Root Mean Square Error)
- R² (Coefficient of Determination)
- MAE (Mean Absolute Error)

### Test 2: Elasticsearch & Kibana

The Elasticsearch+Kibana test suite verifies end-to-end logging and monitoring setup.

#### Prerequisites

Ensure Docker and Docker Compose are running:

```bash
# Check Docker
docker --version

# Check Docker Compose
docker-compose --version

# Verify Docker daemon is running
docker ps
```

#### Run the Test

```bash
python test_elasticsearch_kibana.py
```

#### What It Tests

1. **Docker Check**: Verifies Docker is installed and running
2. **Service Startup**: Starts Elasticsearch, Kibana, and Logstash containers
3. **Elasticsearch Connectivity**: Tests connection to Elasticsearch (port 9200)
4. **Kibana Connectivity**: Tests connection to Kibana (port 5601)
5. **Index Creation**: Creates a test index with proper mappings
6. **Document Indexing**: Indexes test documents
7. **Search Functionality**: Performs full-text search on indexed documents
8. **Logging Integration**: Tests Python logging with Elasticsearch
9. **Index Pattern**: Creates Kibana index pattern for log visualization
10. **Cleanup**: Deletes test data after verification

#### Expected Output

```
============================================================
🧪 ELASTICSEARCH & KIBANA TEST SUITE
============================================================

🐳 Checking Docker Installation...
✅ Docker version 20.10.x, build xxxxx

🔌 Checking Docker Daemon...
✅ Docker daemon is running

🚀 Starting Docker Compose Services...
✅ Docker Compose services started
   Waiting for services to be ready...

⏳ Waiting for services to be ready...
✅ Elasticsearch is ready
✅ Kibana is ready

🔗 Testing Elasticsearch Connectivity...
✅ Elasticsearch connected
   Status: green
   Nodes: 1
   Active Shards: 1

🔗 Testing Kibana Connectivity...
✅ Kibana connected
   Status: green
   Version: 8.0.0

...

============================================================
✅ TEST SUMMARY
============================================================
✅ PASSED - docker_installed
✅ PASSED - docker_running
✅ PASSED - es_connectivity
✅ PASSED - kibana_connectivity
✅ PASSED - index_creation
✅ PASSED - document_indexing
✅ PASSED - elasticsearch_search
✅ PASSED - logging_integration
✅ PASSED - kibana_index_pattern

📈 Overall: 9/9 tests passed

🎉 All Elasticsearch & Kibana tests passed!

📊 Access Kibana at: http://localhost:5601
```

#### Accessing Kibana

After successful test completion:

1. Open browser: http://localhost:5601
2. Navigate to **Analytics > Discover**
3. Select the index pattern from the test
4. View logs and create custom visualizations

#### Stopping Services

To stop Docker Compose services:

```bash
docker-compose down

# Remove data volumes (optional)
docker-compose down -v
```

## Troubleshooting

### MLlib Issues

**Issue**: Spark not found
```
FileNotFoundError: [Errno 2] No such file or directory: 'spark-submit'
```
**Solution**: Set SPARK_HOME environment variable and add to PATH
```bash
# Windows
set SPARK_HOME=C:\path\to\spark
set PATH=%PATH%;%SPARK_HOME%\bin
```

**Issue**: Java not found
**Solution**: Install Java 8+ and set JAVA_HOME
```bash
# Verify Java installation
java -version
```

**Issue**: Out of memory
**Solution**: Reduce data size in test or increase Spark memory
```python
# In test_mllib.py, reduce n_samples
n_samples = 500  # Instead of 1000
```

### Elasticsearch & Kibana Issues

**Issue**: Docker daemon not running
```
Cannot connect to Docker daemon
```
**Solution**: Start Docker Desktop or Docker service

**Issue**: Ports already in use
```
Error response from daemon: ... port is already allocated
```
**Solution**: Stop existing containers or use different ports
```bash
# Check running containers
docker ps

# Stop specific container
docker stop <container_id>
```

**Issue**: Services not starting in time
```
⚠️ Elasticsearch/Kibana did not start in time
```
**Solution**: Increase retry count in test or wait longer
```python
# In test_elasticsearch_kibana.py
return self.wait_for_services(retries=60)  # Increase from 30
```

**Issue**: Connection refused
```
Cannot connect to Elasticsearch at localhost:9200
```
**Solution**: Verify services are running
```bash
# Check logs
docker-compose logs elasticsearch
docker-compose logs kibana

# Restart services
docker-compose down
docker-compose up -d
```

## Advanced Options

### Run MLlib with Custom Data

Replace sample data with your actual processed.csv:

```python
# In test_mllib.py, modify:
tester = MLlibTester()
tester.df = spark.read.csv("path/to/your/processed.csv", header=True, inferSchema=True)
results = tester.run_all_tests()
```

### Configure Elasticsearch Retention

Edit `docker-compose.yml` to set index lifecycle management:

```yaml
elasticsearch:
  environment:
    - xpack.slm.enabled=true
```

### Custom Kibana Dashboards

After test passes, create custom dashboards in Kibana UI:
1. Go to Analytics > Discover
2. Create visualizations
3. Add to Dashboard

## Performance Notes

**MLlib Training Time:**
- Random Forest: ~5-10 seconds (1000 samples)
- Gradient Boosted Trees: ~10-15 seconds (1000 samples)
- Feature Importance: ~1 second

**Elasticsearch & Kibana Startup Time:**
- Initial startup: ~30-60 seconds
- Subsequent tests: ~2-5 seconds
- Total test suite: ~60-120 seconds

## Next Steps

After successful testing:

1. **Integrate logging into your application:**
   ```python
   from logging_config import setup_logging
   logger = setup_logging()
   logger.info("Your application message")
   ```

2. **Create production dashboards in Kibana**
3. **Configure alerting for anomalies**
4. **Set up log rotation and retention policies**

## References

- [Spark MLlib Documentation](https://spark.apache.org/docs/latest/ml-guide.html)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana Documentation](https://www.elastic.co/guide/en/kibana/current/index.html)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
