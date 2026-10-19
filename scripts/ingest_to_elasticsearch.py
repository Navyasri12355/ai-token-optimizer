#!/usr/bin/env python3
"""
Ingest sample data into Elasticsearch for Kibana dashboard visualization
"""

import json
import requests
from typing import List, Dict
import sys
import time

def load_sample_data(filename: str = "sample_data.json") -> List[Dict]:
    """Load sample data from JSON file"""
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        print(f"✓ Loaded {len(data)} records from {filename}")
        return data
    except FileNotFoundError:
        print(f"✗ File {filename} not found. Run generate_sample_data.py first.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"✗ Invalid JSON in {filename}")
        sys.exit(1)

def create_elasticsearch_index(es_url: str, index_name: str, username: str = None, password: str = None):
    """Create Elasticsearch index with proper mapping"""
    headers = {"Content-Type": "application/json"}
    auth = None
    if username and password:
        auth = (username, password)
    
    # Index mapping for token optimizer data
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "model_name": {"type": "keyword"},
                "original_prompt": {"type": "text"},
                "optimized_prompt": {"type": "text"},
                "input_tokens": {"type": "integer"},
                "output_tokens": {"type": "integer"},
                "total_tokens": {"type": "integer"},
                "estimated_cost": {"type": "float"},
                "token_savings_percent": {"type": "float"},
                "compression_percent": {"type": "float"},
                "user_id": {"type": "keyword"},
                "session_id": {"type": "keyword"}
            }
        }
    }
    
    try:
        url = f"{es_url}/{index_name}"
        response = requests.put(url, headers=headers, json=mapping, auth=auth, timeout=10)
        if response.status_code in [200, 201]:
            print(f"✓ Created index: {index_name}")
        elif response.status_code == 400:
            print(f"ℹ Index {index_name} may already exist")
        else:
            print(f"✗ Failed to create index: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Error creating index: {e}")

def ingest_single_record(es_url: str, index_name: str, record: Dict, username: str = None, password: str = None):
    """Ingest a single record into Elasticsearch"""
    headers = {"Content-Type": "application/json"}
    auth = None
    if username and password:
        auth = (username, password)
    
    try:
        url = f"{es_url}/{index_name}/_doc"
        response = requests.post(url, headers=headers, json=record, auth=auth, timeout=5)
        return response.status_code in [200, 201]
    except Exception as e:
        print(f"✗ Error ingesting record: {e}")
        return False

def ingest_bulk_data(es_url: str, index_name: str, records: List[Dict], username: str = None, password: str = None):
    """Ingest multiple records using bulk API"""
    headers = {"Content-Type": "application/x-ndjson"}
    auth = None
    if username and password:
        auth = (username, password)
    
    # Build bulk request body
    bulk_data = ""
    for record in records:
        # Index action
        bulk_data += json.dumps({"index": {"_index": index_name}}) + "\n"
        # Document data
        bulk_data += json.dumps(record) + "\n"
    
    try:
        url = f"{es_url}/_bulk"
        response = requests.post(url, headers=headers, data=bulk_data, auth=auth, timeout=30)
        result = response.json()
        
        if response.status_code == 200:
            successful = result.get('items', [{}])[0].get('index', {}).get('result') == 'created'
            if successful:
                print(f"✓ Bulk ingested {len(records)} records")
                return True
        print(f"✗ Bulk ingestion failed: {response.status_code}")
        return False
    except Exception as e:
        print(f"✗ Error in bulk ingestion: {e}")
        return False

def verify_ingestion(es_url: str, index_name: str, username: str = None, password: str = None):
    """Verify data was ingested correctly"""
    headers = {"Content-Type": "application/json"}
    auth = None
    if username and password:
        auth = (username, password)
    
    try:
        url = f"{es_url}/{index_name}/_count"
        response = requests.get(url, headers=headers, auth=auth, timeout=10)
        if response.status_code == 200:
            count = response.json().get('count', 0)
            print(f"✓ Verified {count} documents in index {index_name}")
            return count
        else:
            print(f"✗ Verification failed: {response.status_code}")
            return 0
    except Exception as e:
        print(f"✗ Error verifying ingestion: {e}")
        return 0

def main():
    print("=== Elasticsearch Data Ingestion ===\n")
    
    # Get Elasticsearch configuration
    es_url = input("Enter Elasticsearch URL (e.g., http://localhost:9200 or https://your-endpoint:9200): ").strip()
    if not es_url:
        print("✗ Elasticsearch URL is required")
        sys.exit(1)
    
    # Remove trailing slash
    es_url = es_url.rstrip('/')
    
    index_name = input("Enter index name (default: token-optimizer-logs): ").strip() or "token-optimizer-logs"
    
    # Authentication (optional)
    use_auth = input("Use authentication? (y/n, default: n): ").strip().lower()
    username = None
    password = None
    if use_auth == 'y':
        username = input("Enter username (default: elastic): ").strip() or "elastic"
        password = input("Enter password: ").strip()
    
    # Load sample data
    print("\nLoading sample data...")
    records = load_sample_data()
    
    # Create index
    print("\nCreating Elasticsearch index...")
    create_elasticsearch_index(es_url, index_name, username, password)
    
    # Ingest data
    print("\nIngesting data into Elasticsearch...")
    success = ingest_bulk_data(es_url, index_name, records, username, password)
    
    if success:
        # Verify ingestion
        print("\nVerifying ingestion...")
        time.sleep(2)  # Wait for indexing
        count = verify_ingestion(es_url, index_name, username, password)
        
        if count > 0:
            print(f"\n✅ Success! {count} records ingested into Elasticsearch")
            print(f"\nNext steps:")
            print(f"1. Open Kibana at {es_url.replace('9200', '5601')}")
            print(f"2. Go to Stack Management → Index Patterns")
            print(f"3. Create index pattern: {index_name}")
            print(f"4. Select @timestamp as time field")
            print(f"5. Import dashboard from kibana/dashboard.ndjson")
            print(f"6. View your dashboard!")
        else:
            print("\n⚠️ Ingestion completed but verification failed")
    else:
        print("\n❌ Failed to ingest data")
        print("Try ingesting records individually for debugging")

if __name__ == "__main__":
    main()
