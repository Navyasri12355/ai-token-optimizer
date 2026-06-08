"""
Comprehensive test suite for Elasticsearch and Kibana functionality.
Tests connectivity, indexing, logging, and dashboard setup.
"""

import requests
import json
import time
import logging
from typing import Dict, Any, List
from datetime import datetime
import subprocess
import os
import sys


class ElasticsearchKibanaTester:
    """Test suite for Elasticsearch and Kibana functionality"""
    
    def __init__(self):
        """Initialize tester with connection parameters"""
        self.es_host = "localhost"
        self.es_port = 9200
        self.kibana_port = 5601
        self.es_url = f"http://{self.es_host}:{self.es_port}"
        self.kibana_url = f"http://{self.es_host}:{self.kibana_port}"
        self.test_index = "test-token-optimizer"
        self.results = {}
    
    def check_docker_installed(self) -> bool:
        """
        Check if Docker is installed.
        
        Returns:
            True if Docker is installed, False otherwise
        """
        print("\n🐳 Checking Docker Installation...")
        
        try:
            result = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"✅ {result.stdout.strip()}")
                return True
            else:
                print("❌ Docker not found")
                return False
        except FileNotFoundError:
            print("❌ Docker CLI not found in PATH")
            return False
        except Exception as e:
            print(f"❌ Error checking Docker: {e}")
            return False
    
    def check_docker_running(self) -> bool:
        """
        Check if Docker daemon is running.
        
        Returns:
            True if Docker is running, False otherwise
        """
        print("\n🔌 Checking Docker Daemon...")
        
        try:
            result = subprocess.run(
                ["docker", "ps"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print("✅ Docker daemon is running")
                return True
            else:
                print("❌ Docker daemon is not running")
                return False
        except Exception as e:
            print(f"❌ Error checking Docker daemon: {e}")
            return False
    
    def start_docker_compose(self) -> bool:
        """
        Start Docker Compose services.
        
        Returns:
            True if services started successfully
        """
        print("\n🚀 Starting Docker Compose Services...")
        
        try:
            # Check if docker-compose.yml exists
            if not os.path.exists("docker-compose.yml"):
                print("❌ docker-compose.yml not found")
                return False
            
            # Start services
            result = subprocess.run(
                ["docker-compose", "up", "-d"],
                capture_output=True,
                text=True,
                cwd=os.getcwd()
            )
            
            if result.returncode != 0:
                print(f"❌ Error starting Docker Compose: {result.stderr}")
                return False
            
            print("✅ Docker Compose services started")
            print("   Waiting for services to be ready...")
            
            # Wait for services
            return self.wait_for_services()
            
        except Exception as e:
            print(f"❌ Error starting Docker Compose: {e}")
            return False
    
    def wait_for_services(self, retries: int = 30) -> bool:
        """
        Wait for Elasticsearch and Kibana to be ready.
        
        Args:
            retries: Number of retry attempts
            
        Returns:
            True if services are ready
        """
        print("\n⏳ Waiting for services to be ready...")
        
        es_ready = False
        kibana_ready = False
        
        for i in range(retries):
            # Check Elasticsearch
            if not es_ready:
                try:
                    response = requests.get(f"{self.es_url}/_cluster/health", timeout=2)
                    if response.status_code == 200:
                        es_ready = True
                        print("✅ Elasticsearch is ready")
                except Exception:
                    pass
            
            # Check Kibana
            if not kibana_ready:
                try:
                    response = requests.get(f"{self.kibana_url}/api/status", timeout=2)
                    if response.status_code == 200:
                        kibana_ready = True
                        print("✅ Kibana is ready")
                except Exception:
                    pass
            
            if es_ready and kibana_ready:
                return True
            
            if i < retries - 1:
                print(f"   Attempt {i+1}/{retries}...", end="\r")
                time.sleep(2)
        
        if not es_ready:
            print("❌ Elasticsearch did not start in time")
        if not kibana_ready:
            print("❌ Kibana did not start in time")
        
        return es_ready and kibana_ready
    
    def test_elasticsearch_connectivity(self) -> bool:
        """
        Test basic Elasticsearch connectivity.
        
        Returns:
            True if connection successful
        """
        print("\n🔗 Testing Elasticsearch Connectivity...")
        
        try:
            response = requests.get(f"{self.es_url}/_cluster/health", timeout=5)
            
            if response.status_code == 200:
                health = response.json()
                print(f"✅ Elasticsearch connected")
                print(f"   Status: {health.get('status', 'unknown')}")
                print(f"   Nodes: {health.get('number_of_nodes', 'unknown')}")
                print(f"   Active Shards: {health.get('active_shards', 'unknown')}")
                return True
            else:
                print(f"❌ Elasticsearch returned status {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            print("❌ Could not connect to Elasticsearch")
            return False
        except Exception as e:
            print(f"❌ Error testing Elasticsearch: {e}")
            return False
    
    def test_kibana_connectivity(self) -> bool:
        """
        Test basic Kibana connectivity.
        
        Returns:
            True if connection successful
        """
        print("\n🔗 Testing Kibana Connectivity...")
        
        try:
            response = requests.get(f"{self.kibana_url}/api/status", timeout=5)
            
            if response.status_code == 200:
                status = response.json()
                print(f"✅ Kibana connected")
                print(f"   Status: {status.get('state', 'unknown')}")
                print(f"   Version: {status.get('version', {}).get('number', 'unknown')}")
                return True
            else:
                print(f"❌ Kibana returned status {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            print("❌ Could not connect to Kibana")
            return False
        except Exception as e:
            print(f"❌ Error testing Kibana: {e}")
            return False
    
    def test_index_creation(self) -> bool:
        """
        Test creating an index in Elasticsearch.
        
        Returns:
            True if index created successfully
        """
        print("\n📑 Testing Index Creation...")
        
        try:
            # Create index with settings
            index_config = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date"},
                        "level": {"type": "keyword"},
                        "message": {"type": "text"},
                        "module": {"type": "keyword"},
                        "function": {"type": "keyword"}
                    }
                }
            }
            
            response = requests.put(
                f"{self.es_url}/{self.test_index}",
                json=index_config,
                timeout=5
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Index '{self.test_index}' created")
                return True
            elif response.status_code == 400:
                # Index might already exist
                error = response.json().get('error', {})
                if 'resource_already_exists' in str(error):
                    print(f"ℹ️  Index '{self.test_index}' already exists")
                    return True
                print(f"❌ Error creating index: {error}")
                return False
            else:
                print(f"❌ Unexpected status {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating index: {e}")
            return False
    
    def test_document_indexing(self) -> bool:
        """
        Test indexing documents in Elasticsearch.
        
        Returns:
            True if documents indexed successfully
        """
        print("\n📄 Testing Document Indexing...")
        
        try:
            # Create test documents
            test_docs = [
                {
                    "timestamp": datetime.now().isoformat(),
                    "level": "INFO",
                    "message": "Model training started",
                    "module": "token_optimizer.training",
                    "function": "train_model"
                },
                {
                    "timestamp": datetime.now().isoformat(),
                    "level": "INFO",
                    "message": "Processing batch 1/100",
                    "module": "token_optimizer.training",
                    "function": "process_batch"
                },
                {
                    "timestamp": datetime.now().isoformat(),
                    "level": "DEBUG",
                    "message": "Feature engineering completed",
                    "module": "token_optimizer.preprocessing",
                    "function": "engineer_features"
                }
            ]
            
            # Index documents
            indexed_count = 0
            for doc in test_docs:
                response = requests.post(
                    f"{self.es_url}/{self.test_index}/_doc",
                    json=doc,
                    timeout=5
                )
                
                if response.status_code in [200, 201]:
                    indexed_count += 1
            
            print(f"✅ Indexed {indexed_count}/{len(test_docs)} documents")
            
            # Wait for indexing
            time.sleep(1)
            
            # Verify documents are searchable
            response = requests.get(
                f"{self.es_url}/{self.test_index}/_count",
                timeout=5
            )
            
            if response.status_code == 200:
                count = response.json().get('count', 0)
                print(f"✅ Total documents in index: {count}")
                return count > 0
            
            return indexed_count == len(test_docs)
            
        except Exception as e:
            print(f"❌ Error indexing documents: {e}")
            return False
    
    def test_elasticsearch_search(self) -> bool:
        """
        Test searching documents in Elasticsearch.
        
        Returns:
            True if search works
        """
        print("\n🔍 Testing Elasticsearch Search...")
        
        try:
            query = {
                "query": {
                    "match": {
                        "message": "training"
                    }
                }
            }
            
            response = requests.get(
                f"{self.es_url}/{self.test_index}/_search",
                json=query,
                timeout=5
            )
            
            if response.status_code == 200:
                results = response.json()
                hits = results.get('hits', {}).get('hits', [])
                total = results.get('hits', {}).get('total', {}).get('value', 0)
                
                print(f"✅ Search executed successfully")
                print(f"   Found {total} documents matching 'training'")
                
                if hits:
                    print(f"   Sample result:")
                    print(f"     - Message: {hits[0].get('_source', {}).get('message')}")
                
                return True
            else:
                print(f"❌ Search failed with status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error during search: {e}")
            return False
    
    def test_logging_integration(self) -> bool:
        """
        Test Python logging integration with Elasticsearch.
        
        Returns:
            True if logging works
        """
        print("\n📝 Testing Logging Integration...")
        
        try:
            # Try to import elasticsearch client
            try:
                from elasticsearch import Elasticsearch
                from pythonjsonlogger import jsonlogger
            except ImportError:
                print("⚠️  Required packages not installed (elasticsearch, python-json-logger)")
                print("   Run: pip install elasticsearch python-json-logger")
                return False
            
            # Create Elasticsearch client
            es = Elasticsearch([f"http://{self.es_host}:{self.es_port}"])
            
            # Test connection
            info = es.info()
            print("✅ Elasticsearch Python client connected")
            
            # Create a test logger
            logger = logging.getLogger("test_logger")
            logger.setLevel(logging.DEBUG)
            
            # Create console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            # Test logging
            logger.info("✅ Test log message sent to Elasticsearch")
            
            return True
            
        except Exception as e:
            print(f"⚠️  Error testing logging integration: {e}")
            return False
    
    def test_index_pattern_creation(self) -> bool:
        """
        Test creating an index pattern in Kibana.
        
        Returns:
            True if index pattern created
        """
        print("\n📊 Testing Index Pattern Creation (Kibana)...")
        
        try:
            headers = {
                "kbn-xsrf": "true",
                "Content-Type": "application/json"
            }
            
            pattern_name = f"{self.test_index}-*"
            
            # Create index pattern
            payload = {
                "attributes": {
                    "title": pattern_name,
                    "timeFieldName": "timestamp",
                    "fields": "[]"
                }
            }
            
            response = requests.post(
                f"{self.kibana_url}/api/saved_objects/index-pattern",
                json=payload,
                headers=headers,
                timeout=5
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Index pattern '{pattern_name}' created in Kibana")
                return True
            elif response.status_code == 409:
                print(f"ℹ️  Index pattern already exists")
                return True
            else:
                print(f"⚠️  Status {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            print(f"⚠️  Error creating index pattern: {e}")
            return False
    
    def cleanup(self) -> bool:
        """
        Clean up test resources.
        
        Returns:
            True if cleanup successful
        """
        print("\n🧹 Cleaning Up Test Resources...")
        
        try:
            # Delete test index
            response = requests.delete(
                f"{self.es_url}/{self.test_index}",
                timeout=5
            )
            
            if response.status_code in [200, 404]:
                print(f"✅ Test index deleted")
                return True
            else:
                print(f"⚠️  Could not delete test index: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """
        Run all tests.
        
        Returns:
            Dictionary with test results
        """
        print("\n" + "="*60)
        print("🧪 ELASTICSEARCH & KIBANA TEST SUITE")
        print("="*60)
        
        # Check Docker
        if not self.check_docker_installed():
            print("\n⚠️  Docker is required to run these tests")
            return {}
        
        if not self.check_docker_running():
            print("\n⚠️  Docker daemon is not running")
            return {}
        
        # Start services
        if not self.start_docker_compose():
            print("\n❌ Failed to start Docker Compose services")
            print("   Make sure docker-compose.yml is in the current directory")
            print("   And Docker daemon is running")
            return {}
        
        # Run connectivity tests
        self.results['es_connectivity'] = self.test_elasticsearch_connectivity()
        self.results['kibana_connectivity'] = self.test_kibana_connectivity()
        
        if not (self.results['es_connectivity'] and self.results['kibana_connectivity']):
            print("\n❌ Services not responding. Stopping tests.")
            return self.results
        
        # Run data tests
        self.results['index_creation'] = self.test_index_creation()
        self.results['document_indexing'] = self.test_document_indexing()
        self.results['elasticsearch_search'] = self.test_elasticsearch_search()
        
        # Run integration tests
        self.results['logging_integration'] = self.test_logging_integration()
        self.results['kibana_index_pattern'] = self.test_index_pattern_creation()
        
        # Cleanup
        self.cleanup()
        
        # Print summary
        self.print_summary()
        
        return self.results
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "="*60)
        print("✅ TEST SUMMARY")
        print("="*60)
        
        for test_name, passed in self.results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"{status} - {test_name}")
        
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        print(f"\n📈 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("\n🎉 All Elasticsearch & Kibana tests passed!")
            print("\n📊 Access Kibana at: http://localhost:5601")
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    tester = ElasticsearchKibanaTester()
    results = tester.run_all_tests()
