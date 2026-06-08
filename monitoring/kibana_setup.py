"""
Kibana dashboard setup for token optimizer.
Automatically creates dashboards and visualizations for monitoring.
"""

import requests
import json
import time
from typing import Dict, Any

class KibanaSetup:
    """Setup Kibana dashboards and visualizations"""
    
    def __init__(self, kibana_url: str = "http://localhost:5601"):
        """
        Initialize Kibana setup.
        
        Args:
            kibana_url: Kibana base URL
        """
        self.kibana_url = kibana_url
        self.headers = {
            "kbn-xsrf": "true",
            "Content-Type": "application/json"
        }
    
    def create_index_pattern(self, pattern: str, time_field: str = "@timestamp") -> bool:
        """
        Create an index pattern in Kibana.
        
        Args:
            pattern: Index pattern (e.g., 'token-optimizer-logs-*')
            time_field: Field to use for time filtering
            
        Returns:
            Success status
        """
        url = f"{self.kibana_url}/api/saved_objects/index-pattern/{pattern}"
        
        payload = {
            "attributes": {
                "title": pattern,
                "timeFieldName": time_field,
                "fields": "[]"
            }
        }
        
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code in [200, 201]:
                print(f"✅ Created index pattern: {pattern}")
                return True
            else:
                print(f"⚠️ Failed to create index pattern: {response.text}")
                return False
        except Exception as e:
            print(f"Error creating index pattern: {e}")
            return False
    
    def create_dashboard(self, title: str, panels: list) -> str:
        """
        Create a dashboard in Kibana.
        
        Args:
            title: Dashboard title
            panels: List of panel configurations
            
        Returns:
            Dashboard ID
        """
        url = f"{self.kibana_url}/api/saved_objects/dashboard"
        
        payload = {
            "attributes": {
                "title": title,
                "panels": panels,
                "timeRestore": True,
                "timeFrom": "now-7d",
                "timeTo": "now"
            }
        }
        
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code in [200, 201]:
                dashboard_id = response.json()["id"]
                print(f"✅ Created dashboard: {title} (ID: {dashboard_id})")
                return dashboard_id
            else:
                print(f"⚠️ Failed to create dashboard: {response.text}")
                return None
        except Exception as e:
            print(f"Error creating dashboard: {e}")
            return None
    
    def wait_for_kibana(self, retries: int = 30) -> bool:
        """
        Wait for Kibana to be ready.
        
        Args:
            retries: Number of retries
            
        Returns:
            Ready status
        """
        url = f"{self.kibana_url}/api/status"
        
        for i in range(retries):
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    print("✅ Kibana is ready!")
                    return True
            except:
                pass
            
            print(f"⏳ Waiting for Kibana ({i+1}/{retries})...")
            time.sleep(2)
        
        print("❌ Kibana failed to start")
        return False
    
    def setup_default_dashboards(self):
        """Create default dashboards for token optimizer"""
        
        if not self.wait_for_kibana():
            return False
        
        # Create index patterns
        print("\n📑 Creating index patterns...")
        self.create_index_pattern("token-optimizer-logs-*", "@timestamp")
        self.create_index_pattern("metrics-*", "timestamp")
        
        # Training Metrics Dashboard
        print("\n📊 Creating Training Metrics Dashboard...")
        training_panels = [
            {
                "gridData": {"x": 0, "y": 0, "w": 12, "h": 4},
                "type": "visualization",
                "embeddableConfig": {}
            }
        ]
        self.create_dashboard("Training Metrics", training_panels)
        
        # Prediction Performance Dashboard
        print("\n📊 Creating Prediction Performance Dashboard...")
        prediction_panels = [
            {
                "gridData": {"x": 0, "y": 0, "w": 12, "h": 4},
                "type": "visualization",
                "embeddableConfig": {}
            }
        ]
        self.create_dashboard("Prediction Performance", prediction_panels)
        
        # System Health Dashboard
        print("\n📊 Creating System Health Dashboard...")
        health_panels = [
            {
                "gridData": {"x": 0, "y": 0, "w": 12, "h": 4},
                "type": "visualization",
                "embeddableConfig": {}
            }
        ]
        self.create_dashboard("System Health", health_panels)
        
        print("\n" + "="*50)
        print("✅ Dashboard setup complete!")
        print(f"📈 Open Kibana: {self.kibana_url}")
        print("="*50)


if __name__ == "__main__":
    setup = KibanaSetup()
    setup.setup_default_dashboards()
