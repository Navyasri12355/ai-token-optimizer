#!/usr/bin/env python3
"""
Generate sample token optimizer data for Elasticsearch/Kibana dashboard
Run this to populate your dashboard with realistic demo data
"""

import random
import json
from datetime import datetime, timedelta
from typing import List, Dict
import requests

# Model configurations
MODELS = [
    "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5", "gpt-5.5-pro",
    "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.4-pro",
    "claude-opus-4-8", "claude-sonnet-5", "claude-sonnet-4-6", "claude-haiku-4-5",
    "gemini-3-1-pro-preview", "gemini-3-5-flash"
]

# Sample prompts
SAMPLE_PROMPTS = [
    "Explain quantum computing in simple terms",
    "Write a Python function to sort a list",
    "What is the difference between REST and GraphQL?",
    "Create a marketing email for a SaaS product",
    "Summarize the key points of machine learning",
    "Debug this JavaScript code: console.log(undefined)",
    "Write a SQL query to get top 10 users by revenue",
    "Explain the concept of microservices architecture",
    "Create a React component for a user profile",
    "What are the best practices for API security?"
]

def generate_sample_record(timestamp: datetime) -> Dict:
    """Generate a single sample token optimizer record"""
    model = random.choice(MODELS)
    prompt = random.choice(SAMPLE_PROMPTS)
    
    # Generate realistic token counts
    input_tokens = random.randint(50, 2000)
    output_tokens = random.randint(20, 1500)
    total_tokens = input_tokens + output_tokens
    
    # Calculate costs based on model pricing (simplified)
    model_pricing = {
        "gpt-5.6-sol": (0.005, 0.03),
        "gpt-5.6-terra": (0.0025, 0.015),
        "gpt-5.6-luna": (0.001, 0.006),
        "gpt-5.5": (0.005, 0.03),
        "gpt-5.5-pro": (0.03, 0.18),
        "gpt-5.4": (0.0025, 0.015),
        "gpt-5.4-mini": (0.00075, 0.0045),
        "gpt-5.4-nano": (0.0002, 0.00125),
        "gpt-5.4-pro": (0.03, 0.18),
        "claude-opus-4-8": (0.005, 0.025),
        "claude-sonnet-5": (0.002, 0.01),
        "claude-sonnet-4-6": (0.003, 0.015),
        "claude-haiku-4-5": (0.001, 0.005),
        "gemini-3-1-pro-preview": (0.002, 0.012),
        "gemini-3-5-flash": (0.0015, 0.009)
    }
    
    input_price, output_price = model_pricing.get(model, (0.001, 0.002))
    estimated_cost = (input_tokens / 1000) * input_price + (output_tokens / 1000) * output_price
    
    # Generate optimization metrics
    token_savings_percent = random.uniform(10, 45)
    compression_percent = random.uniform(15, 40)
    
    return {
        "@timestamp": timestamp.isoformat(),
        "model_name": model,
        "original_prompt": prompt,
        "optimized_prompt": prompt[:len(prompt)//2] + "...",  # Simplified
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "estimated_cost": round(estimated_cost, 6),
        "token_savings_percent": round(token_savings_percent, 1),
        "compression_percent": round(compression_percent, 1),
        "user_id": f"user_{random.randint(1, 100)}",
        "session_id": f"session_{random.randint(1000, 9999)}"
    }

def generate_sample_data(num_records: int = 500, hours_back: int = 24) -> List[Dict]:
    """Generate sample data for the specified time period"""
    records = []
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours_back)
    
    # Generate records distributed over time
    time_delta = (end_time - start_time) / num_records
    
    for i in range(num_records):
        timestamp = start_time + (time_delta * i)
        # Add some randomness to timing
        timestamp += timedelta(minutes=random.randint(-5, 5))
        records.append(generate_sample_record(timestamp))
    
    return records

def save_to_json(records: List[Dict], filename: str = "sample_data.json"):
    """Save records to JSON file"""
    with open(filename, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"Saved {len(records)} records to {filename}")

def send_to_elasticsearch(records: List[Dict], es_url: str, index_name: str = "token-optimizer-logs"):
    """Send records to Elasticsearch"""
    headers = {"Content-Type": "application/json"}
    
    for record in records:
        url = f"{es_url}/{index_name}/_doc"
        try:
            response = requests.post(url, headers=headers, json=record, timeout=5)
            if response.status_code in [200, 201]:
                print(f"✓ Sent record to Elasticsearch")
            else:
                print(f"✗ Failed to send record: {response.status_code}")
        except Exception as e:
            print(f"✗ Error sending to Elasticsearch: {e}")

if __name__ == "__main__":
    print("Generating sample token optimizer data...")
    
    # Generate 500 records over 24 hours
    records = generate_sample_data(num_records=500, hours_back=24)
    print(f"Generated {len(records)} sample records")
    
    # Save to JSON file
    save_to_json(records)
    
    # Optionally send to Elasticsearch if URL is provided
    es_url = input("Enter Elasticsearch URL (press Enter to skip): ").strip()
    if es_url:
        index_name = input("Enter index name (default: token-optimizer-logs): ").strip() or "token-optimizer-logs"
        print(f"\nSending data to Elasticsearch at {es_url}...")
        send_to_elasticsearch(records, es_url, index_name)
    
    print("\nSample data generation complete!")
    print("Import this data into Kibana to populate your dashboard.")
