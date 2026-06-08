"""
Quick start script for Elasticsearch + Kibana setup.
Automates container startup and configuration.
"""

import subprocess
import time
import sys
import requests

def run_command(cmd, description):
    """Run a shell command and report status"""
    print(f"\n📦 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed")
            return True
        else:
            print(f"❌ {description} failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_docker():
    """Check if Docker is installed"""
    print("🐳 Checking Docker installation...")
    try:
        result = subprocess.run("docker --version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {result.stdout.strip()}")
            return True
    except:
        pass
    
    print("❌ Docker not found. Please install Docker: https://www.docker.com/products/docker-desktop")
    return False

def start_containers():
    """Start Docker containers"""
    print("\n" + "="*60)
    print("🚀 STARTING ELASTICSEARCH + KIBANA")
    print("="*60)
    
    if not check_docker():
        sys.exit(1)
    
    # Start containers
    run_command("docker-compose up -d", "Starting containers")
    
    # Wait for services
    print("\n⏳ Waiting for services to start (this may take 1-2 minutes)...")
    
    # Check Elasticsearch
    elasticsearch_ready = wait_for_service("http://localhost:9200", "Elasticsearch")
    
    # Check Kibana
    kibana_ready = wait_for_service("http://localhost:5601/api/status", "Kibana")
    
    if elasticsearch_ready and kibana_ready:
        print("\n✅ All services are running!")
        print("\n" + "="*60)
        print("📊 ACCESS POINTS")
        print("="*60)
        print("📈 Kibana Dashboard: http://localhost:5601")
        print("🔍 Elasticsearch: http://localhost:9200")
        print("📝 Logstash: http://localhost:5000")
        print("="*60)
        return True
    else:
        print("\n⚠️ Some services failed to start. Check Docker logs:")
        print("docker-compose logs")
        return False

def wait_for_service(url, service_name, retries=60, interval=2):
    """Wait for a service to be ready"""
    for i in range(retries):
        try:
            response = requests.get(url, timeout=2)
            if response.status_code < 500:  # Any response is good
                print(f"✅ {service_name} is ready!")
                return True
        except:
            pass
        
        remaining = retries - i - 1
        if remaining > 0:
            print(f"⏳ Waiting for {service_name} ({remaining}s remaining)...")
        time.sleep(interval)
    
    print(f"❌ {service_name} failed to start")
    return False

def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing Python dependencies...")
    run_command(
        "pip install elasticsearch python-json-logger requests",
        "Installing packages"
    )

def setup_kibana():
    """Setup Kibana dashboards"""
    print("\n📊 Setting up Kibana dashboards...")
    time.sleep(5)  # Give Kibana time to fully initialize
    
    try:
        # Import and run setup
        from monitoring.kibana_setup import KibanaSetup
        setup = KibanaSetup()
        setup.setup_default_dashboards()
        return True
    except ImportError:
        print("⚠️ Could not import kibana_setup. Running it separately...")
        return run_command("python monitoring/kibana_setup.py", "Kibana dashboard setup")
    except Exception as e:
        print(f"⚠️ Kibana setup encountered an issue: {e}")
        return False

def test_logging():
    """Test logging and metrics"""
    print("\n🧪 Testing logging and metrics...")
    try:
        from monitoring.logging_config import setup_logging
        from monitoring.metrics import get_metrics_collector
        
        logger = setup_logging()
        logger.info("✅ Logging test message")
        
        collector = get_metrics_collector()
        collector.record_training_metrics(
            model_name="test_model",
            mae=1.5,
            rmse=2.0,
            r2=0.95,
            training_time=10.0,
            dataset_size=1000
        )
        print("✅ Metrics recorded successfully")
        return True
    except Exception as e:
        print(f"⚠️ Testing encountered an issue: {e}")
        print("This is normal if services are still initializing")
        return False

def main():
    """Main setup flow"""
    print("\n" + "="*60)
    print("🎯 ELASTICSEARCH + KIBANA QUICK START")
    print("="*60)
    
    # Step 1: Check Docker
    if not check_docker():
        sys.exit(1)
    
    # Step 2: Start containers
    if not start_containers():
        print("\n⚠️ Services failed to start. Check logs and try again.")
        print("Run: docker-compose logs")
        sys.exit(1)
    
    # Step 3: Install Python dependencies
    install_dependencies()
    
    # Step 4: Setup Kibana
    setup_kibana()
    
    # Step 5: Test logging
    test_logging()
    
    # Final instructions
    print("\n" + "="*60)
    print("✅ SETUP COMPLETE!")
    print("="*60)
    print("\n📚 Next Steps:")
    print("1. Open Kibana: http://localhost:5601")
    print("2. Create index patterns for your data")
    print("3. Build custom dashboards")
    print("4. Integrate logging into your code:")
    print("   from monitoring.logging_config import setup_logging")
    print("   from monitoring.metrics import get_metrics_collector")
    print("\n📖 Full Documentation: ELASTICSEARCH_KIBANA_SETUP.md")
    print("\n🛑 To stop services:")
    print("   docker-compose down")
    print("="*60)

if __name__ == "__main__":
    main()
