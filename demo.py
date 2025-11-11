"""
Demo Script - Aladia Real-Time ETL Pipeline
Demonstrates end-to-end functionality of the ETL pipeline.
"""

import time
import json
import asyncio
from datetime import datetime

import click
from dotenv import load_dotenv

from src.source_db import SourceDatabase
from src.cdc import CDCManager
from src.message_queue import MessageQueueManager
from src.warehouse import WarehouseLoader
from src.orchestration.health_check import HealthChecker

load_dotenv()


class ETLDemo:
    """Demonstrates the complete ETL pipeline functionality."""

    def __init__(self):
        """Initialize demo components."""
        self.source_db = SourceDatabase()
        self.cdc = CDCManager()
        self.message_queue = MessageQueueManager()
        self.warehouse = WarehouseLoader()
        self.health_checker = HealthChecker()

    async def run_demo(self):
        """Run the complete ETL demo."""
        print("\n🚀 Aladia Real-Time ETL Pipeline Demo")
        print("=" * 50)
        
        # Step 1: Health Check
        print("\n📊 Step 1: Checking component health...")
        health_result = await self.health_checker.check_all_components()
        
        healthy_components = sum(1 for comp in health_result["components"].values() 
                               if comp.get("status") == "healthy")
        total_components = len(health_result["components"])
        
        print(f"Health Status: {health_result['overall_status'].upper()}")
        print(f"Healthy Components: {healthy_components}/{total_components}")
        
        if health_result["overall_status"] != "healthy":
            print("⚠️  Some components are unhealthy. Demo may not work properly.")
            for name, status in health_result["components"].items():
                if status.get("status") != "healthy":
                    print(f"  - {name}: {status.get('error', 'Unknown error')}")
        
        # Step 2: Initialize source data
        print("\n📂 Step 2: Setting up source database...")
        try:
            self.source_db.initialize_schema()
            self.source_db.insert_sample_data(20)
            initial_count = self.source_db.get_orders_count()
            print(f"✅ Source database ready with {initial_count} orders")
        except Exception as e:
            print(f"❌ Failed to setup source database: {e}")
            return
        
        # Step 3: Setup CDC
        print("\n🔄 Step 3: Setting up Change Data Capture...")
        try:
            success = self.cdc.setup_orders_cdc()
            if success:
                print("✅ CDC connector configured successfully")
            else:
                print("❌ CDC setup failed")
        except Exception as e:
            print(f"❌ CDC setup error: {e}")
        
        # Step 4: Simulate data changes
        print("\n📈 Step 4: Simulating order updates...")
        try:
            print("Generating order updates to trigger CDC events...")
            self.source_db.simulate_order_updates(5)
            final_count = self.source_db.get_orders_count()
            print(f"✅ Generated {final_count - initial_count} new orders")
        except Exception as e:
            print(f"❌ Failed to simulate updates: {e}")
        
        # Step 5: Check warehouse
        print("\n🏛️ Step 5: Checking data warehouse...")
        try:
            stats = self.warehouse.get_daily_stats()
            if "error" not in stats:
                print(f"✅ Warehouse stats: {json.dumps(stats, indent=2)}")
            else:
                print(f"❌ Warehouse error: {stats['error']}")
        except Exception as e:
            print(f"❌ Warehouse check failed: {e}")
        
        print("\n🎉 Demo completed!")
        print("\nNext steps:")
        print("1. Check Kafka topics for CDC events: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092")
        print("2. View Debezium connector status: curl http://localhost:8083/connectors/orders-postgres-connector/status")
        print("3. Access health dashboard: http://localhost:8000/health")
        print("4. Monitor Spark jobs: http://localhost:8080")

    def cleanup(self):
        """Clean up resources."""
        try:
            self.message_queue.close()
        except Exception as e:
            print(f"Cleanup error: {e}")


@click.group()
def cli():
    """Aladia ETL Pipeline Demo CLI"""
    pass


@cli.command()
def run():
    """Run the complete ETL pipeline demo."""
    demo = ETLDemo()
    try:
        asyncio.run(demo.run_demo())
    finally:
        demo.cleanup()


@cli.command()
def health():
    """Quick health check of all components."""
    async def check():
        health_checker = HealthChecker()
        result = await health_checker.check_all_components()
        
        print(f"\nOverall Status: {result['overall_status'].upper()}")
        print(f"Timestamp: {result['timestamp']}")
        print("\nComponent Details:")
        
        for name, status in result["components"].items():
            status_icon = "✅" if status.get("status") == "healthy" else "❌"
            print(f"  {status_icon} {name}: {status.get('status')}")
            if status.get("error"):
                print(f"      Error: {status.get('error')}")
    
    asyncio.run(check())


@cli.command()
@click.option('--orders', default=10, help='Number of orders to insert')
def generate_data(orders):
    """Generate sample data in source database."""
    print(f"Generating {orders} sample orders...")
    
    source_db = SourceDatabase()
    try:
        source_db.insert_sample_data(orders)
        count = source_db.get_orders_count()
        print(f"✅ Successfully generated data. Total orders: {count}")
    except Exception as e:
        print(f"❌ Failed to generate data: {e}")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()