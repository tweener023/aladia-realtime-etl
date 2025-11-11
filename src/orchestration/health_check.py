"""
Health Check and Orchestration Component
Monitors all ETL pipeline components.
"""

import asyncio
import json
from typing import Dict
from datetime import datetime

from fastapi import FastAPI
import uvicorn
import structlog

# Import our components
from src.source_db import SourceDatabase
from src.cdc import CDCManager
from src.message_queue import MessageQueueManager
from src.warehouse import WarehouseLoader
from src.processing import ETLProcessor

logger = structlog.get_logger()

app = FastAPI(title="Aladia ETL Pipeline Health Monitor", version="0.1.0")


class HealthChecker:
    """Centralized health checking for all pipeline components."""

    def __init__(self):
        """Initialize health checker."""
        self.components = {
            "source_db": SourceDatabase(),
            "cdc": CDCManager(),
            "message_queue": MessageQueueManager(),
            "warehouse": WarehouseLoader(),
            "processor": ETLProcessor(),
        }

    async def check_all_components(self) -> Dict:
        """Check health of all components."""
        results = {}
        overall_healthy = True

        for name, component in self.components.items():
            try:
                health_result = component.health_check()
                results[name] = health_result

                if health_result.get("status") != "healthy":
                    overall_healthy = False

            except Exception as e:
                logger.error(f"Health check failed for {name}", error=str(e))
                results[name] = {
                    "status": "unhealthy",
                    "error": f"Health check exception: {str(e)}",
                }
                overall_healthy = False

        return {
            "overall_status": "healthy" if overall_healthy else "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "components": results,
        }


# Initialize health checker
health_checker = HealthChecker()


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Aladia ETL Pipeline Health Monitor", "version": "0.1.0"}


@app.get("/health")
async def health():
    """Overall health check endpoint."""
    return await health_checker.check_all_components()


@app.get("/health/{component}")
async def component_health(component: str):
    """Health check for specific component."""
    if component not in health_checker.components:
        return {"error": f"Component '{component}' not found"}

    try:
        result = health_checker.components[component].health_check()
        return {component: result}
    except Exception as e:
        return {component: {"status": "unhealthy", "error": str(e)}}


def main():
    """CLI interface for health checks."""
    import click
    from dotenv import load_dotenv

    load_dotenv()

    @click.group()
    def cli():
        """Health Check CLI"""
        pass

    @cli.command()
    def check():
        """Check health of all components."""

        async def run_check():
            result = await health_checker.check_all_components()
            click.echo(json.dumps(result, indent=2))

        asyncio.run(run_check())

    @cli.command()
    @click.option("--host", default="0.0.0.0", help="Host to bind to")
    @click.option("--port", default=8000, help="Port to bind to")
    def serve(host, port):
        """Start health check web server."""
        uvicorn.run(app, host=host, port=port)

    cli()


if __name__ == "__main__":
    main()
