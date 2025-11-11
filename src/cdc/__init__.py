"""
CDC (Change Data Capture) Component
Manages Debezium connectors and CDC event processing.
"""

import os
import json
import time
from typing import Dict, Optional, List
from datetime import datetime

import requests
import structlog

logger = structlog.get_logger()


class CDCManager:
    """Manages Debezium CDC connectors and operations."""

    def __init__(self, connect_url: Optional[str] = None):
        """Initialize CDC Manager with Kafka Connect URL."""
        self.connect_url = connect_url or os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083")

    def health_check(self) -> Dict:
        """Check if Kafka Connect and CDC components are healthy."""
        try:
            response = requests.get(f"{self.connect_url}/", timeout=10)
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "connect_version": response.json().get("version", "unknown"),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            else:
                return {
                    "status": "unhealthy",
                    "error": f"HTTP {response.status_code}",
                    "timestamp": datetime.utcnow().isoformat(),
                }
        except Exception as e:
            logger.error("CDC health check failed", error=str(e))
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def list_connectors(self) -> List[str]:
        """List all registered connectors."""
        try:
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error("Failed to list connectors", error=str(e))
            raise

    def get_connector_status(self, connector_name: str) -> Dict:
        """Get status of a specific connector."""
        try:
            response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/status", timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error("Failed to get connector status", connector=connector_name, error=str(e))
            raise

    def create_connector(self, config_path: str) -> Dict:
        """Create a new Debezium connector from JSON config file."""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)

            connector_name = config.get("name")
            if not connector_name:
                raise ValueError("Connector config must include 'name' field")

            # Check if connector already exists
            existing_connectors = self.list_connectors()
            if connector_name in existing_connectors:
                logger.info(
                    "Connector already exists, updating configuration", connector=connector_name
                )
                return self.update_connector(connector_name, config)

            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={"Content-Type": "application/json"},
                json=config,
                timeout=30,
            )
            response.raise_for_status()

            result = response.json()
            logger.info(
                "Connector created successfully",
                connector=connector_name,
                status=result.get("state"),
            )
            return result

        except Exception as e:
            logger.error("Failed to create connector", config_path=config_path, error=str(e))
            raise

    def update_connector(self, connector_name: str, config: Dict) -> Dict:
        """Update an existing connector configuration."""
        try:
            response = requests.put(
                f"{self.connect_url}/connectors/{connector_name}/config",
                headers={"Content-Type": "application/json"},
                json=config.get("config", config),
                timeout=30,
            )
            response.raise_for_status()

            result = response.json()
            logger.info("Connector updated successfully", connector=connector_name)
            return result

        except Exception as e:
            logger.error("Failed to update connector", connector=connector_name, error=str(e))
            raise

    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector."""
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{connector_name}", timeout=30
            )
            response.raise_for_status()

            logger.info("Connector deleted successfully", connector=connector_name)
            return True

        except Exception as e:
            logger.error("Failed to delete connector", connector=connector_name, error=str(e))
            raise

    def restart_connector(self, connector_name: str) -> bool:
        """Restart a connector."""
        try:
            response = requests.post(
                f"{self.connect_url}/connectors/{connector_name}/restart", timeout=30
            )
            response.raise_for_status()

            logger.info("Connector restarted successfully", connector=connector_name)
            return True

        except Exception as e:
            logger.error("Failed to restart connector", connector=connector_name, error=str(e))
            raise

    def wait_for_connector_running(self, connector_name: str, max_wait_seconds: int = 60) -> bool:
        """Wait for connector to reach RUNNING state."""
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            try:
                status = self.get_connector_status(connector_name)
                connector_state = status.get("connector", {}).get("state")

                if connector_state == "RUNNING":
                    # Also check that all tasks are running
                    tasks = status.get("tasks", [])
                    if all(task.get("state") == "RUNNING" for task in tasks):
                        logger.info("Connector is running successfully", connector=connector_name)
                        return True

                logger.info(
                    "Waiting for connector to start",
                    connector=connector_name,
                    state=connector_state,
                )
                time.sleep(2)

            except Exception as e:
                logger.warning(
                    "Error checking connector status", connector=connector_name, error=str(e)
                )
                time.sleep(2)

        logger.error(
            "Connector failed to reach RUNNING state",
            connector=connector_name,
            max_wait=max_wait_seconds,
        )
        return False

    def setup_orders_cdc(self) -> bool:
        """Setup CDC for orders table using predefined configuration."""
        config_path = "/app/config/debezium/postgres-connector.json"

        # Use relative path if running locally
        if not os.path.exists(config_path):
            config_path = "config/debezium/postgres-connector.json"

        try:
            logger.info("Setting up CDC for orders table")

            # Create the connector
            self.create_connector(config_path)

            # Wait for it to be running
            success = self.wait_for_connector_running("orders-postgres-connector")

            if success:
                logger.info("Orders CDC setup completed successfully")
            else:
                logger.error("Orders CDC setup failed")

            return success

        except Exception as e:
            logger.error("Failed to setup orders CDC", error=str(e))
            raise


def main():
    """CLI interface for CDC management."""
    import click
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    @click.group()
    def cli():
        """CDC Management CLI"""
        pass

    @cli.command()
    def health():
        """Check CDC health."""
        cdc = CDCManager()
        result = cdc.health_check()
        click.echo(f"CDC Health Status: {result}")

    @cli.command()
    def list():
        """List all connectors."""
        cdc = CDCManager()
        connectors = cdc.list_connectors()
        if connectors:
            click.echo("Registered connectors:")
            for connector in connectors:
                click.echo(f"  - {connector}")
        else:
            click.echo("No connectors registered")

    @cli.command()
    @click.argument("connector_name")
    def status(connector_name):
        """Get connector status."""
        cdc = CDCManager()
        status_info = cdc.get_connector_status(connector_name)
        click.echo(json.dumps(status_info, indent=2))

    @cli.command()
    def setup():
        """Setup orders CDC connector."""
        cdc = CDCManager()
        success = cdc.setup_orders_cdc()
        if success:
            click.echo("Orders CDC setup completed successfully!")
        else:
            click.echo("Orders CDC setup failed!")

    @cli.command()
    @click.argument("connector_name")
    def restart(connector_name):
        """Restart a connector."""
        cdc = CDCManager()
        success = cdc.restart_connector(connector_name)
        if success:
            click.echo(f"Connector {connector_name} restarted successfully!")
        else:
            click.echo(f"Failed to restart connector {connector_name}")

    cli()


if __name__ == "__main__":
    main()
