"""
Source Database Component
Manages PostgreSQL source database with sample orders data and CDC setup.
"""

import os
import time
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine, text
import structlog

logger = structlog.get_logger()


class SourceDatabase:
    """Manages the source PostgreSQL database for the ETL pipeline."""

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize database connection."""
        if connection_string is None:
            connection_string = self._build_connection_string()

        self.connection_string = connection_string
        self.engine = create_engine(connection_string)

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from environment variables."""
        host = os.getenv("SOURCE_DB_HOST", "localhost")
        port = os.getenv("SOURCE_DB_PORT", "5432")
        database = os.getenv("SOURCE_DB_NAME", "sourcedb")
        user = os.getenv("SOURCE_DB_USER", "postgres")
        password = os.getenv("SOURCE_DB_PASSWORD", "postgres")

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def health_check(self) -> dict:
        """Check if database is healthy and accessible."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as health_check"))
                row = result.fetchone()
                if row and row[0] == 1:
                    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
                else:
                    return {
                        "status": "unhealthy",
                        "error": "Invalid response",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
        except Exception as e:
            logger.error("Database health check failed", error=str(e))
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def initialize_schema(self):
        """Create database schema and enable CDC features."""
        try:
            with self.engine.connect() as conn:
                # Enable logical replication for CDC
                conn.execute(text("ALTER SYSTEM SET wal_level = logical"))
                conn.execute(text("ALTER SYSTEM SET max_replication_slots = 4"))
                conn.execute(text("ALTER SYSTEM SET max_wal_senders = 4"))
                conn.commit()

                # Create orders table
                create_orders_table = text(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id SERIAL PRIMARY KEY,
                        customer_id INTEGER NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        quantity INTEGER NOT NULL,
                        unit_price DECIMAL(10, 2) NOT NULL,
                        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )
                conn.execute(create_orders_table)

                # Create trigger for updating updated_at timestamp
                create_trigger = text(
                    """
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql';
                    
                    DROP TRIGGER IF EXISTS update_orders_updated_at ON orders;
                    CREATE TRIGGER update_orders_updated_at
                        BEFORE UPDATE ON orders
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_at_column();
                """
                )
                conn.execute(create_trigger)

                # Enable replica identity for CDC
                conn.execute(text("ALTER TABLE orders REPLICA IDENTITY FULL"))

                conn.commit()

                logger.info("Database schema initialized successfully")

        except Exception as e:
            logger.error("Failed to initialize database schema", error=str(e))
            raise

    def insert_sample_data(self, num_orders: int = 10):
        """Insert sample orders data."""
        try:
            with self.engine.connect() as conn:
                sample_products = [
                    "MacBook Pro",
                    "iPhone 14",
                    "AirPods Pro",
                    "iPad Air",
                    "Samsung Galaxy S23",
                    "Dell XPS 13",
                    "Sony WH-1000XM4",
                    "Nintendo Switch",
                    "Tesla Model 3",
                    "Apple Watch",
                ]

                for i in range(num_orders):
                    insert_order = text(
                        """
                        INSERT INTO orders (customer_id, product_name, quantity, unit_price, status)
                        VALUES (:customer_id, :product_name, :quantity, :unit_price, :status)
                    """
                    )

                    conn.execute(
                        insert_order,
                        {
                            "customer_id": (i % 100) + 1,
                            "product_name": sample_products[i % len(sample_products)],
                            "quantity": (i % 5) + 1,
                            "unit_price": round(100.0 + (i * 50.5), 2),
                            "status": "pending" if i % 3 == 0 else "confirmed",
                        },
                    )

                conn.commit()
                logger.info(f"Inserted {num_orders} sample orders")

        except Exception as e:
            logger.error("Failed to insert sample data", error=str(e))
            raise

    def simulate_order_updates(self, iterations: int = 5):
        """Simulate ongoing order updates to generate CDC events."""
        try:
            statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

            for i in range(iterations):
                with self.engine.connect() as conn:
                    # Update random orders
                    update_order = text(
                        """
                        UPDATE orders 
                        SET status = :new_status 
                        WHERE order_id = (
                            SELECT order_id FROM orders 
                            ORDER BY RANDOM() LIMIT 1
                        )
                    """
                    )

                    conn.execute(update_order, {"new_status": statuses[i % len(statuses)]})

                    # Insert new order
                    insert_order = text(
                        """
                        INSERT INTO orders (customer_id, product_name, quantity, unit_price, status)
                        VALUES (:customer_id, :product_name, :quantity, :unit_price, :status)
                    """
                    )

                    conn.execute(
                        insert_order,
                        {
                            "customer_id": 1000 + i,
                            "product_name": f"Dynamic Product {i}",
                            "quantity": (i % 3) + 1,
                            "unit_price": round(50.0 + (i * 25.0), 2),
                            "status": "pending",
                        },
                    )

                    conn.commit()
                    logger.info(f"Simulation iteration {i + 1} completed")

                time.sleep(2)  # Wait between updates to simulate real activity

        except Exception as e:
            logger.error("Failed to simulate order updates", error=str(e))
            raise

    def get_orders_count(self) -> int:
        """Get total number of orders in the database."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM orders"))
                count = result.fetchone()[0]
                return count
        except Exception as e:
            logger.error("Failed to get orders count", error=str(e))
            raise


def main():
    """CLI interface for source database management."""
    import click
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    @click.group()
    def cli():
        """Source Database Management CLI"""
        pass

    @cli.command()
    def init():
        """Initialize database schema and sample data."""
        db = SourceDatabase()
        click.echo("Initializing database schema...")
        db.initialize_schema()
        click.echo("Inserting sample data...")
        db.insert_sample_data(50)
        click.echo("Database initialization completed!")

    @cli.command()
    def health():
        """Check database health."""
        db = SourceDatabase()
        result = db.health_check()
        click.echo(f"Health Status: {result}")

    @cli.command()
    @click.option("--iterations", default=10, help="Number of update iterations")
    def simulate(iterations):
        """Simulate order updates."""
        db = SourceDatabase()
        click.echo(f"Starting order simulation with {iterations} iterations...")
        db.simulate_order_updates(iterations)
        click.echo("Simulation completed!")

    @cli.command()
    def stats():
        """Show database statistics."""
        db = SourceDatabase()
        count = db.get_orders_count()
        click.echo(f"Total orders: {count}")

    cli()


if __name__ == "__main__":
    main()
