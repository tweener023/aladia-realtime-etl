"""
Data Warehouse Component
Handles loading transformed data into the analytics warehouse.
"""

import os
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import create_engine, text
import structlog

logger = structlog.get_logger()


class WarehouseLoader:
    """Handles loading data into the analytics warehouse."""

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize warehouse connection."""
        if connection_string is None:
            connection_string = self._build_connection_string()

        self.connection_string = connection_string
        self.engine = create_engine(connection_string)

    def _build_connection_string(self) -> str:
        """Build warehouse connection string from environment variables."""
        host = os.getenv("WAREHOUSE_DB_HOST", "localhost")
        port = os.getenv("WAREHOUSE_DB_PORT", "5433")
        database = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
        user = os.getenv("WAREHOUSE_DB_USER", "postgres")
        password = os.getenv("WAREHOUSE_DB_PASSWORD", "postgres")

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def health_check(self) -> Dict:
        """Check warehouse health."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as health_check"))
                row = result.fetchone()
                if row and row[0] == 1:
                    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
                else:
                    return {"status": "unhealthy", "error": "Invalid response"}
        except Exception as e:
            logger.error("Warehouse health check failed", error=str(e))
            return {"status": "unhealthy", "error": str(e)}

    def load_order_event(self, event_data: Dict) -> bool:
        """Load a single order event into the warehouse."""
        try:
            with self.engine.connect() as conn:
                insert_query = text(
                    """
                    INSERT INTO order_events (
                        order_id, customer_id, product_name, quantity, unit_price,
                        order_date, status, event_type, source_timestamp
                    ) VALUES (
                        :order_id, :customer_id, :product_name, :quantity, :unit_price,
                        :order_date, :status, :event_type, :source_timestamp
                    )
                """
                )

                conn.execute(
                    insert_query,
                    {
                        "order_id": event_data.get("order_id"),
                        "customer_id": event_data.get("customer_id"),
                        "product_name": event_data.get("product_name"),
                        "quantity": event_data.get("quantity"),
                        "unit_price": float(event_data.get("unit_price", 0)),
                        "order_date": event_data.get("order_date"),
                        "status": event_data.get("status"),
                        "event_type": event_data.get("operation", "INSERT"),
                        "source_timestamp": event_data.get("timestamp", datetime.utcnow()),
                    },
                )

                conn.commit()
                return True

        except Exception as e:
            logger.error("Failed to load order event", event=event_data, error=str(e))
            return False

    def get_daily_stats(self) -> Dict:
        """Get daily statistics from the warehouse."""
        try:
            with self.engine.connect() as conn:
                query = text(
                    """
                    SELECT 
                        COUNT(*) as total_events,
                        COUNT(DISTINCT order_id) as unique_orders,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        SUM(quantity * unit_price) as total_revenue
                    FROM order_events 
                    WHERE DATE(processed_at) = CURRENT_DATE
                """
                )

                result = conn.execute(query)
                row = result.fetchone()

                return {
                    "date": datetime.utcnow().date().isoformat(),
                    "total_events": row[0],
                    "unique_orders": row[1],
                    "unique_customers": row[2],
                    "total_revenue": float(row[3]) if row[3] else 0.0,
                }

        except Exception as e:
            logger.error("Failed to get daily stats", error=str(e))
            return {"error": str(e)}
