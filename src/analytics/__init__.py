"""
Analytics Module for Real-time ETL Pipeline
Provides sample queries and analytics capabilities for the data warehouse.
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text

import structlog

logger = structlog.get_logger()


class AnalyticsEngine:
    """Analytics engine for querying processed ETL data."""
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize analytics engine with database configuration."""
        self.db_config = db_config or {
            'host': 'localhost',
            'port': 5432,
            'database': 'orders_db',
            'username': 'postgres',
            'password': 'postgres'
        }
        
        # Create database connection
        self.engine = self._create_engine()
        logger.info("AnalyticsEngine initialized", db_config=self.db_config)
    
    def _create_engine(self) -> sa.Engine:
        """Create SQLAlchemy engine for database connections."""
        connection_string = (
            f"postgresql://{self.db_config['username']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        
        engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )
        
        return engine
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params=params or {})
                logger.debug("Query executed successfully", 
                           rows_returned=len(result))
                return result
        except Exception as e:
            logger.error("Query execution failed", 
                        error=str(e), 
                        query=query[:100])
            raise
    
    def get_order_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for orders."""
        query = """
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            AVG(quantity) as avg_quantity,
            AVG(price) as avg_price,
            SUM(quantity * price) as total_revenue,
            MIN(order_date) as earliest_order,
            MAX(order_date) as latest_order
        FROM orders
        WHERE status != 'cancelled'
        """
        
        result = self.execute_query(query)
        if not result.empty:
            stats = result.iloc[0].to_dict()
            logger.info("Order summary stats retrieved", stats=stats)
            return stats
        return {}
    
    def get_top_customers(self, limit: int = 10) -> pd.DataFrame:
        """Get top customers by total order value."""
        query = """
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(quantity) as total_quantity,
            SUM(quantity * price) as total_value,
            AVG(price) as avg_order_value,
            MAX(order_date) as last_order_date
        FROM orders 
        WHERE status != 'cancelled'
        GROUP BY customer_id
        ORDER BY total_value DESC
        LIMIT :limit
        """
        
        result = self.execute_query(query, {'limit': limit})
        logger.info("Top customers retrieved", count=len(result))
        return result
    
    def get_product_performance(self, limit: int = 20) -> pd.DataFrame:
        """Get product performance metrics."""
        query = """
        SELECT 
            product_id,
            COUNT(*) as order_count,
            SUM(quantity) as total_sold,
            AVG(quantity) as avg_quantity_per_order,
            AVG(price) as avg_price,
            SUM(quantity * price) as total_revenue,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM orders 
        WHERE status != 'cancelled'
        GROUP BY product_id
        ORDER BY total_revenue DESC
        LIMIT :limit
        """
        
        result = self.execute_query(query, {'limit': limit})
        logger.info("Product performance retrieved", count=len(result))
        return result
    
    def get_daily_order_trends(self, days: int = 30) -> pd.DataFrame:
        """Get daily order trends for the last N days."""
        query = """
        SELECT 
            DATE(order_date) as order_day,
            COUNT(*) as daily_orders,
            COUNT(DISTINCT customer_id) as daily_customers,
            SUM(quantity) as daily_quantity,
            SUM(quantity * price) as daily_revenue,
            AVG(price) as avg_daily_price
        FROM orders 
        WHERE order_date >= CURRENT_DATE - INTERVAL ':days days'
        AND status != 'cancelled'
        GROUP BY DATE(order_date)
        ORDER BY order_day DESC
        """
        
        result = self.execute_query(query, {'days': days})
        logger.info("Daily trends retrieved", 
                   days_count=len(result), 
                   period_days=days)
        return result
    
    def get_order_status_distribution(self) -> pd.DataFrame:
        """Get distribution of order statuses."""
        query = """
        SELECT 
            status,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage,
            SUM(quantity * price) as total_value
        FROM orders
        GROUP BY status
        ORDER BY count DESC
        """
        
        result = self.execute_query(query)
        logger.info("Order status distribution retrieved", 
                   status_count=len(result))
        return result
    
    def get_hourly_order_pattern(self) -> pd.DataFrame:
        """Get hourly order patterns to identify peak times."""
        query = """
        SELECT 
            EXTRACT(HOUR FROM order_date::timestamp) as hour_of_day,
            COUNT(*) as order_count,
            AVG(quantity * price) as avg_order_value
        FROM orders 
        WHERE status != 'cancelled'
        GROUP BY EXTRACT(HOUR FROM order_date::timestamp)
        ORDER BY hour_of_day
        """
        
        result = self.execute_query(query)
        logger.info("Hourly patterns retrieved")
        return result
    
    def get_real_time_metrics(self, minutes: int = 5) -> Dict[str, Any]:
        """Get real-time metrics for the last N minutes."""
        query = """
        SELECT 
            COUNT(*) as recent_orders,
            COUNT(DISTINCT customer_id) as active_customers,
            SUM(quantity * price) as recent_revenue,
            AVG(price) as avg_recent_price
        FROM orders 
        WHERE order_date >= NOW() - INTERVAL ':minutes minutes'
        AND status != 'cancelled'
        """
        
        result = self.execute_query(query, {'minutes': minutes})
        if not result.empty:
            metrics = result.iloc[0].to_dict()
            metrics['time_window_minutes'] = minutes
            metrics['calculated_at'] = datetime.utcnow().isoformat()
            logger.info("Real-time metrics retrieved", metrics=metrics)
            return metrics
        return {}
    
    def detect_anomalies(self, threshold_multiplier: float = 2.0) -> pd.DataFrame:
        """Detect order anomalies based on statistical thresholds."""
        query = """
        WITH order_stats AS (
            SELECT 
                AVG(quantity * price) as avg_order_value,
                STDDEV(quantity * price) as stddev_order_value,
                AVG(quantity) as avg_quantity,
                STDDEV(quantity) as stddev_quantity
            FROM orders 
            WHERE status != 'cancelled'
            AND order_date >= CURRENT_DATE - INTERVAL '7 days'
        )
        SELECT 
            o.id,
            o.customer_id,
            o.product_id,
            o.quantity,
            o.price,
            o.quantity * o.price as order_value,
            o.order_date,
            CASE 
                WHEN o.quantity * o.price > s.avg_order_value + (:threshold * s.stddev_order_value) 
                THEN 'High Value'
                WHEN o.quantity > s.avg_quantity + (:threshold * s.stddev_quantity)
                THEN 'High Quantity'
                WHEN o.quantity * o.price < s.avg_order_value - (:threshold * s.stddev_order_value) 
                THEN 'Low Value'
                ELSE 'Normal'
            END as anomaly_type
        FROM orders o, order_stats s
        WHERE o.status != 'cancelled'
        AND o.order_date >= CURRENT_DATE - INTERVAL '1 day'
        AND (
            o.quantity * o.price > s.avg_order_value + (:threshold * s.stddev_order_value) OR
            o.quantity > s.avg_quantity + (:threshold * s.stddev_quantity) OR
            o.quantity * o.price < s.avg_order_value - (:threshold * s.stddev_order_value)
        )
        ORDER BY o.order_date DESC
        """
        
        result = self.execute_query(query, {'threshold': threshold_multiplier})
        logger.info("Anomaly detection completed", 
                   anomalies_found=len(result),
                   threshold=threshold_multiplier)
        return result
    
    def generate_business_insights(self) -> Dict[str, Any]:
        """Generate comprehensive business insights."""
        insights = {}
        
        try:
            # Get summary stats
            insights['summary'] = self.get_order_summary_stats()
            
            # Top performers
            insights['top_customers'] = self.get_top_customers(5).to_dict('records')
            insights['top_products'] = self.get_product_performance(5).to_dict('records')
            
            # Trends
            daily_trends = self.get_daily_order_trends(7)
            if not daily_trends.empty:
                insights['recent_trend'] = {
                    'daily_avg_orders': daily_trends['daily_orders'].mean(),
                    'daily_avg_revenue': daily_trends['daily_revenue'].mean(),
                    'growth_rate': self._calculate_growth_rate(daily_trends['daily_revenue'])
                }
            
            # Status distribution
            status_dist = self.get_order_status_distribution()
            insights['status_distribution'] = status_dist.to_dict('records')
            
            # Real-time metrics
            insights['real_time'] = self.get_real_time_metrics(15)
            
            # Anomalies
            anomalies = self.detect_anomalies()
            insights['recent_anomalies'] = len(anomalies)
            
            insights['generated_at'] = datetime.utcnow().isoformat()
            logger.info("Business insights generated successfully")
            
        except Exception as e:
            logger.error("Failed to generate insights", error=str(e))
            insights['error'] = str(e)
        
        return insights
    
    def _calculate_growth_rate(self, values: pd.Series) -> float:
        """Calculate simple growth rate from time series."""
        if len(values) < 2:
            return 0.0
        
        try:
            # Simple growth rate: (latest - earliest) / earliest * 100
            latest = values.iloc[-1]
            earliest = values.iloc[0]
            if earliest > 0:
                return ((latest - earliest) / earliest) * 100
        except Exception:
            pass
        
        return 0.0
    
    def export_insights_to_json(self, output_path: str = 'data/analytics/insights.json') -> str:
        """Export business insights to JSON file."""
        try:
            insights = self.generate_business_insights()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(insights, f, indent=2, default=str)
            
            logger.info("Insights exported successfully", output_path=output_path)
            return output_path
            
        except Exception as e:
            logger.error("Failed to export insights", error=str(e))
            raise


class MetricsCollector:
    """Collector for real-time pipeline metrics."""
    
    def __init__(self, analytics_engine: AnalyticsEngine):
        self.analytics = analytics_engine
        logger.info("MetricsCollector initialized")
    
    def collect_pipeline_health_metrics(self) -> Dict[str, Any]:
        """Collect pipeline health and performance metrics."""
        metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'pipeline_status': 'healthy',  # TODO: Implement actual health checks
            'data_freshness': self._check_data_freshness(),
            'throughput': self._calculate_throughput(),
            'error_rate': 0.0,  # TODO: Implement error tracking
            'latency_ms': 0.0   # TODO: Implement latency tracking
        }
        
        return metrics
    
    def _check_data_freshness(self) -> Dict[str, Any]:
        """Check how fresh the data is."""
        try:
            query = """
            SELECT 
                MAX(order_date) as latest_order,
                EXTRACT(EPOCH FROM (NOW() - MAX(order_date))) as seconds_since_last
            FROM orders
            """
            result = self.analytics.execute_query(query)
            
            if not result.empty:
                return {
                    'latest_order': result.iloc[0]['latest_order'],
                    'seconds_since_last': result.iloc[0]['seconds_since_last'],
                    'status': 'fresh' if result.iloc[0]['seconds_since_last'] < 300 else 'stale'
                }
        except Exception as e:
            logger.error("Failed to check data freshness", error=str(e))
        
        return {'status': 'unknown'}
    
    def _calculate_throughput(self) -> Dict[str, Any]:
        """Calculate recent throughput metrics."""
        try:
            query = """
            SELECT 
                COUNT(*) as orders_last_5min
            FROM orders 
            WHERE order_date >= NOW() - INTERVAL '5 minutes'
            """
            result = self.analytics.execute_query(query)
            
            if not result.empty:
                orders_5min = result.iloc[0]['orders_last_5min']
                return {
                    'orders_per_5min': orders_5min,
                    'orders_per_hour_estimate': orders_5min * 12,
                    'orders_per_day_estimate': orders_5min * 288
                }
        except Exception as e:
            logger.error("Failed to calculate throughput", error=str(e))
        
        return {'status': 'unknown'}


def create_analytics_engine(db_config: Optional[Dict[str, Any]] = None) -> AnalyticsEngine:
    """Factory function to create an AnalyticsEngine instance."""
    return AnalyticsEngine(db_config)


if __name__ == "__main__":
    # Example usage
    analytics = create_analytics_engine()
    
    print("=== Order Summary Stats ===")
    summary = analytics.get_order_summary_stats()
    print(json.dumps(summary, indent=2, default=str))
    
    print("\\n=== Top 5 Customers ===")
    top_customers = analytics.get_top_customers(5)
    print(top_customers.to_string())
    
    print("\\n=== Business Insights ===")
    insights = analytics.generate_business_insights()
    print(json.dumps(insights, indent=2, default=str))