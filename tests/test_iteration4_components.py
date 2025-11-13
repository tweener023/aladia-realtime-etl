"""
Unit tests for Apache Beam processor and Analytics engine.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import pandas as pd
from datetime import datetime

from src.processing.beam_processor import BeamProcessor, CDCEventSchema
from src.analytics import AnalyticsEngine, MetricsCollector


class TestBeamProcessor(unittest.TestCase):
    """Test cases for Apache Beam processor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = BeamProcessor()
    
    def test_initialization(self):
        """Test BeamProcessor initialization."""
        self.assertIsNotNone(self.processor.kafka_config)
        self.assertEqual(self.processor.kafka_config['bootstrap_servers'], 'localhost:9092')
        self.assertEqual(self.processor.kafka_config['topic'], 'orders_db.public.orders')
    
    def test_parse_cdc_event_create(self):
        """Test parsing CDC create event."""
        cdc_message = json.dumps({
            "payload": {
                "op": "c",
                "after": {
                    "id": 123,
                    "customer_id": 1,
                    "product_id": "PROD001",
                    "quantity": 2,
                    "price": 99.99,
                    "order_date": "2024-01-15",
                    "status": "pending"
                }
            }
        }).encode('utf-8')
        
        result = self.processor.parse_cdc_event(cdc_message)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 123)
        self.assertEqual(result['_cdc_operation'], 'c')
        self.assertIn('_cdc_timestamp', result)
        self.assertIn('_processed_at', result)
    
    def test_parse_cdc_event_update(self):
        """Test parsing CDC update event."""
        cdc_message = json.dumps({
            "payload": {
                "op": "u",
                "after": {
                    "id": 123,
                    "customer_id": 1,
                    "product_id": "PROD001",
                    "quantity": 3,
                    "price": 99.99,
                    "order_date": "2024-01-15",
                    "status": "confirmed"
                }
            }
        }).encode('utf-8')
        
        result = self.processor.parse_cdc_event(cdc_message)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['quantity'], 3)
        self.assertEqual(result['_cdc_operation'], 'u')
    
    def test_parse_cdc_event_delete(self):
        """Test parsing CDC delete event."""
        cdc_message = json.dumps({
            "payload": {
                "op": "d",
                "before": {
                    "id": 123,
                    "customer_id": 1,
                    "product_id": "PROD001"
                }
            }
        }).encode('utf-8')
        
        result = self.processor.parse_cdc_event(cdc_message)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 123)
        self.assertEqual(result['_cdc_operation'], 'd')
        self.assertTrue(result['_deleted'])
    
    def test_parse_cdc_event_invalid(self):
        """Test parsing invalid CDC event."""
        invalid_message = b"invalid json"
        
        result = self.processor.parse_cdc_event(invalid_message)
        
        self.assertIsNone(result)
    
    def test_validate_and_enrich(self):
        """Test record validation and enrichment."""
        record = {
            "id": 123,
            "customer_id": 1,
            "product_id": "PROD001",
            "quantity": 2,
            "price": 99.99
        }
        
        result = self.processor.validate_and_enrich(record)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['total_amount'], 199.98)
        self.assertTrue(result['_beam_processed'])
        self.assertEqual(result['_pipeline_version'], '1.0.0')
    
    def test_validate_and_enrich_missing_field(self):
        """Test validation with missing required field."""
        record = {
            "customer_id": 1,
            "product_id": "PROD001"
            # Missing 'id' field
        }
        
        result = self.processor.validate_and_enrich(record)
        
        self.assertIsNone(result)
    
    def test_validate_and_enrich_invalid_price(self):
        """Test validation with invalid price."""
        record = {
            "id": 123,
            "customer_id": 1,
            "product_id": "PROD001",
            "quantity": 2,
            "price": "invalid"
        }
        
        result = self.processor.validate_and_enrich(record)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['price'], 0.0)  # Should default to 0.0


class TestCDCEventSchema(unittest.TestCase):
    """Test cases for CDC event schema."""
    
    def test_get_order_schema(self):
        """Test order schema definition."""
        schema = CDCEventSchema.get_order_schema()
        
        self.assertIsInstance(schema, dict)
        self.assertIn('id', schema)
        self.assertIn('customer_id', schema)
        self.assertIn('product_id', schema)
        self.assertEqual(schema['id'], int)
        self.assertEqual(schema['customer_id'], int)
        self.assertEqual(schema['product_id'], str)


class TestAnalyticsEngine(unittest.TestCase):
    """Test cases for Analytics engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock database config for testing
        self.db_config = {
            'host': 'test_host',
            'port': 5432,
            'database': 'test_db',
            'username': 'test_user',
            'password': 'test_password'
        }
    
    @patch('src.analytics.create_engine')
    def test_initialization(self, mock_create_engine):
        """Test AnalyticsEngine initialization."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        analytics = AnalyticsEngine(self.db_config)
        
        self.assertEqual(analytics.db_config, self.db_config)
        self.assertEqual(analytics.engine, mock_engine)
        mock_create_engine.assert_called_once()
    
    @patch('src.analytics.create_engine')
    @patch('pandas.read_sql')
    def test_execute_query_success(self, mock_read_sql, mock_create_engine):
        """Test successful query execution."""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        expected_df = pd.DataFrame({'count': [5], 'sum': [100.0]})
        mock_read_sql.return_value = expected_df
        
        analytics = AnalyticsEngine(self.db_config)
        result = analytics.execute_query("SELECT COUNT(*), SUM(amount) FROM orders")
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        mock_read_sql.assert_called_once()
    
    @patch('src.analytics.create_engine')
    def test_execute_query_failure(self, mock_create_engine):
        """Test query execution failure."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Connection failed")
        
        analytics = AnalyticsEngine(self.db_config)
        
        with self.assertRaises(Exception):
            analytics.execute_query("SELECT * FROM orders")
    
    @patch.object(AnalyticsEngine, 'execute_query')
    def test_get_order_summary_stats(self, mock_execute_query):
        """Test order summary statistics retrieval."""
        mock_df = pd.DataFrame({
            'total_orders': [100],
            'unique_customers': [25],
            'total_revenue': [5000.0],
            'avg_price': [50.0]
        })
        mock_execute_query.return_value = mock_df
        
        analytics = AnalyticsEngine(self.db_config)
        result = analytics.get_order_summary_stats()
        
        self.assertEqual(result['total_orders'], 100)
        self.assertEqual(result['unique_customers'], 25)
        self.assertEqual(result['total_revenue'], 5000.0)
    
    @patch.object(AnalyticsEngine, 'execute_query')
    def test_get_top_customers(self, mock_execute_query):
        """Test top customers retrieval."""
        mock_df = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'order_count': [10, 8, 6],
            'total_value': [1000.0, 800.0, 600.0]
        })
        mock_execute_query.return_value = mock_df
        
        analytics = AnalyticsEngine(self.db_config)
        result = analytics.get_top_customers(3)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]['customer_id'], 1)
        self.assertEqual(result.iloc[0]['total_value'], 1000.0)
    
    @patch.object(AnalyticsEngine, 'execute_query')
    def test_get_real_time_metrics(self, mock_execute_query):
        """Test real-time metrics collection."""
        mock_df = pd.DataFrame({
            'recent_orders': [5],
            'active_customers': [3],
            'recent_revenue': [250.0],
            'avg_recent_price': [50.0]
        })
        mock_execute_query.return_value = mock_df
        
        analytics = AnalyticsEngine(self.db_config)
        result = analytics.get_real_time_metrics(5)
        
        self.assertEqual(result['recent_orders'], 5)
        self.assertEqual(result['time_window_minutes'], 5)
        self.assertIn('calculated_at', result)
    
    @patch.object(AnalyticsEngine, 'execute_query')
    def test_detect_anomalies(self, mock_execute_query):
        """Test anomaly detection."""
        mock_df = pd.DataFrame({
            'id': [123, 124],
            'order_value': [1000.0, 2000.0],
            'anomaly_type': ['High Value', 'High Value']
        })
        mock_execute_query.return_value = mock_df
        
        analytics = AnalyticsEngine(self.db_config)
        result = analytics.detect_anomalies(2.0)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['anomaly_type'], 'High Value')


class TestMetricsCollector(unittest.TestCase):
    """Test cases for MetricsCollector."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_analytics = Mock(spec=AnalyticsEngine)
        self.collector = MetricsCollector(self.mock_analytics)
    
    def test_initialization(self):
        """Test MetricsCollector initialization."""
        self.assertEqual(self.collector.analytics, self.mock_analytics)
    
    @patch.object(MetricsCollector, '_check_data_freshness')
    @patch.object(MetricsCollector, '_calculate_throughput')
    def test_collect_pipeline_health_metrics(self, mock_throughput, mock_freshness):
        """Test pipeline health metrics collection."""
        mock_freshness.return_value = {'status': 'fresh'}
        mock_throughput.return_value = {'orders_per_5min': 10}
        
        result = self.collector.collect_pipeline_health_metrics()
        
        self.assertIn('timestamp', result)
        self.assertEqual(result['pipeline_status'], 'healthy')
        self.assertEqual(result['data_freshness']['status'], 'fresh')
        self.assertEqual(result['throughput']['orders_per_5min'], 10)
    
    def test_check_data_freshness_success(self):
        """Test successful data freshness check."""
        mock_df = pd.DataFrame({
            'latest_order': ['2024-01-15 10:00:00'],
            'seconds_since_last': [60]
        })
        self.mock_analytics.execute_query.return_value = mock_df
        
        result = self.collector._check_data_freshness()
        
        self.assertEqual(result['status'], 'fresh')
        self.assertEqual(result['seconds_since_last'], 60)
    
    def test_check_data_freshness_stale(self):
        """Test stale data detection."""
        mock_df = pd.DataFrame({
            'latest_order': ['2024-01-15 10:00:00'],
            'seconds_since_last': [600]  # > 300 seconds
        })
        self.mock_analytics.execute_query.return_value = mock_df
        
        result = self.collector._check_data_freshness()
        
        self.assertEqual(result['status'], 'stale')
    
    def test_calculate_throughput(self):
        """Test throughput calculation."""
        mock_df = pd.DataFrame({
            'orders_last_5min': [15]
        })
        self.mock_analytics.execute_query.return_value = mock_df
        
        result = self.collector._calculate_throughput()
        
        self.assertEqual(result['orders_per_5min'], 15)
        self.assertEqual(result['orders_per_hour_estimate'], 180)  # 15 * 12
        self.assertEqual(result['orders_per_day_estimate'], 4320)  # 15 * 288


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline."""
    
    def test_beam_processor_factory(self):
        """Test BeamProcessor factory function."""
        from src.processing.beam_processor import create_beam_processor
        
        processor = create_beam_processor()
        self.assertIsInstance(processor, BeamProcessor)
    
    def test_analytics_engine_factory(self):
        """Test AnalyticsEngine factory function."""
        from src.analytics import create_analytics_engine
        
        analytics = create_analytics_engine()
        self.assertIsInstance(analytics, AnalyticsEngine)
    
    def test_end_to_end_cdc_processing(self):
        """Test end-to-end CDC event processing."""
        processor = BeamProcessor()
        
        # Simulate CDC event
        cdc_event = json.dumps({
            "payload": {
                "op": "c",
                "after": {
                    "id": 999,
                    "customer_id": 123,
                    "product_id": "TEST001",
                    "quantity": 1,
                    "price": 25.50,
                    "order_date": "2024-01-15",
                    "status": "pending"
                }
            }
        }).encode('utf-8')
        
        # Process through pipeline
        parsed = processor.parse_cdc_event(cdc_event)
        self.assertIsNotNone(parsed)
        
        enriched = processor.validate_and_enrich(parsed)
        self.assertIsNotNone(enriched)
        
        # Verify enrichment
        self.assertEqual(enriched['total_amount'], 25.50)
        self.assertTrue(enriched['_beam_processed'])
        self.assertEqual(enriched['_cdc_operation'], 'c')


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)