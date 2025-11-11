"""
Basic unit tests for ETL pipeline components.
"""

import pytest
from unittest.mock import Mock, patch
from src.source_db import SourceDatabase
from src.cdc import CDCManager
from src.message_queue import MessageQueueManager
from src.warehouse import WarehouseLoader


class TestSourceDatabase:
    """Test source database component."""

    def test_build_connection_string(self):
        """Test connection string building."""
        with patch.dict(
            "os.environ",
            {
                "SOURCE_DB_HOST": "testhost",
                "SOURCE_DB_PORT": "5432",
                "SOURCE_DB_NAME": "testdb",
                "SOURCE_DB_USER": "testuser",
                "SOURCE_DB_PASSWORD": "testpass",
            },
        ):
            db = SourceDatabase()
            expected = "postgresql://testuser:testpass@testhost:5432/testdb"
            assert db._build_connection_string() == expected

    @patch("sqlalchemy.create_engine")
    def test_health_check_healthy(self, mock_create_engine):
        """Test health check when database is healthy."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = [1]

        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result
        mock_create_engine.return_value = mock_engine

        db = SourceDatabase()
        result = db.health_check()

        assert result["status"] == "healthy"
        assert "timestamp" in result


class TestCDCManager:
    """Test CDC manager component."""

    @patch("requests.get")
    def test_health_check_healthy(self, mock_get):
        """Test CDC health check when healthy."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"version": "1.0.0"}
        mock_get.return_value = mock_response

        cdc = CDCManager("http://test:8083")
        result = cdc.health_check()

        assert result["status"] == "healthy"
        assert result["connect_version"] == "1.0.0"

    @patch("requests.get")
    def test_health_check_unhealthy(self, mock_get):
        """Test CDC health check when unhealthy."""
        mock_get.side_effect = Exception("Connection failed")

        cdc = CDCManager("http://test:8083")
        result = cdc.health_check()

        assert result["status"] == "unhealthy"
        assert "Connection failed" in result["error"]


class TestMessageQueueManager:
    """Test message queue manager."""

    @patch("kafka.KafkaProducer")
    def test_producer_creation(self, mock_producer_class):
        """Test Kafka producer creation."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        mq = MessageQueueManager()
        assert mq.producer is not None

    @patch("kafka.KafkaProducer")
    def test_health_check(self, mock_producer_class):
        """Test message queue health check."""
        mock_producer = Mock()
        mock_producer.list_topics.return_value = Mock(topics={"test-topic": None})
        mock_producer_class.return_value = mock_producer

        mq = MessageQueueManager()
        result = mq.health_check()

        assert result["status"] == "healthy"


class TestWarehouseLoader:
    """Test warehouse loader component."""

    def test_build_connection_string(self):
        """Test warehouse connection string building."""
        with patch.dict(
            "os.environ",
            {
                "WAREHOUSE_DB_HOST": "warehouse_host",
                "WAREHOUSE_DB_PORT": "5433",
                "WAREHOUSE_DB_NAME": "warehouse",
                "WAREHOUSE_DB_USER": "warehouse_user",
                "WAREHOUSE_DB_PASSWORD": "warehouse_pass",
            },
        ):
            warehouse = WarehouseLoader()
            expected = "postgresql://warehouse_user:warehouse_pass@warehouse_host:5433/warehouse"
            assert warehouse._build_connection_string() == expected

    @patch("sqlalchemy.create_engine")
    def test_health_check_healthy(self, mock_create_engine):
        """Test warehouse health check when healthy."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = [1]

        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = mock_result
        mock_create_engine.return_value = mock_engine

        warehouse = WarehouseLoader()
        result = warehouse.health_check()

        assert result["status"] == "healthy"
        assert "timestamp" in result