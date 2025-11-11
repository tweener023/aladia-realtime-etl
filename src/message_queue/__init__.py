"""
Message Queue Component
Handles Kafka producer and consumer operations for the ETL pipeline.
"""

import os
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import dataclass

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import structlog

logger = structlog.get_logger()


@dataclass
class ETLMessage:
    """Standardized message format for ETL pipeline."""

    topic: str
    key: str
    value: Dict[str, Any]
    timestamp: datetime
    source: str
    operation: str  # INSERT, UPDATE, DELETE

    def to_json(self) -> str:
        """Convert message to JSON string."""
        return json.dumps(
            {
                "topic": self.topic,
                "key": self.key,
                "value": self.value,
                "timestamp": self.timestamp.isoformat(),
                "source": self.source,
                "operation": self.operation,
            }
        )

    @classmethod
    def from_cdc_event(cls, cdc_event: Dict) -> "ETLMessage":
        """Create ETLMessage from Debezium CDC event."""
        # Extract operation type from CDC event
        operation = "INSERT"
        if cdc_event.get("__op") == "u":
            operation = "UPDATE"
        elif cdc_event.get("__op") == "d":
            operation = "DELETE"

        # Create standardized message
        return cls(
            topic="cdc-events",
            key=str(cdc_event.get("order_id", "")),
            value=cdc_event,
            timestamp=datetime.utcnow(),
            source="orders-db",
            operation=operation,
        )


class KafkaMessageProducer:
    """Kafka message producer for ETL pipeline."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """Initialize Kafka producer."""
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas to acknowledge
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
        )

    def send_message(self, topic: str, message: ETLMessage) -> bool:
        """Send a message to Kafka topic."""
        try:
            future = self.producer.send(
                topic,
                key=message.key,
                value={
                    "key": message.key,
                    "value": message.value,
                    "timestamp": message.timestamp.isoformat(),
                    "source": message.source,
                    "operation": message.operation,
                },
            )

            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)

            logger.info(
                "Message sent successfully",
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                key=message.key,
            )
            return True

        except KafkaError as e:
            logger.error(
                "Failed to send message to Kafka", topic=topic, key=message.key, error=str(e)
            )
            return False
        except Exception as e:
            logger.error(
                "Unexpected error sending message", topic=topic, key=message.key, error=str(e)
            )
            return False

    def health_check(self) -> Dict:
        """Check if Kafka producer is healthy."""
        try:
            # Try to get metadata for available topics
            metadata = self.producer.list_topics(timeout=5)
            return {
                "status": "healthy",
                "topics_count": len(metadata.topics),
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error("Kafka producer health check failed", error=str(e))
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()


class KafkaMessageConsumer:
    """Kafka message consumer for ETL pipeline."""

    def __init__(
        self,
        topics: List[str],
        group_id: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
    ):
        """Initialize Kafka consumer."""
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.group_id = group_id or os.getenv("KAFKA_CONSUMER_GROUP", "etl-processors")
        self.topics = topics

        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit for better control
            max_poll_records=100,
            consumer_timeout_ms=10000,
        )

    def consume_messages(
        self, message_handler: Callable[[Dict], bool], max_messages: Optional[int] = None
    ) -> int:
        """
        Consume messages and process them with the provided handler.
        Returns the number of messages processed.
        """
        processed_count = 0

        try:
            for message in self.consumer:
                try:
                    # Process the message
                    success = message_handler(message.value)

                    if success:
                        # Commit the offset only if processing was successful
                        self.consumer.commit()
                        processed_count += 1

                        logger.info(
                            "Message processed successfully",
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                            key=message.key,
                        )
                    else:
                        logger.error(
                            "Message processing failed",
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                            key=message.key,
                        )

                except Exception as e:
                    logger.error(
                        "Error processing message",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        error=str(e),
                    )

                # Check if we've reached the max message limit
                if max_messages and processed_count >= max_messages:
                    break

        except Exception as e:
            logger.error("Error in message consumption loop", error=str(e))

        return processed_count

    def health_check(self) -> Dict:
        """Check if Kafka consumer is healthy."""
        try:
            # Check if we can get topic metadata
            partitions = self.consumer.partitions_for_topic(self.topics[0])
            return {
                "status": "healthy",
                "subscribed_topics": self.topics,
                "partitions_count": len(partitions) if partitions else 0,
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error("Kafka consumer health check failed", error=str(e))
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def close(self):
        """Close the consumer connection."""
        if self.consumer:
            self.consumer.close()


class MessageQueueManager:
    """High-level manager for message queue operations."""

    def __init__(self):
        """Initialize message queue manager."""
        self.producer = KafkaMessageProducer()

    def publish_cdc_events(self, cdc_events: List[Dict]) -> int:
        """Publish CDC events to the message queue."""
        published_count = 0

        for event in cdc_events:
            try:
                # Convert CDC event to standardized message
                message = ETLMessage.from_cdc_event(event)

                # Send to Kafka
                success = self.producer.send_message("cdc-events", message)
                if success:
                    published_count += 1

            except Exception as e:
                logger.error("Failed to publish CDC event", event=event, error=str(e))

        logger.info("Published CDC events", total=len(cdc_events), published=published_count)
        return published_count

    def create_consumer(self, topics: List[str]) -> KafkaMessageConsumer:
        """Create a new consumer for specified topics."""
        return KafkaMessageConsumer(topics)

    def health_check(self) -> Dict:
        """Check health of message queue components."""
        producer_health = self.producer.health_check()

        # Test consumer creation
        consumer_health = {"status": "healthy"}
        try:
            test_consumer = KafkaMessageConsumer(["test-topic"])
            consumer_health = test_consumer.health_check()
            test_consumer.close()
        except Exception as e:
            consumer_health = {"status": "unhealthy", "error": str(e)}

        overall_status = (
            "healthy"
            if (producer_health["status"] == "healthy" and consumer_health["status"] == "healthy")
            else "unhealthy"
        )

        return {
            "status": overall_status,
            "producer": producer_health,
            "consumer": consumer_health,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def close(self):
        """Close all connections."""
        if self.producer:
            self.producer.close()


def main():
    """CLI interface for message queue management."""
    import click
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    @click.group()
    def cli():
        """Message Queue Management CLI"""
        pass

    @cli.command()
    def health():
        """Check message queue health."""
        mq = MessageQueueManager()
        result = mq.health_check()
        click.echo(json.dumps(result, indent=2))
        mq.close()

    @cli.command()
    @click.argument("topic")
    @click.option("--count", default=10, help="Number of test messages to send")
    def test_produce(topic, count):
        """Send test messages to a topic."""
        producer = KafkaMessageProducer()

        for i in range(count):
            message = ETLMessage(
                topic=topic,
                key=f"test-key-{i}",
                value={"test_id": i, "message": f"Test message {i}"},
                timestamp=datetime.utcnow(),
                source="test-producer",
                operation="INSERT",
            )

            success = producer.send_message(topic, message)
            click.echo(f"Message {i}: {'SUCCESS' if success else 'FAILED'}")

        producer.close()
        click.echo(f"Sent {count} test messages to topic {topic}")

    @cli.command()
    @click.argument("topic")
    @click.option("--max-messages", default=10, help="Maximum messages to consume")
    def test_consume(topic, max_messages):
        """Consume test messages from a topic."""
        consumer = KafkaMessageConsumer([topic])

        def message_handler(message: Dict) -> bool:
            click.echo(f"Received: {json.dumps(message, indent=2)}")
            return True

        processed = consumer.consume_messages(message_handler, max_messages)
        consumer.close()

        click.echo(f"Processed {processed} messages from topic {topic}")

    cli()


if __name__ == "__main__":
    main()
