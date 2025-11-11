"""
Spark Streaming CDC Processor
Processes Change Data Capture events from Kafka in real-time.
"""

import os
import json
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
import structlog

logger = structlog.get_logger()


class CDCSparkProcessor:
    """Spark Streaming processor for CDC events from Kafka."""

    def __init__(self, kafka_bootstrap_servers: str = "kafka:29092"):
        """Initialize Spark Streaming CDC processor."""
        self.kafka_servers = kafka_bootstrap_servers
        self.spark = self._create_spark_session()
        self._define_schemas()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Kafka support."""
        return SparkSession.builder \
            .appName("CDC-Streaming-Processor") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoint") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

    def _define_schemas(self):
        """Define schemas for CDC event parsing."""
        # Schema for CDC event after/before objects
        self.order_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("total_amount", StringType(), True),  # Base64 encoded decimal
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])

        # Schema for CDC source metadata
        self.source_schema = StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("txId", IntegerType(), True),
            StructField("lsn", IntegerType(), True)
        ])

        # Schema for complete CDC event
        self.cdc_schema = StructType([
            StructField("before", self.order_schema, True),
            StructField("after", self.order_schema, True),
            StructField("source", self.source_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("transaction", StringType(), True)
        ])

    def read_cdc_stream(self, topic: str = "orders_db.public.orders") -> DataFrame:
        """Read CDC events from Kafka as a streaming DataFrame."""
        logger.info("Starting CDC stream processing", topic=topic)

        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON CDC events
        parsed_df = kafka_df \
            .select(from_json(col("value").cast("string"), self.cdc_schema).alias("cdc_event")) \
            .select("cdc_event.*")

        # Add processing timestamp
        enriched_df = parsed_df.withColumn("processed_at", current_timestamp())

        return enriched_df

    def process_cdc_events(self, cdc_df: DataFrame) -> DataFrame:
        """Process and transform CDC events for warehouse loading."""
        
        # Extract relevant fields and handle different operation types
        processed_df = cdc_df.select(
            col("op").alias("operation"),
            col("source.table").alias("source_table"),
            col("source.db").alias("source_database"),
            col("ts_ms").alias("event_timestamp"),
            col("processed_at"),
            
            # For INSERT and UPDATE operations, use 'after' data
            # For DELETE operations, use 'before' data
            when(col("op").isin("c", "u"), col("after")).otherwise(col("before")).alias("record_data"),
            
            # Create change event metadata
            when(col("op") == "c", lit("INSERT"))
            .when(col("op") == "u", lit("UPDATE"))
            .when(col("op") == "d", lit("DELETE"))
            .when(col("op") == "r", lit("SNAPSHOT"))
            .otherwise(lit("UNKNOWN")).alias("change_type")
        )

        # Flatten record data for easier processing
        final_df = processed_df.select(
            col("operation"),
            col("source_table"),
            col("source_database"),
            col("event_timestamp"),
            col("processed_at"),
            col("change_type"),
            col("record_data.id").alias("order_id"),
            col("record_data.customer_name"),
            col("record_data.customer_email"),
            col("record_data.total_amount").alias("total_amount_encoded"),
            col("record_data.status"),
            col("record_data.created_at").alias("order_created_at"),
            col("record_data.updated_at").alias("order_updated_at")
        )

        return final_df

    def start_streaming(self, output_mode: str = "append", trigger_interval: str = "10 seconds") -> None:
        """Start the CDC streaming processing."""
        logger.info("Starting CDC streaming processor")

        try:
            # Read CDC stream
            cdc_stream = self.read_cdc_stream()

            # Process events
            processed_stream = self.process_cdc_events(cdc_stream)

            # Start streaming query with console output for testing
            query = processed_stream.writeStream \
                .outputMode(output_mode) \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime=trigger_interval) \
                .start()

            logger.info("CDC streaming processor started", query_id=query.id)
            
            return query

        except Exception as e:
            logger.error("Failed to start CDC streaming processor", error=str(e))
            raise

    def write_to_warehouse(self, processed_df: DataFrame, warehouse_url: str) -> None:
        """Write processed CDC events to warehouse database."""
        processed_df.writeStream \
            .outputMode("append") \
            .format("jdbc") \
            .option("url", warehouse_url) \
            .option("dbtable", "cdc_events") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .trigger(processingTime="30 seconds") \
            .start()

    def stop_processing(self):
        """Stop Spark session."""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function for testing CDC streaming processor."""
    processor = CDCSparkProcessor()
    
    try:
        # Start streaming processing
        query = processor.start_streaming()
        
        # Keep the streaming query running
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopping CDC processor...")
        query.stop()
        processor.stop_processing()
    except Exception as e:
        logger.error("CDC processing failed", error=str(e))
        processor.stop_processing()
        raise


if __name__ == "__main__":
    main()