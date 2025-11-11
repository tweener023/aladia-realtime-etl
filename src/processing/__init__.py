"""
PySpark Data Processing Component
Consumes from Kafka, transforms data, and loads to warehouse.
"""

import os
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)

import structlog

logger = structlog.get_logger()


class ETLProcessor:
    """PySpark-based ETL processor for real-time data."""

    def __init__(self):
        """Initialize Spark session."""
        self.spark = (
            SparkSession.builder.appName("aladia-etl-processor")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

    def process_cdc_stream(self, max_batches: int = 10) -> Dict:
        """Process CDC events from Kafka stream."""
        try:
            # Define schema for order events
            order_schema = StructType(
                [
                    StructField("order_id", IntegerType(), True),
                    StructField("customer_id", IntegerType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("unit_price", DecimalType(10, 2), True),
                    StructField("order_date", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("operation", StringType(), True),
                ]
            )

            # Read from Kafka
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                )
                .option("subscribe", "orders-db.public.orders")
                .option("startingOffsets", "latest")
                .load()
            )

            # Parse JSON and transform
            processed_df = (
                kafka_df.select(from_json(col("value").cast("string"), order_schema).alias("data"))
                .select("data.*")
                .withColumn("processed_at", lit("current_timestamp"))
            )

            # Write to console for demo (in production, write to warehouse)
            query = (
                processed_df.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .trigger(processingTime="10 seconds")
                .start()
            )

            # Wait for a specified number of batches
            for _ in range(max_batches):
                if query.isActive:
                    query.awaitTermination(timeout=15)
                else:
                    break

            query.stop()

            return {"status": "completed", "batches_processed": max_batches}

        except Exception as e:
            logger.error("Failed to process CDC stream", error=str(e))
            return {"status": "failed", "error": str(e)}

    def health_check(self) -> Dict:
        """Check Spark processor health."""
        try:
            # Test basic Spark functionality
            test_df = self.spark.range(1, 10)
            count = test_df.count()

            return {"status": "healthy", "spark_version": self.spark.version, "test_count": count}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
