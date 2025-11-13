"""
Apache Beam Data Processing Component
Alternative processing framework for real-time ETL pipeline.
Consumes from Kafka, transforms data, and loads to warehouse using Apache Beam.
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.transforms import window
# Note: ReadFromKafka requires additional setup for Kafka IO

import structlog

logger = structlog.get_logger()


class CDCEventSchema:
    """Schema definitions for CDC events."""
    
    @staticmethod
    def get_order_schema() -> Dict[str, Any]:
        """Get order event schema."""
        return {
            "id": int,
            "customer_id": int,
            "product_id": str,
            "quantity": int,
            "price": float,
            "order_date": str,
            "status": str
        }


class BeamProcessor:
    """Apache Beam-based ETL processor for real-time CDC data."""

    def __init__(self, kafka_config: Optional[Dict[str, Any]] = None):
        """Initialize Beam processor with Kafka configuration."""
        self.kafka_config = kafka_config or {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'orders_db.public.orders',
            'consumer_group': 'beam-processor-group'
        }
        
        # Pipeline options
        self.pipeline_options = PipelineOptions([
            '--runner=DirectRunner',  # Use DirectRunner for local development
            '--streaming=true',       # Enable streaming mode
            '--project=aladia-etl',
            '--job_name=beam-cdc-processor',
        ])
        
        logger.info("BeamProcessor initialized", 
                   kafka_config=self.kafka_config)

    def parse_cdc_event(self, kafka_message: bytes) -> Optional[Dict[str, Any]]:
        """Parse CDC event from Kafka message."""
        try:
            # Decode Kafka message
            message_str = kafka_message.decode('utf-8')
            event = json.loads(message_str)
            
            # Extract payload from Debezium CDC format
            if 'payload' not in event:
                logger.warning("No payload in CDC event", event=event)
                return None
                
            payload = event['payload']
            
            # Handle different CDC operations
            operation = payload.get('op', 'unknown')
            
            if operation in ['c', 'u']:  # Create or Update
                after_data = payload.get('after', {})
                if after_data:
                    # Add metadata
                    after_data['_cdc_operation'] = operation
                    after_data['_cdc_timestamp'] = datetime.utcnow().isoformat()
                    after_data['_processed_at'] = datetime.utcnow().isoformat()
                    
                    logger.debug("Parsed CDC event", 
                               operation=operation,
                               record_id=after_data.get('id'))
                    return after_data
                    
            elif operation == 'd':  # Delete
                before_data = payload.get('before', {})
                if before_data:
                    before_data['_cdc_operation'] = operation
                    before_data['_cdc_timestamp'] = datetime.utcnow().isoformat()
                    before_data['_processed_at'] = datetime.utcnow().isoformat()
                    before_data['_deleted'] = True
                    
                    logger.debug("Parsed CDC delete event", 
                               record_id=before_data.get('id'))
                    return before_data
                    
            logger.warning("Unsupported CDC operation", operation=operation)
            return None
            
        except Exception as e:
            logger.error("Failed to parse CDC event", 
                        error=str(e), 
                        message=kafka_message[:100])
            return None

    def validate_and_enrich(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and enrich the parsed record."""
        try:
            # Basic validation
            required_fields = ['id', 'customer_id', 'product_id']
            for field in required_fields:
                if field not in record or record[field] is None:
                    logger.warning("Missing required field", 
                                 field=field, 
                                 record_id=record.get('id'))
                    return None
            
            # Data type validation and conversion
            if 'price' in record:
                try:
                    record['price'] = float(record['price'])
                except (ValueError, TypeError):
                    logger.warning("Invalid price format", 
                                 price=record.get('price'),
                                 record_id=record.get('id'))
                    record['price'] = 0.0
            
            if 'quantity' in record:
                try:
                    record['quantity'] = int(record['quantity'])
                except (ValueError, TypeError):
                    logger.warning("Invalid quantity format", 
                                 quantity=record.get('quantity'),
                                 record_id=record.get('id'))
                    record['quantity'] = 1
            
            # Business logic enrichment
            if 'price' in record and 'quantity' in record:
                record['total_amount'] = record['price'] * record['quantity']
            
            # Add processing metadata
            record['_beam_processed'] = True
            record['_pipeline_version'] = '1.0.0'
            
            logger.debug("Record validated and enriched", 
                        record_id=record.get('id'))
            return record
            
        except Exception as e:
            logger.error("Failed to validate/enrich record", 
                        error=str(e), 
                        record=record)
            return None

    def create_pipeline(self) -> beam.Pipeline:
        """Create the Apache Beam processing pipeline."""
        
        # Custom DoFn for parsing CDC events
        class ParseCDCEventFn(beam.DoFn):
            def __init__(self, processor):
                self.processor = processor
            
            def process(self, element):
                parsed = self.processor.parse_cdc_event(element)
                if parsed:
                    yield parsed
        
        # Custom DoFn for validation and enrichment
        class ValidateEnrichFn(beam.DoFn):
            def __init__(self, processor):
                self.processor = processor
            
            def process(self, element):
                validated = self.processor.validate_and_enrich(element)
                if validated:
                    yield validated
        
        # Custom DoFn for formatting output
        class FormatOutputFn(beam.DoFn):
            def process(self, element):
                # Convert to JSON string for output
                yield json.dumps(element, default=str)
        
        # Create pipeline
        pipeline = beam.Pipeline(options=self.pipeline_options)
        
        # Note: Kafka streaming requires additional Beam Kafka IO setup
        # For demo purposes, we'll use batch processing with sample data
        logger.warning("Kafka streaming mode requires additional Beam IO setup")
        logger.info("Use run_batch_pipeline() method for testing Beam functionality")
        
        # Placeholder pipeline for streaming (would need Kafka IO connector)
        (pipeline
         | 'Create Placeholder' >> beam.Create(['Streaming mode requires Kafka IO setup'])
         | 'Write Message' >> WriteToText(
             'data/processed/beam_streaming_info',
             file_name_suffix='.txt',
             num_shards=1))
        
        return pipeline

    def run_streaming_pipeline(self):
        """Run the streaming pipeline."""
        logger.info("Starting Apache Beam streaming pipeline")
        
        try:
            pipeline = self.create_pipeline()
            result = pipeline.run()
            
            logger.info("Beam pipeline started successfully")
            
            # For streaming pipelines, this will run indefinitely
            if hasattr(result, 'wait_until_finish'):
                result.wait_until_finish()
                
        except Exception as e:
            logger.error("Failed to run Beam pipeline", error=str(e))
            raise

    def run_batch_pipeline(self, output_path: str = 'data/processed/beam_batch_output'):
        """Run a batch processing pipeline for testing."""
        logger.info("Starting Apache Beam batch pipeline", output_path=output_path)
        
        try:
            # Modify pipeline options for batch processing
            batch_options = PipelineOptions([
                '--runner=DirectRunner',
                '--project=aladia-etl',
                '--job_name=beam-cdc-batch-processor',
            ])
            
            pipeline = beam.Pipeline(options=batch_options)
            
            # For batch, we'll simulate reading from a file instead of Kafka
            # This is useful for testing and development
            
            sample_events = [
                json.dumps({
                    "payload": {
                        "op": "c",
                        "after": {
                            "id": 1001,
                            "customer_id": 1,
                            "product_id": "PROD001",
                            "quantity": 2,
                            "price": 99.99,
                            "order_date": "2024-01-15",
                            "status": "pending"
                        }
                    }
                }).encode('utf-8'),
                json.dumps({
                    "payload": {
                        "op": "u",
                        "after": {
                            "id": 1001,
                            "customer_id": 1,
                            "product_id": "PROD001",
                            "quantity": 2,
                            "price": 99.99,
                            "order_date": "2024-01-15",
                            "status": "confirmed"
                        }
                    }
                }).encode('utf-8')
            ]
            
            # Custom DoFn classes (same as above)
            class ParseCDCEventFn(beam.DoFn):
                def __init__(self, processor):
                    self.processor = processor
                
                def process(self, element):
                    parsed = self.processor.parse_cdc_event(element)
                    if parsed:
                        yield parsed
            
            class ValidateEnrichFn(beam.DoFn):
                def __init__(self, processor):
                    self.processor = processor
                
                def process(self, element):
                    validated = self.processor.validate_and_enrich(element)
                    if validated:
                        yield validated
            
            class FormatOutputFn(beam.DoFn):
                def process(self, element):
                    yield json.dumps(element, default=str)
            
            # Define batch pipeline
            (pipeline
             | 'Create Sample Events' >> beam.Create(sample_events)
             | 'Parse CDC Events' >> beam.ParDo(ParseCDCEventFn(self))
             | 'Validate and Enrich' >> beam.ParDo(ValidateEnrichFn(self))
             | 'Format Output' >> beam.ParDo(FormatOutputFn())
             | 'Write Results' >> WriteToText(
                 output_path,
                 file_name_suffix='.json',
                 num_shards=1))
            
            result = pipeline.run()
            result.wait_until_finish()
            
            logger.info("Beam batch pipeline completed successfully")
            
        except Exception as e:
            logger.error("Failed to run Beam batch pipeline", error=str(e))
            raise


def create_beam_processor(kafka_config: Optional[Dict[str, Any]] = None) -> BeamProcessor:
    """Factory function to create a BeamProcessor instance."""
    return BeamProcessor(kafka_config)


if __name__ == "__main__":
    # Example usage
    import sys
    
    processor = create_beam_processor()
    
    if len(sys.argv) > 1 and sys.argv[1] == "batch":
        # Run batch pipeline for testing
        processor.run_batch_pipeline()
    else:
        # Run streaming pipeline
        processor.run_streaming_pipeline()