#!/usr/bin/env python3
"""
Test script for CDC Spark Streaming processor.
Tests end-to-end CDC flow: PostgreSQL -> Kafka -> Spark Streaming.
"""

import sys
import os
import time
import threading
import signal
sys.path.append('/app')

from src.streaming import CDCSparkProcessor
import structlog

logger = structlog.get_logger()


def test_cdc_streaming():
    """Test CDC streaming processing with live data."""
    logger.info("=== Starting CDC Streaming Test ===")
    
    # Initialize processor
    processor = CDCSparkProcessor()
    streaming_query = None
    
    try:
        # Start streaming processing
        logger.info("Starting Spark Streaming for CDC events...")
        streaming_query = processor.start_streaming(trigger_interval="5 seconds")
        
        logger.info("Streaming query started successfully!")
        logger.info("Query ID: " + str(streaming_query.id))
        
        # Let it run for a bit to process events
        logger.info("Processing CDC events for 60 seconds...")
        time.sleep(60)
        
        # Check if query is still active
        if streaming_query.isActive:
            logger.info("Streaming query is active and processing events")
            
            # Show query status
            progress = streaming_query.lastProgress
            if progress:
                logger.info("Processing progress", 
                          batch_id=progress.get('batchId'),
                          num_input_rows=progress.get('inputRowsPerSecond', 0),
                          processing_time=progress.get('durationMs', {}).get('triggerExecution', 0))
            
        else:
            logger.warning("Streaming query is not active")
            
    except Exception as e:
        logger.error("CDC streaming test failed", error=str(e))
        
    finally:
        if streaming_query:
            logger.info("Stopping streaming query...")
            streaming_query.stop()
        
        processor.stop_processing()
        logger.info("CDC streaming test completed")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    logger.info("Received interrupt signal, shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, signal_handler)
    
    test_cdc_streaming()