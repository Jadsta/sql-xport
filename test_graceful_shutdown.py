#!/usr/bin/env python3
"""
Simple test script to verify graceful shutdown works correctly.
Run this and then send SIGTERM or press Ctrl+C to test.
"""

import time
import logging
from sql_exporter import graceful_shutdown, signal_handler
import signal

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

def main():
    logging.info("Starting graceful shutdown test...")
    logging.info("Press Ctrl+C or send SIGTERM to test graceful shutdown")
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Simulate long-running process
        for i in range(100):
            logging.info(f"Working... iteration {i}")
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt")
        graceful_shutdown()

if __name__ == "__main__":
    main()