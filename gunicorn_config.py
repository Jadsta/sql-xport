
bind = "0.0.0.0:8000"
workers = 4  # Adjust based on CPU cores
threads = 1  # !!!KEEP THIS AS 1 for blocking operations
worker_class = "gthread"  # Use 'sync' for pure blocking, 'gthread' for lightweight concurrency

loglevel = "info"
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr

# Timeout settings for client disconnects and stuck requests
timeout = 45      # Worker timeout - kills workers stuck on requests (should be > max scrape time)
keepalive = 2     # Keep-alive for HTTP connections  
graceful_timeout = 30  # Graceful shutdown timeout
worker_connections = 1000  # Maximum concurrent requests per worker

# Handle client disconnects gracefully
max_requests = 500        # Restart workers after this many requests (prevents memory leaks)
max_requests_jitter = 50  # Add randomness to worker restart
preload_app = False       # Don't preload to allow per-worker cleanup

# Enable proper signal handling for Docker
enable_stdio_inheritance = True

# Custom worker timeout handling
def worker_timeout(worker):
    """Called when a worker times out (client disconnect or stuck request)."""
    import logging
    logging.warning(f"Worker {worker.pid} timed out - likely client disconnect or stuck query")

disable_redirect_access_to_syslog = True


### For TLS support, uncomment and set certfile and keyfile paths below. 
### As well as change bind to use :8443.
# certfile = "/certs/server.crt"
# keyfile = "/certs/server.key"

def worker_exit(server, worker):
    """Called when a worker is exiting (for cleanup)."""
    import logging
    logging.info(f"Worker {worker.pid} exiting - cleanup initiated")

def on_exit(server):
    """Called when the master process is exiting."""
    import logging
    logging.info("Gunicorn master process exiting")
