
bind = "0.0.0.0:8000"
workers = 4  # Adjust based on CPU cores
threads = 1  # !!!KEEP THIS AS 1 for blocking operations
worker_class = "gthread"  # Use 'sync' for pure blocking, 'gthread' for lightweight concurrency

loglevel = "info"
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr

timeout = 60      # Worker timeout in seconds
keepalive = 2     # Keep-alive for HTTP connections
graceful_timeout = 30  # Graceful shutdown timeout
worker_connections = 1000  # Maximum concurrent requests per worker

# Handle client disconnects gracefully
max_requests = 1000       # Restart workers after this many requests (prevents memory leaks)
max_requests_jitter = 50  # Add randomness to worker restart
preload_app = False       # Don't preload to allow per-worker cleanup

disable_redirect_access_to_syslog = True


### For TLS support, uncomment and set certfile and keyfile paths below. 
### As well as change bind to use :8443.
# certfile = "/certs/server.crt"
# keyfile = "/certs/server.key"
