bind = "0.0.0.0:8000"
workers = 4  # Adjust based on CPU cores
threads = 1  # !!!KEEP THIS AS 1 for blocking operations
worker_class = "gthread"  # Use 'sync' for pure blocking, 'gthread' for lightweight concurrency

loglevel = "info"

# Disable HTTP access logs - application already logs scrape operations
# This prevents duplicate/inconsistent log formats
accesslog = None

errorlog = "-"   # Log to stderr

timeout = 60      # Worker timeout in seconds
keepalive = 2     # Keep-alive for HTTP connections

disable_redirect_access_to_syslog = True


### For TLS support, uncomment and set certfile and keyfile paths below. 
### As well as change bind to use :8443.
# certfile = "/certs/server.crt"
# keyfile = "/certs/server.key"