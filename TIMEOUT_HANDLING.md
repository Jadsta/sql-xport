# Timeout and Client Disconnect Handling

This SQL exporter implements multiple layers of timeout protection to handle cases where Prometheus disconnects mid-scrape or queries get stuck.

## Timeout Layers (Defense in Depth)

### 1. Teradata Request Timeout (Database Level)
```yaml
data_sources:
  teradata_main:
    request_timeout: 15  # Teradata native query timeout
```
- **What:** Database server enforces query timeout
- **When:** Individual SQL queries that run too long
- **Action:** Database cancels query and raises exception
- **Benefit:** Most reliable, server-side enforcement

### 2. Flask Scrape Timeout (Application Level) 
```yaml
global:
  scrape_timeout: 30  # Overall scrape operation timeout
```
- **What:** ThreadPoolExecutor timeout on entire scrape operation
- **When:** Total scrape time exceeds limit
- **Action:** Returns HTTP 504 Gateway Timeout
- **Benefit:** Prevents infinite scrape operations

### 3. Gunicorn Worker Timeout (Process Level)
```python
timeout = 45  # Worker timeout in gunicorn_config.py
```
- **What:** Master process kills stuck worker processes
- **When:** Worker doesn't respond for 45 seconds
- **Action:** SIGTERM -> SIGKILL worker, start new worker
- **Benefit:** Handles completely stuck Python processes

### 4. Client Disconnect Detection (Request Level)
```python
check_client_connected()  # During query processing
```
- **What:** Checks if Flask request context is still valid
- **When:** Between query completions in ThreadPoolExecutor
- **Action:** Cancels remaining queries early
- **Benefit:** Stops work when client disconnected

## Timeline Example

When Prometheus dies mid-scrape:

```
T=0s    Prometheus starts scrape request
T=5s    Prometheus process killed (docker stop, etc.)
T=6s    Client disconnect detection triggers (between queries)
T=6s    Remaining queries cancelled, resources cleaned up
T=7s    HTTP response attempted (but client gone)
```

Without disconnect detection:
```
T=0s    Prometheus starts scrape request  
T=5s    Prometheus process killed
T=15s   Individual queries still timeout (Teradata request_timeout)
T=30s   Overall scrape times out (scrape_timeout) 
T=45s   Worker timeout kills the worker process (last resort)
```

## Configuration Guidelines

### Recommended Timeout Hierarchy
```
request_timeout (15s) < scrape_timeout (30s) < worker_timeout (45s)
```

### For Fast Queries (< 5s typical)
```yaml
global:
  scrape_timeout: 20
  request_timeout: 10

# gunicorn_config.py
timeout = 30
```

### For Slow Queries (10-30s typical)
```yaml
global: 
  scrape_timeout: 60
  request_timeout: 45

# gunicorn_config.py  
timeout = 75
```

### For Very Slow Analytics (minutes)
```yaml
global:
  scrape_timeout: 300  # 5 minutes
  request_timeout: 240 # 4 minutes

# gunicorn_config.py
timeout = 360  # 6 minutes
```

## Testing Client Disconnects

### Test Locally
```powershell
# Start exporter
python sql_exporter.py

# Start a scrape, then Ctrl+C curl quickly
curl http://localhost:8000/metrics?exporter=your_exporter
# Press Ctrl+C while scrape is running
```

### Test with Docker
```bash
# Start container with metrics
docker run -p 8000:8000 sql-exporter

# In another terminal, start scrape and kill it
curl http://localhost:8000/metrics &
PID=$!
sleep 2
kill $PID

# Check exporter logs for disconnect detection
docker logs sql-exporter
```

### Test Worker Timeout
```bash
# Temporarily set very low worker timeout for testing
# Edit gunicorn_config.py: timeout = 5

# Start long scrape that will exceed worker timeout
curl http://localhost:8000/metrics?exporter=slow_exporter
# Should see worker timeout and restart in logs
```

## Log Messages to Watch For

### Normal Operation
```
[INFO] Scrape completed in 12.345 seconds
```

### Client Disconnect Detected
```
[WARNING] Client appears to be disconnected - stopping query processing
[INFO] Cancelled 3 remaining queries due to client disconnect
```

### Timeout Scenarios
```
[ERROR] Scrape exceeded timeout of 30 seconds  
[WARNING] Worker 1234 timed out - likely client disconnect or stuck query
[ERROR] Query execution failed: request timeout exceeded
```

### Graceful Shutdown
```
[INFO] Received SIGTERM - initiating graceful shutdown...
[INFO] Closed 8 connections for 'teradata_main'
```

## Benefits

1. **Resource Protection** - Prevents runaway queries
2. **Fast Failure** - Detects client disconnects quickly  
3. **Graceful Degradation** - Multiple timeout layers
4. **Resource Cleanup** - Connections properly closed
5. **Observability** - Clear logging of timeout events