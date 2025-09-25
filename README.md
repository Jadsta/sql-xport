# SQL Exporter for Teradata

## Overview
This project is a Prometheus exporter for Teradata, designed to run in production using Gunicorn. It supports connection pooling, dynamic exporter configuration, and safe concurrent access for multiple Prometheus scrapes.

## Features
- Connection pooling per data source, with limits set in `settings.yml`
- Exporter configuration via individual YAML files in the `targets/` directory
- Blocking/waiting for connections when pool is exhausted
- Gunicorn-ready for production deployment
- Thread-safe connection management
- Timezone support for metric timestamps (configurable per data source and exporter)
- Advanced diagnostics and logging for connection and query issues
- Option to force new connection for each scrape (for debugging)

## File Structure
```
sql_exporter.py         # Main exporter application
settings.yml            # Data source connection settings and pool sizes
targets/
  <exporter>/           # Exporter directories (e.g., test, prod, etc.)
    sql_exporter.yml    # Exporter configuration
    <collector>.yml     # Collector configuration(s)
gunicorn_config.py      # Gunicorn production settings
```

## Configuration
### settings.yml
Define your data sources, connection pool sizes, and timezones:
```yaml
data_sources:
  teradata_main:
    host: "your-teradata-host"
    user: "your-user"
    password: "your-password"
    max_connections: 8  # Maximum pool size for this data source
    timezone: "Australia/Sydney"  # Example: AEST timezone
    # Add other connection settings as needed
  teradata_backup:
    host: "your-backup-host"
    user: "backup-user"
    password: "backup-password"
    max_connections: 4  # Example pool size for backup data source
    timezone: "UTC"     # Example: use UTC for backup
    # Add other connection settings as needed
```

### targets/<exporter>/sql_exporter.yml (example)
Reference your data source and set exporter-specific limits and optional timezone override:
```yaml
global:
  scrape_timeout_offset: 500ms
  max_connections: 2
  max_idle_connections: 1
  max_connection_lifetime: 5m
  timezone: "America/New_York"  # Optional: override timezone for this exporter

target:
  data_source_name: teradata_main
  collectors: [test_metrics*]

collector_files:
  - "test_collector.yml"
```

### gunicorn_config.py
Recommended Gunicorn settings for production:
```python
bind = "0.0.0.0:8000"
workers = 4
worker_class = "gthread"
threads = 4
loglevel = "info"
accesslog = "-"
errorlog = "-"
timeout = 60
keepalive = 2
```

## Running the Exporter
1. Install dependencies:
   ```pwsh
   pip install flask gunicorn teradatasql pyyaml pytz tzlocal
   ```
2. Start Gunicorn:
   ```pwsh
   gunicorn -c gunicorn_config.py sql_exporter:app
   ```
3. Configure Prometheus to scrape metrics from `http://<host>:8000/metrics?exporter=test`

## How Connection Pooling and Timezone Works
- Each data source has a shared pool sized by `max_connections` in `settings.yml`.
- Each exporter can use up to its own `max_connections` (from its yml), but only as many as are available in the pool.
- If all connections are busy, requests will wait until a connection is available (blocking, not failing).
- Timestamps for metrics use the timezone specified in the exporter config (if set), otherwise the data source, otherwise system timezone.
- To debug connection/session issues, set `force_new_connection: true` in your data source config to bypass pooling for that source.
- Diagnostic logs will show whether pooling or new connections are used for each scrape.

## Development & Testing
- The Flask development server is disabled; always use Gunicorn for concurrency and production safety.
- You can add more exporters by creating new directories under `targets/` and placing `sql_exporter.yml` and collector files inside.
- Update your Prometheus scrape configs to reference the new exporter names.

## Troubleshooting
- If you see `ImportError: No module named ...`, ensure all dependencies are installed.
- If Prometheus scrapes time out, consider increasing pool size or reducing scrape frequency.
- Adjust Gunicorn worker and thread counts for your server’s CPU and expected load.
- Check logs for `[DEBUG]` messages to diagnose pooling and connection behavior.

## License
MIT
