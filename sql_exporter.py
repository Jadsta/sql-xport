import yaml                     # For loading .yml config and collector files
import glob                     # For resolving collector file patterns
import time                     # For timestamps and connection lifetime tracking
import logging                  # For structured logging
from pathlib import Path        # For clean file path handling
from flask import Flask, request, Response  # For HTTP metrics endpoint
import teradatasql              # For connecting to Teradata
import datetime                 # For handling datetime values and formatting
from threading import Semaphore, Lock # For limiting concurrent connections
from queue import Queue         # For managing idle connection pool
from collections import defaultdict
import queue
import pytz
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import traceback                # For detailed error logging



# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__)

def parse_duration(s):
    """
    Converts a duration string like '500ms', '10s', '5m', or '2h' into seconds (float).
    """
    units = {
        'ms': 0.001,
        's': 1,
        'm': 60,
        'h': 3600
    }

    s = s.strip().lower()
    for unit, factor in units.items():
        if s.endswith(unit):
            try:
                return float(s[:-len(unit)]) * factor
            except ValueError:
                raise ValueError(f"Invalid numeric value in duration: {s}")
    # Default fallback: assume it's raw seconds
    try:
        return float(s)
    except ValueError:
        raise ValueError(f"Unrecognized duration format: {s}")

def load_sql_exporter_config(exporter_name):
    """
    Loads the sql_exporter.yml file for the given exporter name.
    Returns the parsed config dictionary and the base directory path.
    """
    from pathlib import Path
    import yaml
    import logging

    config_path = Path(f"./{exporter_name}/sql_exporter.yml")
    logging.info(f"Loading config from: {config_path}")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as f:
        config = yaml.safe_load(f)

    logging.debug(f"Loaded config: {config}")
    return config, config_path.parent

def build_dsn(conn_config):
    logging.debug(f"Building DSN from connection config: {conn_config}")
    return {
        "host": conn_config["host"],
        "user": conn_config["user"],
        "password": conn_config["password"]
    }

def resolve_collectors(config, base_dir):
    collector_files = []
    logging.info(f"Resolving collector files using patterns: {config.get('collector_files', [])}")
    for pattern in config.get('collector_files', []):
        full_pattern = base_dir / pattern
        matched = glob.glob(str(full_pattern))
        logging.debug(f"Pattern '{pattern}' matched files: {matched}")
        collector_files.extend(matched)

    matched_collectors = []
    for file_path in collector_files:
        with open(file_path) as f:
            collector = yaml.safe_load(f)
            collector_name = collector.get('collector_name')  # ✅ supports collector_name
            logging.debug(f"Evaluating collector '{collector_name}' from file: {file_path}")
            if collector_name and any(glob.fnmatch.fnmatch(collector_name, pattern) for pattern in config['target']['collectors']):
                matched_collectors.append((collector_name, collector))

    logging.info(f"Matched collectors: {[name for name, _ in matched_collectors]}")
    return matched_collectors

def resolve_queries_from_metrics(collector):
    query_map = {q['query_name']: q['query'] for q in collector.get('queries', [])}
    resolved = []

    for metric in collector.get('metrics', []):
        query_text = metric.get('query') or query_map.get(metric.get('query_ref'))
        if not query_text:
            logging.warning(f"Missing query for metric: {metric.get('metric_name')}")
            continue

        resolved.append({
            'sql': query_text,
            'metric_name': metric['metric_name'],
            'labels': metric.get('key_labels', []),
            'static_labels': metric.get('static_labels', {}),
            'values': metric.get('values', []),
            'help': metric.get('help', ''),
            'type': metric.get('type', 'gauge'),
            'timestamp_value': metric.get('timestamp_value'),
            'static_value': metric.get('static_value')
        })

    return resolved
    
def format_value(val):
    if isinstance(val, datetime.datetime):
        return f"{val.timestamp():.9e}"
    if isinstance(val, datetime.date):
        # Convert to datetime for timestamp
        dt = datetime.datetime.combine(val, datetime.time())
        return f"{dt.timestamp():.9e}"

    try:
        float_val = float(val)
        if float_val == 0:
            return "0"
        elif abs(float_val) >= 1e6 or abs(float_val) < 1e-3:
            return f"{float_val:.7e}"
        else:
            return f"{float_val:.6f}".rstrip('0').rstrip('.')
    except (ValueError, TypeError):
        return "0"



        
def load_queries_from_collectors(matched_collectors):
    queries = []
    for name, collector in matched_collectors:
        logging.info(f"Loading collector: {name}")
        queries.extend(resolve_queries_from_metrics(collector))
    return queries
    
class PooledConnection:
    def __init__(self, conn):
        self.conn = conn
        self.created_at = time.time()

    def is_expired(self, max_lifetime):
        return max_lifetime > 0 and (time.time() - self.created_at) > max_lifetime
        
def connect_with_retries(conn_config):
    retries = conn_config.get('connection_retries', 1)
    delay = conn_config.get('retry_delay', 1)
    connect_timeout = conn_config.get('connect_timeout', None)
    dsn = build_dsn(conn_config)
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            # Pass connect_timeout if supported by teradatasql
            if connect_timeout is not None:
                dsn['logmech'] = dsn.get('logmech', None)  # placeholder for other options
                dsn['connect_timeout'] = connect_timeout
            return teradatasql.connect(**dsn)
        except Exception as exc:
            last_exc = exc
            logging.warning(f"Connection attempt {attempt} failed: {exc}")
            if attempt < retries:
                time.sleep(delay)
    raise last_exc

def run_queries(dsn_dict, queries, connection_pool, max_idle, max_lifetime, tz, conn_config=None):
    import datetime
    import time
    from collections import defaultdict
    logging.info("Acquiring DB connection...")

    conn_wrapper = None
    try:
        # Try to reuse a connection
        while not connection_pool.empty():
            candidate = connection_pool.get()
            if candidate.is_expired(max_lifetime):
                logging.info("Discarding expired connection")
                candidate.conn.close()
            else:
                conn_wrapper = candidate
                break

        # Create new if none available
        if conn_wrapper is None:
            logging.info("Opening new DB connection with retries")
            conn = connect_with_retries(conn_config or {})
            conn_wrapper = PooledConnection(conn)

        cursor = conn_wrapper.conn.cursor()
        metric_blocks = defaultdict(list)

        for query_def in queries:
            metric_name = query_def['metric_name']
            help_text = query_def.get('help', '')
            metric_type = query_def.get('type', 'gauge')

            # Add HELP and TYPE lines
            metric_blocks[metric_name].append(f"# HELP {metric_name} {help_text}")
            metric_blocks[metric_name].append(f"# TYPE {metric_name} {metric_type}")

            logging.info(f"Executing query: {query_def['sql']}")
            cursor.execute(query_def['sql'])

            for row in cursor.fetchall():
                logging.debug(f"Query result row: {row}")
                labels = []

                # Resolve labels by column name
                for label in query_def.get('labels', []):
                    try:
                        i = next(idx for idx, col in enumerate(cursor.description) if col[0].lower() == label.lower())
                        labels.append(f'{label}="{row[i]}"')
                    except StopIteration:
                        logging.warning(f"Label column '{label}' not found in result set for metric {metric_name}")

                # Add static labels
                for k, v in query_def.get('static_labels', {}).items():
                    labels.append(f'{k}="{v}"')

                # Resolve value
                value = query_def.get('static_value', None)
                if value is None and query_def.get('values'):
                    value_column = query_def['values'][0]
                    try:
                        value_index = next(i for i, col in enumerate(cursor.description) if col[0].lower() == value_column.lower())
                        raw_value = row[value_index]
                        value = format_value(raw_value)
                    except Exception as e:
                        logging.warning(f"Could not resolve value column '{value_column}' for metric {metric_name}: {e}")
                        value = "0"
                else:
                    value = format_value(value)

                # Resolve timestamp
                timestamp = ""
                ts_col = query_def.get('timestamp_value')
                if ts_col:
                    try:
                        ts_index = next(i for i, col in enumerate(cursor.description) if col[0].lower() == ts_col.lower())
                        ts_raw = row[ts_index]
                        if isinstance(ts_raw, datetime.datetime):
                            if ts_raw.tzinfo is None:
                                ts_raw = tz.localize(ts_raw)
                            timestamp = f" {int(ts_raw.timestamp() * 1000)}"
                        elif isinstance(ts_raw, datetime.date):
                            dt = datetime.datetime.combine(ts_raw, datetime.time())
                            dt = tz.localize(dt)
                            timestamp = f" {int(dt.timestamp() * 1000)}"
                    except Exception as e:
                        logging.warning(f"Could not extract timestamp for {metric_name}: {e}")

                metric_line = f"{metric_name}{{{','.join(labels)}}} {value}{timestamp}"
                metric_blocks[metric_name].append(metric_line)

        # Return to pool if under idle limit
        if connection_pool.qsize() < max_idle:
            connection_pool.put(conn_wrapper)
        else:
            logging.info("Idle pool full, closing connection")
            conn_wrapper.conn.close()

        # Sort metrics alphabetically by metric name
        sorted_output = []
        for metric_name in sorted(metric_blocks.keys()):
            sorted_output.extend(metric_blocks[metric_name])

        return sorted_output

    except Exception as e:
        if conn_wrapper:
            conn_wrapper.conn.close()
        raise e

def load_settings(settings_path):
    """
    Loads the settings.yml file containing data source connection info.
    """
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f)

def get_connection_config_from_settings(data_source_name, settings_path):
    settings = load_settings(settings_path)
    data_sources = settings.get('data_sources', {})
    if data_source_name not in data_sources:
        raise KeyError(f"Data source '{data_source_name}' not found in settings.yml")
    return data_sources[data_source_name]

# Global connection pools per data source
connection_pools = {}
pool_locks = {}

def get_or_create_pool(data_source_name, conn_config, max_pool_size):
    """
    Get or create a shared connection pool for the given data source.
    """
    global connection_pools, pool_locks
    if data_source_name not in connection_pools:
        connection_pools[data_source_name] = Queue(maxsize=max_pool_size)
        pool_locks[data_source_name] = Lock()
    return connection_pools[data_source_name], pool_locks[data_source_name]

def acquire_connections(connection_pool, pool_lock, num_needed, timeout=30):
    """
    Acquire up to num_needed connections from the pool, blocking until available.
    Returns a list of connections.
    """
    acquired = []
    for _ in range(num_needed):
        try:
            conn = connection_pool.get(block=True, timeout=timeout)
            acquired.append(conn)
        except queue.Empty:
            break  # Timeout reached, stop waiting
    return acquired

def get_timezone(global_config, conn_config):
    tz_name = global_config.get('timezone') or conn_config.get('timezone') or 'system'
    if tz_name == 'system':
        # Use system local timezone
        try:
            import tzlocal
            return tzlocal.get_localzone()
        except ImportError:
            return datetime.timezone.utc  # fallback to UTC
    else:
        return pytz.timezone(tz_name)

@app.route('/metrics')
def metrics():
    exporter = request.args.get('exporter')
    if not exporter:
        logging.warning("Missing 'exporter' parameter in request")
        return "Missing 'exporter' parameter", 400

    try:
        config, base_dir = load_sql_exporter_config(exporter)
        start = time.time()

        # ✅ Extract global settings
        global_config = config.get('global', {})
        exporter_max_conn = global_config.get('max_connections', 1)
        max_idle = global_config.get('max_idle_connections', 1)
        max_lifetime = parse_duration(global_config.get('max_connection_lifetime', '0'))
        scrape_timeout_offset = parse_duration(global_config.get('scrape_timeout_offset', '0'))

        # Load connection config from settings.yml using data_source_name
        data_source_name = config['target']['data_source_name']
        # Always load settings.yml from the main script directory
        settings_path = Path(__file__).parent / 'settings.yml'
        conn_config = get_connection_config_from_settings(data_source_name, settings_path)
        dsn = build_dsn(conn_config)
        pool_size = conn_config.get('max_connections', 1)

        # Get or create the shared pool for this data source
        connection_pool, pool_lock = get_or_create_pool(data_source_name, conn_config, pool_size)

        matched_collectors = resolve_collectors(config, base_dir)
        queries = load_queries_from_collectors(matched_collectors)
        tz = get_timezone(global_config, conn_config)

        # Acquire connections, blocking if necessary
        with pool_lock:
            num_to_acquire = min(exporter_max_conn, pool_size)
            acquired_conns = acquire_connections(connection_pool, pool_lock, num_to_acquire)
            temp_pool = Queue(maxsize=num_to_acquire)
            for conn in acquired_conns:
                temp_pool.put(conn)

        # Enforce scrape timeout using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_queries, dsn, queries, temp_pool, max_idle, max_lifetime, tz, conn_config)
            try:
                metrics_output = future.result(timeout=scrape_timeout_offset)
            except TimeoutError:
                logging.error(f"Scrape exceeded timeout of {scrape_timeout_offset} seconds")
                return Response(f'up{{target="unknown"}} 0\nerror{{message="scrape_timeout"}} 1', mimetype='text/plain', status=504)

        # After scrape, return connections to the shared pool
        with pool_lock:
            while not temp_pool.empty():
                connection_pool.put(temp_pool.get())

        duration = time.time() - start
        target_name = config['target'].get('name')

        if target_name:
            metrics_output.append(f'up{{target="{target_name}"}} 1')
            metrics_output.append(f'scrape_duration_seconds{{target="{target_name}"}} {duration:.3f}')

        logging.info(f"Scrape completed in {duration:.3f} seconds")
        return Response("\n".join(metrics_output), mimetype='text/plain')

    except Exception as e:
        logging.error(f"Error during scrape: {str(e)}")
        return Response(f'up{{target="unknown"}} 0\nerror{{message="{str(e)}"}} 1', mimetype='text/plain', status=500)

# --- Pre-populate connection pools at startup ---
def initialize_connection_pools():
    try:
        # Find all exporter configs in subfolders
        for exporter_folder in Path(__file__).parent.glob('*/sql_exporter.yml'):
            exporter_name = exporter_folder.parent.name
            config, _ = load_sql_exporter_config(exporter_name)
            data_source_name = config['target']['data_source_name']
            settings_path = Path(__file__).parent / 'settings.yml'
            conn_config = get_connection_config_from_settings(data_source_name, settings_path)
            pool_size = conn_config.get('max_connections', 1)
            connection_pool, pool_lock = get_or_create_pool(data_source_name, conn_config, pool_size)
            # Pre-populate pool with live connections
            with pool_lock:
                for _ in range(pool_size):
                    try:
                        conn = connect_with_retries(conn_config)
                        connection_pool.put(PooledConnection(conn))
                        logging.info(f"Pre-populated connection for data source '{data_source_name}'")
                    except Exception as e:
                        tb = traceback.format_exc()
                        logging.error(f"Failed to pre-populate connection for '{data_source_name}': {e}\n{tb}")
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Error initializing connection pools: {e}\n{tb}")

# Call this at startup
initialize_connection_pools()