import yaml                     # For loading .yml config and collector files
import glob                     # For resolving collector file patterns
import time                     # For timestamps and connection lifetime tracking
import logging                  # For structured logging
from pathlib import Path        # For clean file path handling
from flask import Flask, request, Response  # For HTTP metrics endpoint
import gzip
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
try:
    import tzlocal
except Exception:
    tzlocal = None



# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class TZFormatter(logging.Formatter):
    """Logging formatter that renders times in a given tzinfo (pytz).

    Usage: set handler.setFormatter(TZFormatter(fmt, datefmt, tz=tzobj))
    """
    def __init__(self, fmt=None, datefmt=None, tz=None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.tz = tz

    def formatTime(self, record, datefmt=None):
        # record.created is a POSIX timestamp
        try:
            if self.tz is not None:
                dt = datetime.datetime.fromtimestamp(record.created, tz=self.tz)
            else:
                dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
        except Exception:
            dt = datetime.datetime.fromtimestamp(record.created)

        if datefmt:
            return dt.strftime(datefmt)
        # Default ISO
        return dt.isoformat()


def apply_logging_timezone(tzinfo):
    """Replace formatters on existing root handlers to use tzinfo for timestamps."""
    fmt = '[%(asctime)s] %(levelname)s: %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    for h in logging.root.handlers:
        try:
            h.setFormatter(TZFormatter(fmt=fmt, datefmt=datefmt, tz=tzinfo))
        except Exception:
            # fallback: ignore failures to set formatter
            pass

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
    Loads the sql_exporter.yml file for the given exporter name from targets/<exporter>/sql_exporter.yml.
    Returns the parsed config dictionary and the base directory path.
    """
    from pathlib import Path
    import yaml
    import logging

    config_path = Path(f"./targets/{exporter_name}/sql_exporter.yml")
    logging.info(f"Loading config from: {config_path}")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as f:
        config = yaml.safe_load(f)

    # Exclude password from logging
    config_to_log = yaml.safe_load(yaml.dump(config))
    for ds in config_to_log.get('data_sources', {}).values():
        if 'password' in ds:
            ds['password'] = '***'
    log_metrics = config_to_log.get('global', {}).get('log_scraped_metrics', False)
    logging.info(f"Loaded config: {config_to_log}")
    logging.info(f"log_scraped_metrics enabled: {log_metrics}")
    return config, config_path.parent

def build_dsn(conn_config):
    dsn = {
        "host": conn_config["host"],
        "user": conn_config["user"],
        "password": conn_config.get("password")
    }
    # Include optional parameters if present
    for key in ["logmech", "connect_timeout"]:
        if key in conn_config:
            dsn[key] = conn_config[key]
    # Do NOT set queryBand here; it is set after connection
    # Mask password for logging
    dsn_log = dict(dsn)
    if 'password' in dsn_log:
        dsn_log['password'] = '***'
    logging.debug(f"Building DSN from connection config: {dsn_log}")
    return dsn

def resolve_collectors(config, base_dir):
    collector_files = []
    logging.info(f"Resolving collector files using patterns: {config.get('collector_files', [])}")
    for pattern in config.get('collector_files', []):
        # Only match files in the current exporter directory
        matched = [str(p) for p in Path(base_dir).glob(pattern) if p.is_file()]
        logging.debug(f"Pattern '{pattern}' matched files: {matched}")
        collector_files.extend(matched)

    matched_collectors = []
    available_collectors = []  # list of (collector_name, file_path)
    for file_path in collector_files:
        try:
            with open(file_path) as f:
                collector = yaml.safe_load(f)
        except Exception as e:
            logging.warning(f"Failed to parse collector file '{file_path}': {e}")
            # record that the file existed but couldn't be parsed
            available_collectors.append((f"<<parse_error>>", file_path))
            continue
        collector_name = collector.get('collector_name') if isinstance(collector, dict) else None
        logging.info(f"Evaluating collector '{collector_name}' from file: {file_path}")
        if collector_name:
            available_collectors.append((collector_name, file_path))
            if any(glob.fnmatch.fnmatch(collector_name, pattern) for pattern in config['target']['collectors']):
                matched_collectors.append((collector_name, collector))

    matched_names = [name for name, _ in matched_collectors]
    available_names = [name for name, _ in available_collectors]
    logging.info(f"Matched collectors: {matched_names}")
    # Log available collector names at INFO so it's visible in standard logs
    logging.info(f"Available collectors: {available_names}")
    return matched_collectors, available_collectors

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
            return f"{float_val:.9e}"
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
        # Mask password in DSN for logging
        dsn_log = dict(dsn)
        if 'password' in dsn_log:
            dsn_log['password'] = '***'
        logging.debug(f"Attempt {attempt}: Trying to connect with DSN: {dsn_log} and config: [password hidden]")
        start_time = time.time()
        try:
            # Pass connect_timeout if supported by teradatasql
            if connect_timeout is not None:
                dsn['connect_timeout'] = connect_timeout
            logging.info(f"Connection attempt {attempt} DSN: {dsn_log}")
            conn = teradatasql.connect(**dsn)
            # Set query band for session if present
            if "query_band" in conn_config:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SET QUERY_BAND = '{conn_config['query_band']}' FOR SESSION;")
                    cursor.close()
                    logging.info(f"Set query band for session: {conn_config['query_band']}")
                except Exception as qb_exc:
                    logging.warning(f"Failed to set query band: {qb_exc}")
            elapsed = time.time() - start_time
            logging.info(f"Connection attempt {attempt} succeeded in {elapsed:.2f} seconds.")
            return conn
        except Exception as exc:
            last_exc = exc
            elapsed = time.time() - start_time
            tb = traceback.format_exc()
            logging.warning(f"Connection attempt {attempt} failed after {elapsed:.2f} seconds: {exc}\nTraceback:\n{tb}")
            if attempt < retries:
                time.sleep(delay)
    if last_exc is not None:
        logging.error(f"All connection attempts failed. Last error: {last_exc}\nTraceback:\n{traceback.format_exc()}")
        raise last_exc
    else:
        logging.error(f"All connection attempts failed, but no exception was captured. DSN: {dsn_log}, config: [password hidden]")
        raise Exception("Unknown connection error: no exception captured during retries")

def is_connection_alive(conn):
    """
    Checks if the Teradata connection is alive by running a lightweight query.
    Returns True if alive, False otherwise.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        return True
    except Exception as e:
        logging.warning(f"Connection health check failed: {e}")
        return False

def run_queries(dsn_dict, queries, connection_pool, max_idle, max_lifetime, tz, conn_config=None):
    import datetime
    import time
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
    logging.info("Acquiring DB connection(s) for parallel query execution...")

    # Group metrics by their SQL query
    query_to_metrics = defaultdict(list)
    for q in queries:
        query_to_metrics[q['sql']].append(q)

    query_defs = []
    for sql, metrics in query_to_metrics.items():
        # Use the first metric as a template for connection/query config
        query_def = metrics[0].copy()
        query_def['metrics'] = metrics
        query_defs.append(query_def)

    # If there are no query definitions, nothing to do — return empty output to
    # the caller. This protects against creating a ThreadPoolExecutor with
    # max_workers=0 when there are no queries (which raises ValueError).
    if not query_defs:
        logging.error("No query definitions produced from collectors; aborting run_queries")
        return []

    query_retries = (conn_config or {}).get('query_retries', 1)
    query_retry_delay = (conn_config or {}).get('query_retry_delay', 1)
    force_new_connection = conn_config.get('force_new_connection', False) if conn_config else False
    num_queries = len(query_defs)
    # Respect configured max_connections but ensure at least one worker
    config_max_conn = (conn_config.get('max_connections', 1) if conn_config else 1)
    max_workers = max(1, min(num_queries, config_max_conn))

    def execute_query(query_def):
        conn_wrapper = None
        try:
            if force_new_connection:
                conn = connect_with_retries(conn_config or {})
                conn_wrapper = PooledConnection(conn)
            else:
                if not connection_pool.empty():
                    candidate = connection_pool.get()
                    if candidate.is_expired(max_lifetime) or not is_connection_alive(candidate.conn):
                        try:
                            candidate.conn.close()
                        except Exception:
                            pass
                        conn = connect_with_retries(conn_config or {})
                        conn_wrapper = PooledConnection(conn)
                    else:
                        conn_wrapper = candidate
                else:
                    conn = connect_with_retries(conn_config or {})
                    conn_wrapper = PooledConnection(conn)

            sql = query_def['sql']
            metrics = query_def['metrics']
            metric_samples = defaultdict(list)
            metric_meta = {}
            attempt = 0
            while attempt < query_retries:
                try:
                    cursor = conn_wrapper.conn.cursor()
                    logging.info(f"[DEBUG] Executing on connection: {conn_wrapper.conn}, cursor: {cursor}")
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    break  # Success
                except Exception as e:
                    tb = traceback.format_exc()
                    logging.error(f"[ERROR] Query execution failed for SQL '{sql}' (attempt {attempt+1}): {e}\nTraceback:\n{tb}")
                    if 'lost connection' in str(e).lower() or 'connection reset' in str(e).lower() or 'session reset' in str(e).lower():
                        try:
                            conn_wrapper.conn.close()
                        except Exception:
                            pass
                        conn = connect_with_retries(conn_config or {})
                        conn_wrapper = PooledConnection(conn)
                        attempt += 1
                        if attempt < query_retries:
                            time.sleep(query_retry_delay)
                        continue
                    else:
                        raise
            else:
                raise Exception(f"Query failed after {query_retries} retries due to lost connection.")

            # For each metric referencing this query, process the result set
            for metric in metrics:
                metric_name = metric['metric_name']
                help_text = metric.get('help', '')
                metric_type = metric.get('type', 'gauge')
                metric_meta[metric_name] = (help_text, metric_type)
                for row in rows:
                    labels = []
                    for label in metric.get('labels', []):
                        try:
                            i = next(idx for idx, col in enumerate(cursor.description) if col[0].lower() == label.lower())
                            labels.append(f'{label}="{row[i]}"')
                        except StopIteration:
                            logging.warning(f"Label column '{label}' not found in result set for metric {metric_name}")
                    for k, v in metric.get('static_labels', {}).items():
                        labels.append(f'{k}="{v}"')
                    value = metric.get('static_value', None)
                    if value is None and metric.get('values'):
                        value_column = metric['values'][0]
                        try:
                            value_index = next(i for i, col in enumerate(cursor.description) if col[0].lower() == value_column.lower())
                            raw_value = row[value_index]
                            value = format_value(raw_value)
                        except Exception as e:
                            logging.warning(f"Could not resolve value column '{value_column}' for metric {metric_name}: {e}")
                            value = "0"
                    else:
                        value = format_value(value)
                    timestamp = ""
                    ts_col = metric.get('timestamp_value')
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
                    # Sort labels alphabetically by label name
                    labels_sorted = sorted(labels, key=lambda x: x.split('=')[0])
                    metric_line = f"{metric_name}{{{','.join(labels_sorted)}}} {value}{timestamp}"
                    metric_samples[metric_name].append(metric_line)
            # Return connection to pool if not forced new
            if not force_new_connection and connection_pool.qsize() < max_idle:
                connection_pool.put(conn_wrapper)
            else:
                try:
                    conn_wrapper.conn.close()
                except Exception:
                    pass
            return metric_meta, metric_samples
        except Exception as e:
            if conn_wrapper:
                try:
                    conn_wrapper.conn.close()
                except Exception as close_exc:
                    logging.error(f"[ERROR] Failed to close connection after error: {close_exc}")
            raise e

    # Collect all metric samples and meta from all queries
    all_metric_meta = {}
    all_metric_samples = defaultdict(list)
    errors = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_query = {executor.submit(execute_query, q): q for q in query_defs}
        try:
            for future in as_completed(future_to_query, timeout=conn_config.get('scrape_timeout_offset', 0) or None):
                query_def = future_to_query[future]
                try:
                    metric_meta, metric_samples = future.result()
                    for mname, meta in metric_meta.items():
                        all_metric_meta[mname] = meta
                    for mname, samples in metric_samples.items():
                        all_metric_samples[mname].extend(samples)
                except Exception as e:
                    logging.error(f"Error running query for SQL '{query_def['sql']}': {e}")
                    errors[query_def['sql']] = str(e)
        except TimeoutError:
            logging.error("Partial scrape: scrape_timeout reached, returning partial results.")
    # Add error metrics for failed queries
    for sql in errors:
        all_metric_samples[f'up_error_{sql}'].append(f'up{{query="{sql}"}} 0')
        all_metric_samples[f'up_error_{sql}'].append(f'error{{query="{sql}",message="{errors[sql]}"}} 1')
    # Output metrics grouped by metric_name, with samples sorted alphabetically
    sorted_output = []
    for mname in sorted(all_metric_samples.keys()):
        if mname in all_metric_meta:
            help_text, metric_type = all_metric_meta[mname]
            sorted_output.append(f"# HELP {mname} {help_text}")
            sorted_output.append(f"# TYPE {mname} {metric_type}")
        # Sort samples for this metric_name
        sorted_output.extend(sorted(all_metric_samples[mname]))
    return sorted_output

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

def acquire_connections(connection_pool, pool_lock, num_needed, timeout=30, conn_config=None, max_lifetime=0):
    """
    Acquire up to num_needed live connections from the pool, replacing dead/expired ones with new connections.
    Returns a list of connections.
    """
    acquired = []
    attempts = 0
    while len(acquired) < num_needed and attempts < num_needed * 2:
        attempts += 1
        try:
            conn_wrapper = connection_pool.get(block=True, timeout=timeout)
            # Check expiration and health
            expired = conn_wrapper.is_expired(max_lifetime)
            alive = is_connection_alive(conn_wrapper.conn)
            if expired or not alive:
                logging.info(f"Discarding pooled connection (expired={expired}, alive={alive})")
                try:
                    conn_wrapper.conn.close()
                except Exception as e:
                    logging.warning(f"Error closing dead/expired connection: {e}")
                # Replace with new connection
                if conn_config:
                    try:
                        new_conn = connect_with_retries(conn_config)
                        acquired.append(PooledConnection(new_conn))
                        logging.info("Replaced dead/expired connection with new live connection")
                    except Exception as e:
                        logging.error(f"Failed to create new connection after discarding dead/expired: {e}")
                continue
            acquired.append(conn_wrapper)
        except queue.Empty:
            break  # Timeout reached, stop waiting
    # If not enough connections, try to create new ones
    while len(acquired) < num_needed and conn_config:
        try:
            new_conn = connect_with_retries(conn_config)
            acquired.append(PooledConnection(new_conn))
            logging.info("Created new connection to meet required pool size")
        except Exception as e:
            logging.error(f"Failed to create new connection: {e}")
            break
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


def make_text_response(body_text, status=200):
    """Create a text/plain response with escaping=underscores in Content-Type and
    gzip-compressed body when the client supports it via Accept-Encoding.
    """
    try:
        body_bytes = body_text.encode('utf-8') if isinstance(body_text, str) else body_text
    except Exception:
        body_bytes = str(body_text).encode('utf-8')

    content_type = 'text/plain; charset=utf-8; escaping=underscores'
    accept_enc = request.headers.get('Accept-Encoding', '') or ''
    logging.debug(f"Client Accept-Encoding header: '{accept_enc}'")
    if 'gzip' in accept_enc.lower():
        logging.info("Client supports gzip; attempting to compress response")
        try:
            compressed = gzip.compress(body_bytes)
            logging.info(f"Compressed response: {len(body_bytes)} -> {len(compressed)} bytes")
            resp = Response(compressed, status=status)
            resp.headers['Content-Encoding'] = 'gzip'
            resp.headers['Content-Type'] = content_type
            resp.headers['Content-Length'] = str(len(compressed))
            return resp
        except Exception as e:
            logging.warning(f"Gzip compression failed: {e}; sending uncompressed response")
            # fallback to uncompressed
            pass

    logging.debug("Sending uncompressed response")
    resp = Response(body_bytes, status=status)
    resp.headers['Content-Type'] = content_type
    resp.headers['Content-Length'] = str(len(body_bytes))
    return resp

@app.route('/metrics')
def metrics():
    exporter = request.args.get('exporter')
    if not exporter:
        logging.warning("Missing 'exporter' parameter in request")
        return make_text_response("Missing 'exporter' parameter", status=400)
    # Sanitize exporter name: only allow alphanumeric, underscore, dash
    import re
    if not re.match(r'^[A-Za-z0-9_-]+$', exporter):
        logging.warning(f"Invalid exporter parameter: {exporter}")
        return make_text_response("Invalid 'exporter' parameter", status=400)

    try:
        config, base_dir = load_sql_exporter_config(exporter)
        start = time.time()

        # ✅ Extract global settings
        global_config = config.get('global', {})
        # Ensure exporter max connections is at least 1 to avoid zero-worker situations
        exporter_max_conn = max(1, global_config.get('max_connections', 1))
        max_idle = global_config.get('max_idle_connections', 1)
        max_lifetime = parse_duration(global_config.get('max_connection_lifetime', '0'))
        scrape_timeout_offset = parse_duration(global_config.get('scrape_timeout_offset', '0'))
        # Load connection config from settings.yml using data_source_name
        data_source_name = config['target']['data_source_name']
        settings_path = Path(__file__).parent / 'settings.yml'
        conn_config = get_connection_config_from_settings(data_source_name, settings_path)
        # Get scrape_timeout from config, fallback to settings.yml, then default
        settings = load_settings(settings_path)
        settings_scrape_timeout = settings.get('scrape_timeout', 30)
        scrape_timeout_val = global_config.get('scrape_timeout', settings_scrape_timeout)
        scrape_timeout = parse_duration(str(scrape_timeout_val))

        # --- Get Prometheus scrape timeout from header ---
        prometheus_timeout = request.headers.get('X-Prometheus-Scrape-Timeout-Seconds')
        if prometheus_timeout:
            try:
                prometheus_timeout = float(prometheus_timeout)
            except Exception:
                prometheus_timeout = None
        # Calculate effective timeout
        if prometheus_timeout:
            effective_timeout = max(prometheus_timeout - scrape_timeout_offset, 0.1)
        else:
            effective_timeout = scrape_timeout

        dsn = build_dsn(conn_config)
        # Ensure pool size is at least 1
        pool_size = max(1, conn_config.get('max_connections', 1))

        connection_pool, pool_lock = get_or_create_pool(data_source_name, conn_config, pool_size)

        matched_collectors, available_collectors = resolve_collectors(config, base_dir)
        # Validate that requested collectors actually exist
        requested_collectors = config['target'].get('collectors', [])
        if len(matched_collectors) == 0:
            # If collector files exist but none matched the requested collector names,
            # return a helpful error listing the available collector names to aid debugging.
            if available_collectors:
                available_names = [name for name, _ in available_collectors]
                msg = (
                    f"Collectors files were found, but none matched the requested names {requested_collectors}. "
                    f"Available collector names: {available_names}. Check for misspellings in your collector_name fields."
                )
            else:
                msg = f"No collectors matching patterns {config.get('collector_files', [])} were found for exporter '{exporter}' in {base_dir}"
            logging.error(msg)
            return make_text_response(msg, status=400)

        queries = load_queries_from_collectors(matched_collectors)
        # If collectors matched but produced no queries/metrics, return an informative error
        if not queries:
            matched_names = [name for name, _ in matched_collectors]
            msg = f"Collectors {matched_names} matched but contain no metrics/queries for exporter '{exporter}'"
            logging.error(msg)
            return make_text_response(msg, status=400)
        tz = get_timezone(global_config, conn_config)

        with pool_lock:
            num_to_acquire = min(exporter_max_conn, pool_size)
            num_to_acquire = max(1, num_to_acquire)
            acquired_conns = acquire_connections(connection_pool, pool_lock, num_to_acquire, conn_config=conn_config, max_lifetime=max_lifetime)
            temp_pool = Queue(maxsize=num_to_acquire)
            for conn in acquired_conns:
                temp_pool.put(conn)

        # Enforce scrape timeout using effective_timeout
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_queries, dsn, queries, temp_pool, max_idle, max_lifetime, tz, conn_config)
            try:
                metrics_output = future.result(timeout=effective_timeout)
            except TimeoutError:
                logging.error(f"Scrape exceeded timeout of {effective_timeout} seconds")
                return make_text_response('up{target="unknown"} 0\nerror{message="scrape_timeout"} 1', status=504)

        with pool_lock:
            while not temp_pool.empty():
                connection_pool.put(temp_pool.get())

        duration = time.time() - start
        target_name = config['target'].get('name')

        if target_name:
            metrics_output.append(f'up{{target="{target_name}"}} 1')
            metrics_output.append(f'scrape_duration_seconds{{target="{target_name}"}} {duration:.3f}')

        # Log metrics if enabled (from settings.yml)
        log_metrics = settings.get('global', {}).get('log_scraped_metrics', False)
        if log_metrics:
            log_time = datetime.datetime.now().isoformat()
            logging.info(f"--- Metrics scrape at {log_time} ---\n" + "\n".join(metrics_output) + "\n--- End scrape ---")

        logging.info(f"Scrape completed in {duration:.3f} seconds")
        return make_text_response("\n".join(metrics_output), status=200)

    except Exception as e:
        logging.error(f"Error during scrape: {str(e)}")
        return make_text_response(f'up{{target="unknown"}} 0\nerror{{message="{str(e)}"}} 1', status=500)

# --- Pre-populate connection pools at startup ---
def initialize_connection_pools():
    try:
        # Load all data sources from settings.yml
        settings_path = Path(__file__).parent / 'settings.yml'
        settings = load_settings(settings_path)
        # Log global settings, hide passwords
        settings_to_log = yaml.safe_load(yaml.dump(settings))
        for ds in settings_to_log.get('data_sources', {}).values():
            if 'password' in ds:
                ds['password'] = '***'
        log_metrics = settings_to_log.get('global', {}).get('log_scraped_metrics', False)
        logging.info(f"Loaded settings.yml (passwords hidden): {settings_to_log}")
        logging.info(f"log_scraped_metrics enabled: {log_metrics}")
        # Apply timezone from settings.yml to logging output so container logs use the desired timezone
        tz_name = settings_to_log.get('global', {}).get('timezone', 'system')
        tzinfo = None
        try:
            if tz_name == 'system':
                if tzlocal:
                    tzinfo = tzlocal.get_localzone()
                else:
                    tzinfo = datetime.timezone.utc
            else:
                try:
                    tzinfo = pytz.timezone(tz_name)
                except Exception:
                    logging.warning(f"Invalid timezone '{tz_name}' in settings.yml; falling back to UTC")
                    tzinfo = datetime.timezone.utc
        except Exception as e:
            logging.warning(f"Failed to resolve timezone '{tz_name}': {e}; falling back to UTC")
            tzinfo = datetime.timezone.utc
        try:
            apply_logging_timezone(tzinfo)
            logging.info(f"Applied logging timezone from settings.yml: {tz_name}")
        except Exception:
            logging.warning("Failed to apply timezone-aware logging formatter; continuing with default formatting")
        data_sources = settings.get('data_sources', {})
        for data_source_name, conn_config in data_sources.items():
            # Ensure pool_size is at least 1 when pre-populating
            pool_size = max(1, conn_config.get('max_connections', 1))
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