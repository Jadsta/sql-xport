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
import sys                      # For stderr output in logging handler
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import traceback                # For detailed error logging
try:
    import tzlocal
except Exception:
    tzlocal = None



# Configure logging based on settings.yml (global.log_level). If settings.yml is missing or
# the setting is invalid, fall back to INFO.
_level = logging.INFO
try:
    _settings_path = Path(__file__).parent / 'settings.yml'
    if _settings_path.exists():
        try:
            import yaml as _yaml
            _settings = _yaml.safe_load(_settings_path.read_text()) or {}
            _gl = (_settings.get('global') or {})
            _lvl = _gl.get('log_level') or _gl.get('logging_level')
            if _lvl:
                _lvl_name = str(_lvl).upper()
                _level = getattr(logging, _lvl_name, logging.INFO)
        except Exception:
            # Ignore YAML parse/read errors and use default level
            _level = logging.INFO
except Exception:
    _level = logging.INFO

logging.basicConfig(
    level=_level,
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


class TeradataLogHandler(logging.Handler):
    """Custom logging handler that writes logs to a Teradata database table."""
    
    def __init__(self, db_config, table_name, username='sql_exporter', batch_size=10, flush_interval=5, tz=None):
        super().__init__()
        self.db_config = db_config
        self.table_name = table_name
        self.username = username
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.tz = tz or datetime.timezone.utc  # Use provided timezone or default to UTC
        self.buffer = []
        self.last_flush = time.time()
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Teradata for logging."""
        try:
            dsn = {
                "host": self.db_config["host"],
                "user": self.db_config["user"],
                "password": self.db_config.get("password")
            }
            for key in ["logmech", "connect_timeout"]:
                if key in self.db_config:
                    dsn[key] = self.db_config[key]
            self.conn = teradatasql.connect(**dsn)
        except Exception as e:
            # Don't crash if database logging connection fails
            print(f"Failed to connect to Teradata for logging: {e}", file=sys.stderr)
            self.conn = None
    
    def emit(self, record):
        """Buffer log record and flush to database when batch is full."""
        try:
            # Create timestamp components using configured timezone
            dt = datetime.datetime.fromtimestamp(record.created, tz=self.tz)
            create_date = dt.strftime('%Y-%m-%d')
            create_time = dt.strftime('%H:%M:%S')
            create_ts = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # milliseconds
            
            # Use only the message, not the formatted log entry
            event_text = record.getMessage()
            event_type = record.levelname
            
            self.buffer.append((create_date, create_time, create_ts, self.username, event_type, event_text))
            
            # Flush if buffer is full or flush interval exceeded
            if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush) >= self.flush_interval:
                self.flush()
        except Exception:
            # Silently fail to prevent logging errors from breaking the application
            pass
    
    def flush(self):
        """Write buffered log records to Teradata."""
        if not self.buffer:
            return
        
        if not self.conn:
            self._connect()
        
        if not self.conn:
            # Still no connection, clear buffer and give up
            self.buffer = []
            return
        
        try:
            cursor = self.conn.cursor()
            # Build multi-row insert
            sql = f"""INSERT INTO {self.table_name} 
                (CreateDate, CreateTime, CreateTS, UserName, EventType, EventText)
                VALUES (?, ?, ?, ?, ?, ?)"""
            
            cursor.executemany(sql, self.buffer)
            self.conn.commit()
            cursor.close()
            self.buffer = []
            self.last_flush = time.time()
        except Exception as e:
            # If insert fails, try to reconnect for next batch
            print(f"Failed to write logs to Teradata: {e}", file=sys.stderr)
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self.buffer = []  # Clear buffer to prevent memory buildup
    
    def close(self):
        """Flush remaining logs and close connection."""
        self.flush()
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        super().close()


# Optional: Add separate error log file if configured in settings.yml
try:
    if _settings_path.exists():
        _error_log_file = (_settings.get('global') or {}).get('error_log_file')
        if _error_log_file:
            _error_handler = logging.FileHandler(_error_log_file)
            _error_handler.setLevel(logging.ERROR)
            _error_handler.setFormatter(logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            logging.getLogger().addHandler(_error_handler)
            logging.getLogger().info(f"Error logging to file: {_error_log_file}")
except Exception as e:
    # If error log setup fails, continue without it
    pass

# Optional: Add Teradata database logging if configured in settings.yml
try:
    if _settings_path.exists():
        _db_logging = (_settings.get('global') or {}).get('database_logging')
        if _db_logging and _db_logging.get('enabled'):
            _db_log_config = _db_logging.get('connection', {})
            _db_log_table = _db_logging.get('table_name')
            # Use the database connection username as the logger username
            _db_log_username = _db_log_config.get('user', 'sql_exporter')
            _db_log_batch = _db_logging.get('batch_size', 10)
            _db_log_interval = _db_logging.get('flush_interval', 5)
            _db_log_level = _db_logging.get('log_level', 'INFO')
            
            # Get timezone for database logging timestamps
            _tz_name = (_settings.get('global') or {}).get('timezone', 'system')
            _db_tz = None
            try:
                if _tz_name == 'system':
                    if tzlocal:
                        _db_tz = tzlocal.get_localzone()
                    else:
                        _db_tz = datetime.timezone.utc
                else:
                    _db_tz = pytz.timezone(_tz_name)
            except Exception:
                _db_tz = datetime.timezone.utc
            
            if _db_log_config and _db_log_table:
                _td_handler = TeradataLogHandler(
                    db_config=_db_log_config,
                    table_name=_db_log_table,
                    username=_db_log_username,
                    batch_size=_db_log_batch,
                    flush_interval=_db_log_interval,
                    tz=_db_tz
                )
                _td_handler.setLevel(getattr(logging, _db_log_level.upper(), logging.INFO))
                logging.getLogger().addHandler(_td_handler)
                logging.getLogger().info(f"Database logging enabled to {_db_log_table} (level: {_db_log_level}, tz: {_tz_name})")
except Exception as e:
    # If database logging setup fails, continue without it
    print(f"Failed to initialize database logging: {e}", file=sys.stderr)
    pass

logging.getLogger().info(f"Logging initialized at level: {logging.getLevelName(_level)} (from settings.yml)")


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
    logging.debug(f"Matched collectors: {matched_names}")
    # Log available collector names at DEBUG to reduce log verbosity
    logging.debug(f"Available collectors: {available_names}")
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
            # Historically the exporter used an exponential format whose
            # precision varied with the magnitude (e+07 -> 7 digits, e+09 -> 9 digits, etc.).
            # Reproduce that behavior: use the absolute value of the base-10
            # exponent as the number of digits after the decimal point in
            # the exponential representation.
            import math
            try:
                exponent = int(math.floor(math.log10(abs(float_val))))
            except Exception:
                exponent = 0
            precision = abs(exponent) if exponent != 0 else 1
            # Cap precision to avoid extremely long outputs for very large/small values
            precision = max(1, min(precision, 12))
            # Format then strip trailing zeros from the mantissa (but keep at least one digit)
            s = f"{float_val:.{precision}e}"
            try:
                mantissa, exp = s.split('e')
                mantissa = mantissa.rstrip('0').rstrip('.')
                if mantissa == '' or mantissa == '-' or mantissa == '+':
                    mantissa = '0'
                s = f"{mantissa}e{exp}"
            except Exception:
                pass
            return s
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

def run_queries(dsn_dict, queries, connection_pool, max_idle, tz, conn_config=None, effective_timeout=None):
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
        cursor = None
        # Get data_source_name from conn_config for connection count tracking
        data_source_name = None
        if conn_config:
            # Try to find data_source_name from settings by matching conn_config
            try:
                settings = load_settings(Path(__file__).parent / 'settings.yml')
                for ds_name, ds_config in settings.get('data_sources', {}).items():
                    if ds_config.get('host') == conn_config.get('host') and ds_config.get('user') == conn_config.get('user'):
                        data_source_name = ds_name
                        break
            except Exception:
                pass
        try:
            if force_new_connection:
                conn = connect_with_retries(conn_config or {})
                conn_wrapper = PooledConnection(conn)
                # Increment count for new connection
                if data_source_name and data_source_name in connection_counts:
                    connection_counts[data_source_name] += 1
                    logging.debug(f"Created new connection (force_new_connection=True), count: {connection_counts[data_source_name]}")
            else:
                if not connection_pool.empty():
                    candidate = connection_pool.get()
                    # Check if connection is alive
                    if not is_connection_alive(candidate.conn):
                        try:
                            candidate.conn.close()
                            # Decrement count for closed dead connection (it was counted when acquired)
                            if data_source_name and data_source_name in connection_counts:
                                connection_counts[data_source_name] -= 1
                                logging.info(f"Closed dead connection (alive=False), count: {connection_counts[data_source_name]}")
                        except Exception:
                            pass
                        conn = connect_with_retries(conn_config or {})
                        conn_wrapper = PooledConnection(conn)
                        # Increment count for replacement connection
                        if data_source_name and data_source_name in connection_counts:
                            connection_counts[data_source_name] += 1
                            logging.debug(f"Created replacement connection, count: {connection_counts[data_source_name]}")
                    else:
                        conn_wrapper = candidate
                else:
                    # temp_pool is empty, create new connection
                    conn = connect_with_retries(conn_config or {})
                    conn_wrapper = PooledConnection(conn)
                    # Increment count for new connection
                    if data_source_name and data_source_name in connection_counts:
                        connection_counts[data_source_name] += 1
                        logging.debug(f"Created new connection (temp_pool empty), count: {connection_counts[data_source_name]}")

            sql = query_def['sql']
            metrics = query_def['metrics']
            metric_samples = defaultdict(list)
            metric_meta = {}
            attempt = 0
            while attempt < query_retries:
                try:
                    cursor = conn_wrapper.conn.cursor()
                    logging.debug(f"Executing on connection: {conn_wrapper.conn}, cursor: {cursor}")
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    break  # Success
                except Exception as e:
                    # Close cursor before retrying if it exists
                    if cursor:
                        try:
                            cursor.close()
                            logging.debug(f"Cursor closed before retry for SQL: {sql}")
                        except Exception as cursor_close_exc:
                            logging.warning(f"Failed to close cursor before retry for SQL '{sql}': {cursor_close_exc}")
                        cursor = None
                    
                    tb = traceback.format_exc()
                    logging.error(f"[ERROR] Query execution failed for SQL '{sql}' (attempt {attempt+1}): {e}\nTraceback:\n{tb}")
                    if 'lost connection' in str(e).lower() or 'connection reset' in str(e).lower() or 'session reset' in str(e).lower():
                        try:
                            conn_wrapper.conn.close()
                            # Note: We don't decrement here because we're replacing with a new connection
                            # The count stays the same (close old, create new = net zero)
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
            
            # Return connection to temp pool if not forced new connection
            if not force_new_connection:
                connection_pool.put(conn_wrapper)  # connection_pool is temp_pool here
            else:
                try:
                    conn_wrapper.conn.close()
                    # Decrement connection count when force_new_connection is used
                    if data_source_name and data_source_name in connection_counts:
                        connection_counts[data_source_name] -= 1
                        logging.debug(f"Decremented connection count after force_new_connection close (count: {connection_counts[data_source_name]})")
                except Exception:
                    pass
            return metric_meta, metric_samples
        except Exception as e:
            # On SQL errors (not connection errors), return connection to pool
            # Connection errors are already handled in the retry logic above
            if conn_wrapper and not force_new_connection:
                try:
                    connection_pool.put(conn_wrapper)  # Return to temp_pool
                    logging.debug(f"Returned connection to pool after SQL error: {e}")
                except Exception as pool_exc:
                    logging.error(f"Failed to return connection to pool after SQL error: {pool_exc}")
                    # If we can't return to pool, close it
                    try:
                        conn_wrapper.conn.close()
                        # Decrement connection count
                        if data_source_name and data_source_name in connection_counts:
                            connection_counts[data_source_name] -= 1
                            logging.debug(f"Decremented connection count after failed pool return (count: {connection_counts[data_source_name]})")
                    except Exception:
                        pass
            elif conn_wrapper and force_new_connection:
                # If force_new_connection, close it as intended
                try:
                    conn_wrapper.conn.close()
                    # Decrement connection count
                    if data_source_name and data_source_name in connection_counts:
                        connection_counts[data_source_name] -= 1
                        logging.debug(f"Decremented connection count after closing in exception handler (count: {connection_counts[data_source_name]})")
                except Exception:
                    pass
            raise e
        finally:
            # Always close cursor to prevent Teradata sessions from staying in responding state
            if cursor:
                try:
                    cursor.close()
                    logging.debug(f"Cursor closed in finally block for SQL: {sql}")
                except Exception as cursor_close_exc:
                    logging.warning(f"Failed to close cursor in finally block for SQL '{sql}': {cursor_close_exc}")

    # Collect all metric samples and meta from all queries
    all_metric_meta = {}
    all_metric_samples = defaultdict(list)
    errors = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_query = {executor.submit(execute_query, q): q for q in query_defs}
        try:
            for future in as_completed(future_to_query, timeout=effective_timeout):
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
connection_counts = {}  # Track total number of connections per data source

def get_or_create_pool(data_source_name, conn_config, max_pool_size):
    """
    Get or create a shared connection pool for the given data source.
    """
    global connection_pools, pool_locks, connection_counts
    if data_source_name not in connection_pools:
        connection_pools[data_source_name] = Queue(maxsize=max_pool_size)
        pool_locks[data_source_name] = Lock()
        connection_counts[data_source_name] = 0
    return connection_pools[data_source_name], pool_locks[data_source_name]

def acquire_connections(connection_pool, pool_lock, num_needed, pool_timeout=30, conn_config=None, pool_size=None, connection_count=None, data_source_name=None):
    """
    Acquire up to num_needed live connections from the pool, replacing dead/expired ones with new connections.
    Returns a list of connections.
    """
    global connection_counts
    acquired = []
    attempts = 0
    while len(acquired) < num_needed and attempts < num_needed * 2:
        attempts += 1
        try:
            conn_wrapper = connection_pool.get(block=True, timeout=pool_timeout)
            # Check connection health
            alive = is_connection_alive(conn_wrapper.conn)
            
            if not alive:
                logging.info(f"Discarding pooled connection (alive={alive})")
                try:
                    conn_wrapper.conn.close()
                    # Decrement connection count when closing
                    if data_source_name and data_source_name in connection_counts:
                        connection_counts[data_source_name] -= 1
                        logging.debug(f"Connection count for '{data_source_name}': {connection_counts[data_source_name]}")
                except Exception as e:
                    logging.warning(f"Error closing dead/expired connection: {e}")
                # Replace with new connection
                if conn_config:
                    try:
                        new_conn = connect_with_retries(conn_config)
                        acquired.append(PooledConnection(new_conn))
                        # Increment connection count when creating (replacement)
                        if data_source_name:
                            connection_counts[data_source_name] += 1
                            logging.info(f"Replaced dead connection with new live connection (total: {connection_counts[data_source_name]})")
                    except Exception as e:
                        logging.error(f"Failed to create new connection after discarding dead: {e}")
                continue
            # Connection is good, use it
            acquired.append(conn_wrapper)
        except queue.Empty:
            # Timeout reached waiting for pool connection
            logging.warning(f"Timed out after {pool_timeout}s waiting for connection from pool (acquired {len(acquired)}/{num_needed})")
            break
    
    # If not enough connections, check if we can create new ones
    if len(acquired) < num_needed:
        # Check if we're at max capacity using total connection count
        current_count = connection_counts.get(data_source_name, 0) if data_source_name else 0
        if pool_size is not None and current_count >= pool_size:
            # At max capacity, cannot create more connections
            raise Exception(f"No connections available in pool within scrape timeout ({pool_timeout}s). Pool at max capacity ({pool_size}, currently {current_count} connections).")
        
        # Pool not at max, create new connections to fill the gap
        while len(acquired) < num_needed and conn_config:
            current_count = connection_counts.get(data_source_name, 0) if data_source_name else 0
            if pool_size is not None and current_count >= pool_size:
                logging.warning(f"Reached max connections ({pool_size}) while creating new connections")
                break
            try:
                new_conn = connect_with_retries(conn_config)
                acquired.append(PooledConnection(new_conn))
                # Increment connection count when creating
                if data_source_name:
                    connection_counts[data_source_name] += 1
                    logging.info(f"Created new connection to meet required pool size (total: {connection_counts[data_source_name]})")
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

    # Use escaping=allow-utf-8 to match the old exporter behavior (allows UTF-8 in label values)
    content_type = 'text/plain; version=0.0.4; charset=utf-8; escaping=allow-utf-8'
    accept_enc = request.headers.get('Accept-Encoding', '') or ''
    logging.debug(f"Client Accept-Encoding header: '{accept_enc}'")
    # Always advertise that the response varies by Accept-Encoding so proxies
    # don't cache the wrong representation.
    vary_header = 'Accept-Encoding'
    if 'gzip' in accept_enc.lower():
        logging.debug("Client supports gzip; attempting to compress response")
        try:
            compressed = gzip.compress(body_bytes)
            logging.debug(f"Compressed response: {len(body_bytes)} -> {len(compressed)} bytes")
            resp = Response(compressed, status=status)
            resp.headers['Content-Encoding'] = 'gzip'
            resp.headers['Content-Type'] = content_type
            resp.headers['Content-Length'] = str(len(compressed))
            resp.headers['Vary'] = vary_header
            resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            return resp
        except Exception as e:
            logging.warning(f"Gzip compression failed: {e}; sending uncompressed response")
            # fallback to uncompressed
            pass

    logging.debug("Sending uncompressed response")
    resp = Response(body_bytes, status=status)
    resp.headers['Content-Type'] = content_type
    resp.headers['Content-Length'] = str(len(body_bytes))
    resp.headers['Vary'] = 'Accept-Encoding'
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp

@app.route('/metrics')
def metrics():
    exporter = request.args.get('exporter')
    # Log incoming request headers at DEBUG for troubleshooting Prometheus scrapes.
    try:
        hdrs = {k: ('***' if k.lower() in ('authorization', 'proxy-authorization') else v) for k, v in request.headers.items()}
        logging.debug(f"Incoming request from {request.remote_addr}, path={request.path}, headers={hdrs}")
    except Exception as e:
        logging.debug(f"Failed to serialize request headers for debug logging: {e}")
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
        scrape_timeout_offset = parse_duration(global_config.get('scrape_timeout_offset', '0'))
        # Load connection config from settings.yml using data_source_name
        data_source_name = config['target']['data_source_name']
        settings_path = Path(__file__).parent / 'settings.yml'
        conn_config = get_connection_config_from_settings(data_source_name, settings_path)
        
        # Log pool state before scrape
        pool_size = max(1, conn_config.get('max_connections', 1))
        connection_pool, pool_lock = get_or_create_pool(data_source_name, conn_config, pool_size)
        with pool_lock:
            pool_actual = connection_pool.qsize()
            count_tracked = connection_counts.get(data_source_name, 0)
            logging.info(f"Pool state for '{data_source_name}': tracked={count_tracked}, in_pool={pool_actual}, max={pool_size}")
            # If count is higher than pool size, reset it
            if count_tracked > pool_size:
                logging.warning(f"Connection count ({count_tracked}) exceeds pool size ({pool_size}), resetting to pool size")
                connection_counts[data_source_name] = pool_actual

        # ✅ Extract global settings
        global_config = config.get('global', {})
        # Ensure exporter max connections is at least 1 to avoid zero-worker situations
        exporter_max_conn = max(1, global_config.get('max_connections', 1))
        max_idle = global_config.get('max_idle_connections', 1)
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
            # Use a short timeout for acquiring connections (5 seconds) instead of full scrape timeout
            # This prevents blocking when connections are held by background workers from previous scrapes
            connection_acquire_timeout = 10
            acquired_conns = acquire_connections(connection_pool, pool_lock, num_to_acquire, pool_timeout=connection_acquire_timeout, conn_config=conn_config, pool_size=pool_size, data_source_name=data_source_name)
            temp_pool = Queue(maxsize=num_to_acquire)
            for conn in acquired_conns:
                temp_pool.put(conn)

        timeout_occurred = False
        try:
            # Enforce scrape timeout using effective_timeout
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_queries, dsn, queries, temp_pool, max_idle, tz, conn_config, effective_timeout)
                try:
                    metrics_output = future.result(timeout=effective_timeout)
                except TimeoutError:
                    target_name = config['target'].get('name', 'unknown')
                    logging.error(f"Scrape exceeded timeout of {effective_timeout} seconds for exporter='{exporter}', target='{target_name}'")
                    # Note: Don't return here - let finally block return connections to pool
                    # Worker threads may still be using connections, but we need to ensure cleanup
                    metrics_output = [f'up{{target="{target_name}",exporter="{exporter}"}} 0', f'error{{target="{target_name}",exporter="{exporter}",message="scrape_timeout"}} 1']
                    timeout_occurred = True
        finally:
            # CRITICAL: Always return connections from temp_pool back to main pool
            # This runs even on timeout/error to prevent connection leaks
            with pool_lock:
                returned_count = 0
                while not temp_pool.empty():
                    conn_wrapper = temp_pool.get()
                    # Check if main pool has space (respecting max_idle)
                    if connection_pool.qsize() < max_idle:
                        try:
                            connection_pool.put_nowait(conn_wrapper)
                            returned_count += 1
                        except queue.Full:
                            # Pool is full, close this connection
                            logging.info(f"Main pool full (size={connection_pool.qsize()}), closing connection instead of returning")
                            try:
                                conn_wrapper.conn.close()
                                # Decrement connection count
                                if data_source_name in connection_counts:
                                    connection_counts[data_source_name] -= 1
                                    logging.debug(f"Connection count for '{data_source_name}': {connection_counts[data_source_name]}")
                            except Exception as e:
                                logging.error(f"Failed to close connection: {e}")
                    else:
                        # Pool already at max_idle, close this connection
                        logging.info(f"Main pool at max_idle ({max_idle}), closing excess connection")
                        try:
                            conn_wrapper.conn.close()
                            # Decrement connection count
                            if data_source_name in connection_counts:
                                connection_counts[data_source_name] -= 1
                                logging.debug(f"Connection count for '{data_source_name}': {connection_counts[data_source_name]}")
                        except Exception as e:
                            logging.error(f"Failed to close excess connection: {e}")
                if returned_count > 0:
                    logging.debug(f"Returned {returned_count} connections to main pool from temp_pool")

        # If timeout occurred, return early with error response
        if 'timeout_occurred' in locals() and timeout_occurred:
            return make_text_response("\n".join(metrics_output), status=504)

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
        # Note: Don't clean up temp_pool here either - let workers finish naturally
        # Worker threads may still be using connections from temp_pool
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
                        # Increment connection count
                        connection_counts[data_source_name] += 1
                        logging.info(f"Pre-populated connection for data source '{data_source_name}' (total: {connection_counts[data_source_name]})")
                    except Exception as e:
                        tb = traceback.format_exc()
                        logging.error(f"Failed to pre-populate connection for '{data_source_name}': {e}\n{tb}")
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Error initializing connection pools: {e}\n{tb}")

# Call this at startup
initialize_connection_pools()