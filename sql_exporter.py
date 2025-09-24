import yaml                     # For loading .yml config and collector files
import glob                     # For resolving collector file patterns
import time                     # For timestamps and connection lifetime tracking
import logging                  # For structured logging
from pathlib import Path        # For clean file path handling
from flask import Flask, request, Response  # For HTTP metrics endpoint
import teradatasql              # For connecting to Teradata
import datetime                 # For handling datetime values and formatting
from threading import Semaphore # For limiting concurrent connections
from queue import Queue         # For managing idle connection pool



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
        
def run_queries(dsn_dict, queries, connection_pool, max_idle, max_lifetime):
    import datetime
    import time
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
            logging.info("Opening new DB connection")
            conn = teradatasql.connect(**dsn_dict)
            conn_wrapper = PooledConnection(conn)

        cursor = conn_wrapper.conn.cursor()
        metrics = []

        for query_def in queries:
            metric_name = query_def['metric_name']
            help_text = query_def.get('help', '')
            metric_type = query_def.get('type', 'gauge')

            metrics.append(f"# HELP {metric_name} {help_text}")
            metrics.append(f"# TYPE {metric_name} {metric_type}")

            logging.info(f"Executing query: {query_def['sql']}")
            cursor.execute(query_def['sql'])

            for row in cursor.fetchall():
                logging.debug(f"Query result row: {row}")
                labels = []
                
                label_indexes = [
                    i for i, col in enumerate(cursor.description)
                    if col[0].lower() in [l.lower() for l in query_def.get('labels', [])]
                ]

                for i in label_indexes:
                    col_name = cursor.description[i][0]
                    labels.append(f'{col_name}="{row[i]}"')

                for k, v in query_def.get('static_labels', {}).items():
                    labels.append(f'{k}="{v}"')

                value_index = len(query_def.get('labels', []))
                value = query_def.get('static_value', None)

                if value is None and query_def.get('values'):
                    value_column = query_def['values'][0]
                    try:
                        value_index = cursor.description.index(next(d for d in cursor.description if d[0].lower() == value_column.lower()))
                        raw_value = row[value_index]
                        value = format_value(raw_value)
                    except Exception as e:
                        logging.warning(f"Could not resolve value column '{value_column}' for metric {metric_name}: {e}")
                        value = "0"

                timestamp = ""
                ts_col = query_def.get('timestamp_value')
                if ts_col:
                    try:
                        # Match column name to index using cursor.description
                        ts_index = next(i for i, col in enumerate(cursor.description) if col[0].lower() == ts_col.lower())
                        ts_raw = row[ts_index]
                
                        if isinstance(ts_raw, datetime.datetime):
                            # Match Go exporter: milliseconds since epoch
                            timestamp = f" {int(ts_raw.timestamp() * 1000)}"
                        elif isinstance(ts_raw, datetime.date):
                            dt = datetime.datetime.combine(ts_raw, datetime.time())
                            timestamp = f" {int(dt.timestamp() * 1000)}"
                        else:
                            logging.warning(f"Timestamp column '{ts_col}' is not datetime/date for metric {metric_name}")
                    except Exception as e:
                        logging.warning(f"Could not extract timestamp for {metric_name}: {e}")

                metric = f"{metric_name}{{{','.join(labels)}}} {value}{timestamp}"
                metrics.append(metric)

        # Return to pool if under idle limit
        if connection_pool.qsize() < max_idle:
            connection_pool.put(conn_wrapper)
        else:
            logging.info("Idle pool full, closing connection")
            conn_wrapper.conn.close()

        return metrics

    except Exception as e:
        if conn_wrapper:
            conn_wrapper.conn.close()
        raise e

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
        max_conn = global_config.get('max_connections', 1)
        max_idle = global_config.get('max_idle_connections', 1)
        max_lifetime = parse_duration(global_config.get('max_connection_lifetime', '0'))

        # ✅ Initialize connection pool
        connection_pool = Queue(maxsize=max_conn)

        matched_collectors = resolve_collectors(config, base_dir)
        queries = load_queries_from_collectors(matched_collectors)

        conn_config = config['target']['connection']
        dsn = build_dsn(conn_config)

        # ✅ Pass pool and limits into run_queries
        metrics_output = run_queries(dsn, queries, connection_pool, max_idle, max_lifetime)

        duration = time.time() - start
        target_name = config['target'].get('name', 'unknown')

        metrics_output.append(f'up{{target="{target_name}"}} 1')
        metrics_output.append(f'scrape_duration_seconds{{target="{target_name}"}} {duration:.3f}')

        logging.info(f"Scrape completed in {duration:.3f} seconds")
        return Response("\n".join(metrics_output), mimetype='text/plain')

    except Exception as e:
        logging.error(f"Error during scrape: {str(e)}")
        return Response(f'up{{target="unknown"}} 0\nerror{{message="{str(e)}"}} 1', mimetype='text/plain', status=500)

if __name__ == '__main__':
    logging.info("Starting SQL Exporter on port 8000")
    app.run(host='0.0.0.0', port=8000)
