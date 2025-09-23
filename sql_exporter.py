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

def load_sql_exporter_config(exporter_name):
    config_path = Path(f"./{exporter_name}/sql_exporter.yml")
    logging.info(f"Loading config from: {config_path}")
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

def run_queries(dsn_dict, queries):
    logging.info("Connecting to Teradata with DSN")
    logging.debug(f"DSN dict: {dsn_dict}")
    conn = teradatasql.connect(**dsn_dict)
    cursor = conn.cursor()
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

            for i, label in enumerate(query_def.get('labels', [])):
                labels.append(f'{label}="{row[i]}"')

            for k, v in query_def.get('static_labels', {}).items():
                labels.append(f'{k}="{v}"')

            value_index = len(query_def.get('labels', []))
            value = query_def.get('static_value', None)

            if value is None and query_def.get('values'):
                raw_value = row[value_index]
                value = format_value(raw_value)
            elif value is not None:
                value = format_value(value)

            timestamp = ""
            ts_col = query_def.get('timestamp_value')
            if ts_col:
                try:
                    ts_index = query_def['labels'].index(ts_col) if ts_col in query_def['labels'] else value_index + 1
                    ts_raw = row[ts_index]
                    if isinstance(ts_raw, datetime.datetime):
                        timestamp = f" {ts_raw.timestamp():.9e}"
                except Exception as e:
                    logging.warning(f"Could not extract timestamp for {metric_name}: {e}")

            metric = f"{metric_name}{{{','.join(labels)}}} {value}{timestamp}"
            metrics.append(metric)

    conn.close()
    logging.info("Connection closed")
    return metrics





@app.route('/metrics')
def metrics():
    exporter = request.args.get('exporter')
    if not exporter:
        logging.warning("Missing 'exporter' parameter in request")
        return "Missing 'exporter' parameter", 400

    try:
        config, base_dir = load_sql_exporter_config(exporter)
        start = time.time()

        matched_collectors = resolve_collectors(config, base_dir)
        queries = load_queries_from_collectors(matched_collectors)

        conn_config = config['target']['connection']
        dsn = build_dsn(conn_config)

        metrics_output = run_queries(dsn, queries)

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
