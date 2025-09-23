import yaml
import glob
import time
import logging
from pathlib import Path
from flask import Flask, request, Response
import teradatasql

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
            collector_name = collector.get('name')
            logging.debug(f"Evaluating collector '{collector_name}' from file: {file_path}")
            if collector_name and any(glob.fnmatch.fnmatch(collector_name, pattern) for pattern in config['target']['collectors']):
                matched_collectors.append((collector_name, collector))

    logging.info(f"Matched collectors: {[name for name, _ in matched_collectors]}")
    return matched_collectors

def load_queries_from_collectors(matched_collectors):
    queries = []
    for name, collector in matched_collectors:
        logging.info(f"Loading collector: {name}")
        queries.extend(collector.get('queries', []))
    return queries

def run_queries(dsn_dict, queries):
    logging.info("Connecting to Teradata with DSN")
    logging.debug(f"DSN dict: {dsn_dict}")
    conn = teradatasql.connect(**dsn_dict)
    cursor = conn.cursor()
    metrics = []

    for query_def in queries:
        logging.info(f"Executing query: {query_def['sql']}")
        cursor.execute(query_def['sql'])
        for row in cursor.fetchall():
            logging.debug(f"Query result row: {row}")
            labels = ",".join(f'{label}="{row[i]}"' for i, label in enumerate(query_def.get('labels', [])))
            value = row[len(query_def.get('labels', []))]
            metric = f"{query_def['metric_name']}{{{labels}}} {value}"
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
