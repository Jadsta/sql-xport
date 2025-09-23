import yaml
import glob
import time
from pathlib import Path
from flask import Flask, request, Response
import teradatasql

app = Flask(__name__)

def load_sql_exporter_config(exporter_name):
    config_path = Path(f"./{exporter_name}/sql_exporter.yml")
    with config_path.open() as f:
        return yaml.safe_load(f)

def build_dsn(conn_config):
    # Safely wrap values in quotes to handle special characters
    return f'host="{conn_config["host"]}";user="{conn_config["user"]}";password="{conn_config["password"]}"'

def resolve_collectors(config):
    collector_files = []
    for pattern in config.get('collector_files', []):
        collector_files.extend(glob.glob(f"./{pattern}"))

    matched_collectors = []
    for file_path in collector_files:
        name = Path(file_path).stem.replace(".collector", "")
        if any(glob.fnmatch.fnmatch(name, pattern) for pattern in config['target']['collectors']):
            matched_collectors.append(file_path)

    return matched_collectors

def load_queries_from_collectors(file_paths):
    queries = []
    for path in file_paths:
        with open(path) as f:
            collector = yaml.safe_load(f)
            queries.extend(collector.get('queries', []))
    return queries

def run_queries(dsn, queries):
    conn = teradatasql.connect(dsn)
    cursor = conn.cursor()
    metrics = []

    for query_def in queries:
        cursor.execute(query_def['sql'])
        for row in cursor.fetchall():
            labels = ",".join(f'{label}="{row[i]}"' for i, label in enumerate(query_def.get('labels', [])))
            value = row[len(query_def.get('labels', []))]
            metric = f"{query_def['metric_name']}{{{labels}}} {value}"
            metrics.append(metric)

    conn.close()
    return metrics

@app.route('/metrics')
def metrics():
    exporter = request.args.get('exporter')
    if not exporter:
        return "Missing 'exporter' parameter", 400

    try:
        config = load_sql_exporter_config(exporter)
        start = time.time()

        collector_files = resolve_collectors(config)
        queries = load_queries_from_collectors(collector_files)

        conn_config = config['target']['connection']
        dsn = build_dsn(conn_config)

        metrics_output = run_queries(dsn, queries)

        duration = time.time() - start
        target_name = config['target'].get('name', 'unknown')

        metrics_output.append(f'up{{target="{target_name}"}} 1')
        metrics_output.append(f'scrape_duration_seconds{{target="{target_name}"}} {duration:.3f}')

        return Response("\n".join(metrics_output), mimetype='text/plain')

    except Exception as e:
        return Response(f'up{{target="unknown"}} 0\nerror{{message="{str(e)}"}} 1', mimetype='text/plain', status=500)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
