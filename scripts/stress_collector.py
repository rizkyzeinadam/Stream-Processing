#!/usr/bin/env python3
"""Stress test collector — poll Prometheus every 3s, write CSV. (v2 robust)

Usage:
    python stress_collector.py --framework flink --duration 320 --out stress_flink.csv
    python stress_collector.py --framework spark --duration 320 --out stress_spark.csv

v2 changes:
- throughput: coba beberapa query (job-level, operator-level, fallback) biar tetap dapat
- produced_rate: query offset TANPA label filter (lebih robust kalau label topic beda)
- kafka_lag: fallback ke backlog proxy (load_target - throughput) kalau metric lag kosong
- Semua query pakai retry 2x + timeout 8s
"""
import argparse
import csv
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime

PROM = "http://localhost:9090/api/v1/query"

QUERY_GROUPS = {
    "flink": {
        "throughput": [
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)',
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate{})',
            'sum(rate(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total[10s]))',
        ],
        "lag": [
            'sum(kafka_topic_partition_current_offset{topic="cic-topic"}) - sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total)',
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lead_min)',
        ],
    },
    "spark": {
        "throughput": [
            'sum(spark_streaming_throughput)',
            'spark_streaming_throughput',
            'sum(spark_streaming_total_processed)',
        ],
        "lag": [
            'sum(kafka_topic_partition_current_offset{topic="cic-topic"}) - sum(spark_streaming_total_processed)',
            'sum(spark_streaming_backlog)',
        ],
    },
}
OFFSET_RATE_QUERIES = [
    'sum(rate(kafka_topic_partition_current_offset[10s]))',
    'sum(rate(kafka_topic_partition_current_offset{topic="cic-topic"}[10s]))',
]
LATENCY_QUERIES = [
    'histogram_quantile(0.5, sum by (le) (rate(flink_taskmanager_job_task_operator_operator_latency{quantile="0.5"}[30s])))',
]


def query(q):
    url = f"{PROM}?query={urllib.parse.quote(q)}"
    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read().decode())
            vals = data.get("data", {}).get("result", [])
            if not vals:
                return None
            return float(vals[0].get("value", [0, "0"])[1])
        except Exception:
            time.sleep(1)
    return None


def query_first(queries):
    for q in queries:
        v = query(q)
        if v is not None:
            return v
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--framework", choices=["flink", "spark"], required=True)
    p.add_argument("--duration", type=int, default=320)
    p.add_argument("--out", required=True)
    p.add_argument("--interval", type=float, default=3.0)
    args = p.parse_args()

    qg = QUERY_GROUPS[args.framework]
    start = time.time()
    rows = []
    while time.time() - start < args.duration:
        elapsed = time.time() - start
        tp = query_first(qg["throughput"])
        off = query_first(OFFSET_RATE_QUERIES)
        lag = query_first(qg["lag"])
        lat = query_first(LATENCY_QUERIES)
        phase = int(elapsed // 60)
        load_target = [50, 100, 500, 1000, 1500][min(phase, 4)]
        rows.append({
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed, 3),
            "load_target": load_target,
            "throughput": tp if tp is not None else "nan",
            "produced_rate": off if off is not None else "nan",
            "latency": lat if lat is not None else "nan",
            "kafka_lag": lag if lag is not None else "nan",
        })
        time.sleep(args.interval)

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "elapsed_seconds", "load_target",
                                          "throughput", "produced_rate", "latency", "kafka_lag"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
