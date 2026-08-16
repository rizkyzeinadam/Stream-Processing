#!/usr/bin/env python3
"""
Metrics Collector for Stream Processing Experiments
Collects metrics from Prometheus and logs them to CSV
"""

import requests
import time
import csv
import logging
import argparse
import math
from datetime import datetime
from typing import Dict, List, Any
import json
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/metrics_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collect metrics from Prometheus"""
    
    def __init__(self, prometheus_url: str = 'http://localhost:9090'):
        """Initialize metrics collector"""
        self.prometheus_url = prometheus_url
        self.metrics = []
        self.start_time = None
        self.recovery_start_time = None
        self.last_recovery_time = 0.0
        self.default_load_rate = 100
        logger.info(f"Connected to Prometheus at {prometheus_url}")

    def check_prometheus_health(self, retries: int = 5, delay: float = 5.0) -> bool:
        """Return True if Prometheus is reachable, retrying a few times."""
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(f"{self.prometheus_url}/-/healthy", timeout=10)
                if response.status_code == 200:
                    logger.info(f"Prometheus healthy (attempt {attempt})")
                    return True
            except Exception as e:
                logger.error(f"Health check attempt {attempt}/{retries} FAILED: {type(e).__name__}: {e}")
            time.sleep(delay)
        logger.error(f"Prometheus unreachable after {retries} attempts")
        return False

    def query_prometheus(self, query: str) -> Any:
        """Query Prometheus for metrics"""
        try:
            response = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params={'query': query},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error querying Prometheus: {e}")
            return None

    def query_first_value(self, queries: List[str]) -> float:
        """Try multiple Prometheus queries and return the first available numeric value."""
        for query in queries:
            result = self.query_prometheus(query)
            if result and result.get('data', {}).get('result'):
                try:
                    value = float(result['data']['result'][0]['value'][1])
                    if math.isfinite(value):
                        return value
                except (IndexError, ValueError, KeyError, TypeError):
                    continue
        return math.nan
    
    def query_range(self, query: str, start: float, end: float, step: str = '15s') -> Any:
        """Query Prometheus for metric range"""
        try:
            response = requests.get(
                f"{self.prometheus_url}/api/v1/query_range",
                params={
                    'query': query,
                    'start': int(start),
                    'end': int(end),
                    'step': step
                },
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error querying Prometheus range: {e}")
            return None
    
    def get_throughput(self) -> float:
        """Get current throughput (messages/sec)."""
        return self.query_first_value([
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)',
            'sum(flink_taskmanager_job_task_operator_numRecordsInPerSecond)',
            'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond)',
            'sum(flink_taskmanager_job_task_numRecordsInPerSecond)',
            'sum(flink_taskmanager_job_task_numRecordsOutPerSecond)',
            'sum(flink_taskmanager_job_task_operator_numRecordsInPerSecond)',
            'sum(spark_streaming_throughput)',
            'sum(spark_streaming_throughput_worker1 + spark_streaming_throughput_worker2)',
            'sum(rate(kafka_topic_partition_current_offset[30s]))',
        ])

    
    def get_latency(self) -> float:
        """Get average latency (ms)."""
        return self.query_first_value([
            'avg(flink_taskmanager_job_task_operator_fetch_latency_avg)',
            'avg(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency)',
            'avg(flink_taskmanager_job_task_operator_latency)',
            'avg(flink_taskmanager_job_latency)',
            'avg(flink_taskmanager_job_task_operator_average_record_delay_millis)',
            'avg(spark_streaming_latency)',
            'avg(spark_streaming_latency_average)',
        ])
    
    def get_cpu_usage(self) -> float:
        """Get aggregated CPU usage for the active framework via cAdvisor container metrics."""
        framework = self.detect_framework()
        cpu_metrics = self.get_framework_cpu(framework)
        total = float(cpu_metrics.get('total', math.nan))
        return total if math.isfinite(total) else math.nan

    def get_memory_usage(self) -> float:
        """Get aggregated memory usage in MiB for the active framework via cAdvisor container metrics."""
        framework = self.detect_framework()
        memory_metrics = self.get_framework_memory(framework)
        total = float(memory_metrics.get('total', math.nan))
        return total if math.isfinite(total) else math.nan

    def detect_framework(self) -> str:
        """Infer the active framework from Prometheus scrape jobs.

        The Flink Prometheus config exposes job 'flink'; the Spark config exposes
        'spark-master'/'spark-worker1'/'spark-worker2'/'spark-app'. Keying off the
        `up{}` targets (rather than a data metric) avoids misdetection when a metric
        name differs or the stream is momentarily idle.
        """
        if self._has_result('up{job="flink"}'):
            return 'flink'
        if self._has_result('up{job=~"spark-master|spark-worker1|spark-worker2|spark-app"}'):
            return 'spark'
        return 'spark'

    def _has_result(self, query: str) -> bool:
        """Return True if the Prometheus query returns at least one result series."""
        result = self.query_prometheus(query)
        return bool(result and result.get('data', {}).get('result'))

    def get_cpu_per_container(self, container_name: str) -> float:
        """Get CPU usage for a specific container via cAdvisor metrics."""
        queries = [
            f'rate(container_cpu_usage_seconds_total{{name="{container_name}"}}[1m]) * 100',
            f'rate(container_cpu_usage_seconds_total{{container="{container_name}"}}[1m]) * 100',
            f'rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service="{container_name}"}}[1m]) * 100',
        ]
        result = self.query_first_value(queries)
        return result if math.isfinite(result) and result >= 0 else 0.0

    def get_framework_cpu(self, framework: str) -> dict:
        """Get CPU usage for all containers in a framework."""
        containers = {
            'flink': ['flink-jobmanager', 'flink-taskmanager1', 'flink-taskmanager2'],
            'spark': ['spark-driver', 'spark-worker1', 'spark-worker2'],
        }
        result = {}
        for container in containers.get(framework, []):
            result[container] = self.get_cpu_per_container(container)
        cpu_values = list(result.values())
        result['total'] = sum(cpu_values) / len(cpu_values) if cpu_values else math.nan
        return result

    def get_memory_per_container(self, container_name: str) -> float:
        """Get memory usage in MiB for a specific container via cAdvisor.

        Reports absolute usage (not percentage-of-limit) because this cAdvisor
        deployment does not export container_spec_memory_limit_bytes, so a
        limit-based percentage would always be NaN -> 0.
        """
        queries = [
            f'container_memory_usage_bytes{{name="{container_name}"}} / 1048576',
            f'container_memory_usage_bytes{{container="{container_name}"}} / 1048576',
            f'container_memory_usage_bytes{{container_label_com_docker_compose_service="{container_name}"}} / 1048576',
        ]
        result = self.query_first_value(queries)
        return result if math.isfinite(result) and result >= 0 else 0.0

    def get_framework_memory(self, framework: str) -> dict:
        """Get memory usage in MiB for all containers in a framework."""
        containers = {
            'flink': ['flink-jobmanager', 'flink-taskmanager1', 'flink-taskmanager2'],
            'spark': ['spark-driver', 'spark-worker1', 'spark-worker2'],
        }
        result = {}
        for container in containers.get(framework, []):
            result[container] = self.get_memory_per_container(container)
        cpu_values = list(result.values())
        result['total'] = sum(result.values()) if cpu_values else math.nan
        return result
    
    def get_kafka_lag(self) -> float:
        """Get the active backlog for the current framework.

        Flink: (produced offset) - (consumed counter), not records_lead_min
        which over-counts after job restart (observed ~400k false lag).
        NOTE: records_consumed_total resets on job restart, so values after a
        fault are an upper-bound estimate.
        """
        return self.query_first_value([
            'sum(kafka_topic_partition_current_offset{topic="cic-topic"}) - sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total)',
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lead_min)',
            'sum(spark_streaming_backlog)',
            'sum(kafka_consumergroup_lag{group="spark-group"})',
            'sum(kafka_consumergroup_lag{group="flink-group"})',
            'sum(kafka_consumergroup_lag)',
            'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)',
        ])

    def get_recovery_time(self, effective_lag: float) -> float:
        """Estimate recovery time window while backlog exists and preserve last completed recovery."""
        if not math.isfinite(effective_lag):
            return self.last_recovery_time if self.last_recovery_time else 0.0

        now = time.time()
        if effective_lag > 0:
            if self.recovery_start_time is None:
                self.recovery_start_time = now
            return max(now - self.recovery_start_time, 0.001)

        if self.recovery_start_time is not None:
            self.last_recovery_time = now - self.recovery_start_time
            self.recovery_start_time = None

        return self.last_recovery_time
    
    def collect_metrics(self, load_rate: int) -> Dict[str, Any]:
        """Collect all metrics at once."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        throughput = self.get_throughput()
        latency = self.get_latency()
        raw_lag = self.get_kafka_lag()
        kafka_lag = raw_lag if math.isfinite(raw_lag) and raw_lag >= 0 else math.nan
        recovery_time = self.get_recovery_time(kafka_lag) if math.isfinite(kafka_lag) else 0.0
        cpu = self.get_cpu_usage()
        memory = self.get_memory_usage()
        
        metric = {
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'load_rate': load_rate,
            'throughput': throughput,
            'latency': latency,
            'cpu': cpu,
            'memory': memory,
            'kafka_lag': kafka_lag,
            'recovery_time': recovery_time,
        }
        
        self.metrics.append(metric)
        logger.info(f"Collected metrics - Throughput: {metric['throughput']:.1f} msg/s, "
               f"Latency: {metric['latency']:.1f}ms, "
                   f"CPU: {metric['cpu']:.1f}%, "
                   f"Memory: {metric['memory']:.1f} MiB, "
                   f"Lag: {metric['kafka_lag']:.0f}")
        
        return metric

    def get_current_load_rate_from_config(self, elapsed: float, load_config: dict) -> int:
        """Return the load rate for the current elapsed time using load.yaml."""
        if not load_config:
            return self.default_load_rate

        load_pattern = load_config.get('load_pattern', [])
        if not load_pattern:
            return self.default_load_rate

        time_acc = 0
        for stage in load_pattern:
            stage_duration = int(stage.get('duration', 0))
            stage_rate = int(stage.get('rate', self.default_load_rate))
            if elapsed < time_acc + stage_duration:
                return stage_rate
            time_acc += stage_duration

        return int(load_pattern[-1].get('rate', self.default_load_rate))
    
    def save_metrics_csv(self, output_file: str):
        """Save collected metrics to CSV file"""
        if not self.metrics:
            logger.warning("No metrics to save")
            return
        
        try:
            with open(output_file, 'w', newline='') as f:
                fieldnames = [
                    'timestamp', 'elapsed_seconds', 'load_rate', 'throughput',
                    'latency', 'cpu', 'memory', 'kafka_lag', 'recovery_time'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.metrics)
            logger.info(f"Metrics saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving metrics: {e}")
    
    def start_collection(self):
        """Start metrics collection timer"""
        self.start_time = time.time()
        self.metrics = []
        logger.info("Metrics collection started")
    
    def run_continuous_collection(self, duration: int = 120, interval: int = 1, load_rate: int = 100):
        """Continuously collect metrics for a specified duration"""
        logger.info(f"Starting continuous collection for {duration}s with {interval}s interval")
        self.start_collection()
        
        elapsed = 0
        while elapsed < duration:
            self.collect_metrics(load_rate)
            time.sleep(interval)
            elapsed = time.time() - self.start_time
        
        logger.info(f"Continuous collection complete. Collected {len(self.metrics)} metric samples")

    def run_dynamic_collection(self, duration: int = 120, interval: int = 1, load_config: dict = None):
        """Collect metrics using the active load rate from a YAML load config."""
        logger.info(f"Starting dynamic collection for {duration}s with {interval}s interval")
        self.start_collection()

        elapsed = 0
        while elapsed < duration:
            elapsed = time.time() - self.start_time
            current_load_rate = self.get_current_load_rate_from_config(elapsed, load_config)
            self.collect_metrics(current_load_rate)
            time.sleep(interval)

        logger.info(f"Dynamic collection complete. Collected {len(self.metrics)} metric samples")


def main():
    parser = argparse.ArgumentParser(description='Metrics Collector for Stream Processing')
    parser.add_argument('--prometheus', default='http://localhost:9090', help='Prometheus URL')
    parser.add_argument('--output', required=True, help='Output CSV file')
    parser.add_argument('--duration', type=int, default=120, help='Collection duration in seconds')
    parser.add_argument('--interval', type=int, default=1, help='Collection interval in seconds')
    parser.add_argument('--load-rate', type=int, default=100, help='Current load rate (msg/s)')
    parser.add_argument('--load-config', help='Path to load.yaml for dynamic load rate')
    
    args = parser.parse_args()
    
    collector = MetricsCollector(prometheus_url=args.prometheus)
    if not collector.check_prometheus_health():
        logger.error(
            f"Prometheus is unreachable at {args.prometheus}. "
            "Start the monitoring stack first (e.g. `bash scripts/run_experiment.sh spark` "
            "or `bash scripts/run_experiment.sh flink`) before collecting metrics."
        )
        raise SystemExit(1)

    if args.load_config:
        try:
            with open(args.load_config, 'r', encoding='utf-8') as file_handle:
                load_config = yaml.safe_load(file_handle) or {}
        except Exception as exc:
            logger.error(f"Error loading dynamic load config {args.load_config}: {exc}")
            raise SystemExit(1)

        collector.run_dynamic_collection(
            duration=args.duration,
            interval=args.interval,
            load_config=load_config,
        )
    else:
        collector.run_continuous_collection(
            duration=args.duration,
            interval=args.interval,
            load_rate=args.load_rate,
        )
    collector.save_metrics_csv(args.output)


if __name__ == '__main__':
    main()
