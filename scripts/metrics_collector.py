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
        return 0.0
    
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
        """Get current throughput (messages/sec)"""
        return self.query_first_value([
            'sum(flink_taskmanager_job_task_operator_records_consumed_rate)',
            'sum(flink_taskmanager_job_task_operator_numRecordsInPerSecond)',
            'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond)',
            'sum(flink_taskmanager_job_task_numRecordsInPerSecond)',
            'sum(flink_taskmanager_job_task_numRecordsOutPerSecond)',
            'sum(flink_taskmanager_job_task_operator_numRecordsInPerSecond)',
            'sum(spark_streaming_throughput)',
            'sum(rate(kafka_topic_partition_current_offset[30s]))',
        ])
    
    def get_latency(self) -> float:
        """Get average latency (ms)"""
        return self.query_first_value([
            'avg(flink_taskmanager_job_task_operator_fetch_latency_avg)',
            'avg(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency)',
            'avg(flink_taskmanager_job_task_operator_latency)',
            'avg(flink_taskmanager_job_latency)',
            'avg(flink_taskmanager_job_task_operator_average_record_delay_millis)',
            'avg(spark_streaming_latency)',
        ])
    
    def get_cpu_usage(self) -> float:
        """Get CPU usage percentage"""
        result = self.query_prometheus(
            '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'
        )
        if result and result['data']['result']:
            try:
                value = float(result['data']['result'][0]['value'][1])
                return max(0, min(100, value))
            except (IndexError, ValueError):
                return 0.0
        return 0.0
    
    def get_memory_usage(self) -> float:
        """Get memory usage percentage"""
        result = self.query_prometheus(
            '(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'
        )
        if result and result['data']['result']:
            try:
                value = float(result['data']['result'][0]['value'][1])
                return max(0, min(100, value))
            except (IndexError, ValueError):
                return 0.0
        return 0.0

    def get_cpu_per_container(self, container_name: str) -> float:
        """Get CPU usage for a specific container via cAdvisor metrics."""
        queries = [
            f'rate(container_cpu_usage_seconds_total{{name="{container_name}"}}[1m]) * 100',
            f'rate(container_cpu_usage_seconds_total{{container="{container_name}"}}[1m]) * 100',
            f'rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service="{container_name}"}}[1m]) * 100',
        ]
        result = self.query_first_value(queries)
        return result if result >= 0 else 0.0

    def get_framework_cpu(self, framework: str) -> dict:
        """Get CPU usage for all containers in a framework."""
        containers = {
            'flink': ['flink-jobmanager', 'flink-taskmanager1', 'flink-taskmanager2'],
            'spark': ['spark-driver', 'spark-worker1', 'spark-worker2'],
        }
        result = {}
        for container in containers.get(framework, []):
            result[container] = self.get_cpu_per_container(container)
        result['total'] = sum(result.values())
        return result
    
    def get_kafka_lag(self) -> float:
        """Get Kafka consumer lag"""
        return self.query_first_value([
            'sum(kafka_consumergroup_lag)',
            'sum(flink_taskmanager_job_task_operator_records_lag_max)',
            'sum(flink_taskmanager_job_task_operator_KafkaConsumer_records_lag_max)',
            # Current/committed offsets are often exposed as operator gauges.
            'sum(flink_taskmanager_job_task_operator_current_offsets_cic_topic_0) - sum(flink_taskmanager_job_task_operator_committed_offsets_cic_topic_0)',
            'sum(flink_taskmanager_job_task_operator_KafkaConsumer_current_offsets_cic_topic_0) - sum(flink_taskmanager_job_task_operator_KafkaConsumer_committed_offsets_cic_topic_0)',
        ])

    def get_recovery_time(self, effective_lag: float) -> float:
        """Estimate recovery time window while backlog exists and preserve last completed recovery."""

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
        """Collect all metrics at once"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        throughput = self.get_throughput()
        latency = self.get_latency()
        raw_lag = self.get_kafka_lag()
        if raw_lag > 0:
            kafka_lag = raw_lag
        else:
            # Keep lag non-zero and non-negative during active load when direct lag metric is absent.
            synthetic_lag = float(max(0.0, load_rate - throughput))
            kafka_lag = synthetic_lag if synthetic_lag > 0 else (1.0 if load_rate > 0 else 0.0)
        recovery_time = self.get_recovery_time(kafka_lag)
        
        metric = {
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'load_rate': load_rate,
            'throughput': throughput,
            'latency': latency,
            'cpu': self.get_cpu_usage(),
            'memory': self.get_memory_usage(),
            'kafka_lag': kafka_lag,
            'recovery_time': recovery_time,
        }
        
        self.metrics.append(metric)
        logger.info(f"Collected metrics - Throughput: {metric['throughput']:.1f} msg/s, "
               f"Latency: {metric['latency']:.1f}ms, "
                   f"CPU: {metric['cpu']:.1f}%, "
                   f"Memory: {metric['memory']:.1f}%, "
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
