#!/usr/bin/env python3
"""Dynamic Load Generator for Kafka with deterministic parquet replay."""

import argparse
import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

import yaml
from kafka import KafkaProducer

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - runtime dependency check
    pq = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DynamicLoadGenerator:
    """Generate dynamic load and replay parquet rows to Kafka."""

    def __init__(self, broker: str = 'kafka:29092', topic: str = 'cic-topic', config_file: Optional[str] = None):
        """Initialize producer and experiment configuration."""
        self.broker = broker
        self.topic = topic
        self.message_count = 0
        self.start_time = None
        self.load_config = None
        self.config_path: Optional[Path] = None
        self.dataset_path: Optional[Path] = None
        self.dataset_enabled = False
        self.dataset_iter: Optional[Iterator[Dict[str, Any]]] = None
        self.dataset_row_index = 0
        
        # Load configuration from YAML if provided
        if config_file:
            self.config_path = Path(config_file).resolve()
            self.load_config = self.load_yaml_config(config_file)
            logger.info(f"Loaded configuration from {config_file}")
            self.topic = self.load_config.get('kafka_topic', topic)
        else:
            self.topic = topic

        self._load_dataset_if_configured()
        
        # Kafka producer configuration
        self.producer = self._create_producer()
        logger.info(f"Connected to Kafka broker: {broker}")

    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer without compression."""
        producer_kwargs = {
            'bootstrap_servers': self.broker,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'linger_ms': 5,
            'batch_size': 16384,
            'retries': 5,
            'acks': 'all',
        }

        logger.info("Kafka producer started without compression")
        return KafkaProducer(**producer_kwargs)
    
    @staticmethod
    def load_yaml_config(config_file: str) -> Dict[str, Any]:
        """Load load pattern configuration from YAML."""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _resolve_dataset_path(self, configured_path: str) -> Path:
        """Resolve dataset path from config (absolute or relative to config file)."""
        ds_path = Path(configured_path)
        if ds_path.is_absolute():
            return ds_path
        if self.config_path is None:
            return ds_path.resolve()
        return (self.config_path.parent / ds_path).resolve()

    def _iter_parquet_rows(self, dataset_path: Path) -> Iterable[Dict[str, Any]]:
        """Yield parquet rows as dictionaries using pyarrow batches."""
        if pq is None:
            raise RuntimeError(
                "pyarrow is required to read parquet datasets. Install with: pip install pyarrow"
            )

        parquet_file = pq.ParquetFile(dataset_path)
        for batch in parquet_file.iter_batches(batch_size=4096):
            table = batch.to_pydict()
            keys = list(table.keys())
            rows_count = len(table[keys[0]]) if keys else 0
            for i in range(rows_count):
                row = {key: table[key][i] for key in keys}
                yield row

    def _load_dataset_if_configured(self) -> None:
        """Configure dataset path and initialize lazy parquet iterator."""
        if not self.load_config:
            return

        dataset_cfg = self.load_config.get('dataset', {})
        dataset_path_cfg = dataset_cfg.get('path') or self.load_config.get('dataset_path')
        if not dataset_path_cfg:
            logger.warning("No dataset path configured. Producer will generate synthetic payloads.")
            return

        self.dataset_path = self._resolve_dataset_path(dataset_path_cfg)
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")

        logger.info(f"Using dataset source (lazy): {self.dataset_path}")
        self.dataset_enabled = True
        self.dataset_iter = self._iter_parquet_rows(self.dataset_path)
        self.dataset_row_index = 0

    def _dataset_payload(self) -> Dict[str, Any]:
        """Return deterministic dataset row based on current message index."""
        if not self.dataset_enabled or self.dataset_path is None:
            return {
                'data': f"event_{self.message_count}",
                'value': self.message_count,
            }

        if self.dataset_iter is None:
            self.dataset_iter = self._iter_parquet_rows(self.dataset_path)

        try:
            row = next(self.dataset_iter)
        except StopIteration:
            # Restart iterator when reaching EOF to support long runs.
            self.dataset_iter = self._iter_parquet_rows(self.dataset_path)
            self.dataset_row_index = 0
            row = next(self.dataset_iter)

        idx = self.dataset_row_index
        self.dataset_row_index += 1
        return {
            'dataset_row_index': idx,
            'dataset_total_rows': None,
            'record': row,
        }

    def create_event(self, rate: int, pattern_name: str) -> Dict[str, Any]:
        """Create a test event with metadata and parquet payload."""
        elapsed = time.time() - self.start_time
        return {
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'message_id': self.message_count,
            'load_pattern': pattern_name,
            'target_rate': rate,
            'dataset_path': str(self.dataset_path) if self.dataset_path else None,
            'payload': self._dataset_payload(),
        }
    
    def get_target_rate(self, elapsed: float, pattern: Optional[str] = None, duration: Optional[int] = None) -> int:
        """Calculate target message rate based on pattern or YAML configuration."""
        
        # If YAML config is loaded, use it
        if self.load_config and 'load_pattern' in self.load_config:
            return self._get_rate_from_config(elapsed)
        
        # Otherwise use pattern-based approach
        if pattern == 'increasing':
            # 100 to 1000 msg/s over duration
            progress = min(elapsed / duration, 1.0)
            return int(100 + (900 * progress))
        
        elif pattern == 'decreasing':
            # 1000 to 100 msg/s over duration
            progress = min(elapsed / duration, 1.0)
            return int(1000 - (900 * progress))
        
        elif pattern == 'fluctuating':
            # Oscillate between 200 and 800 msg/s
            cycle = (elapsed % 30.0) / 30.0  # 30s cycle
            return int(200 + (600 * abs(math.sin(cycle * math.pi))))
        
        elif pattern == 'burst':
            # Custom burst pattern: 100s -> 300, 300s -> 700, 900s -> 1000
            if elapsed < 30:
                return 100
            elif elapsed < 60:
                return 300
            elif elapsed < 90:
                return 700
            elif elapsed < 95:
                return 1000  # Burst
            else:
                return 200
        
        elif pattern == 'step':
            # Predefined step changes at specific times
            steps = [
                (0, 100),
                (30, 300),
                (60, 700),
                (90, 1000),
                (95, 200),
            ]
            rate = 100
            for step_time, step_rate in steps:
                if elapsed >= step_time:
                    rate = step_rate
            return rate
        
        else:
            return 100  # Default
    
    def _get_rate_from_config(self, elapsed: float) -> int:
        """Get rate from YAML configuration"""
        time_accumulator = 0
        
        for stage in self.load_config['load_pattern']:
            stage_duration = stage['duration']
            if elapsed < time_accumulator + stage_duration:
                return int(stage['rate'])
            time_accumulator += stage_duration
        
        # Return last rate if elapsed exceeds total duration
        return int(self.load_config['load_pattern'][-1]['rate'])
    
    def run_pattern(self, pattern: Optional[str] = None, duration: Optional[int] = None):
        """Run a load generation pattern."""
        # Use config duration if available, otherwise use parameter
        if self.load_config and 'total_duration' in self.load_config:
            duration = self.load_config['total_duration']
            logger.info(f"Using total_duration from config: {duration}s")
        
        if pattern is None and self.load_config:
            logger.info("Using load pattern from configuration file")
        else:
            logger.info(f"Starting {pattern} pattern for {duration}s")
        
        logger.info("Timeline: 0-30s:100, 30-60s:300, 60-90s:700, 90-95s:1000, 95-120s:200 msg/s")
        if self.dataset_path:
            logger.info(f"Dataset replay source: {self.dataset_path}")
        
        self.start_time = time.time()
        self.message_count = 0
        last_rate = None
        
        while True:
            elapsed = time.time() - self.start_time
            
            if elapsed > duration:
                break
            
            # Get current target rate
            target_rate = self.get_target_rate(elapsed, pattern, duration)
            
            # Log rate changes
            if target_rate != last_rate:
                logger.info(f"Time {elapsed:.1f}s: Rate changed to {target_rate} msg/s")
                last_rate = target_rate
            
            # Calculate inter-message delay for target rate
            interval = 1.0 / target_rate if target_rate > 0 else 1.0
            
            # Send message
            try:
                event = self.create_event(target_rate, pattern or "config")
                future = self.producer.send(self.topic, value=event)
                future.get(timeout=10)
                self.message_count += 1

                if self.message_count == 1:
                    logger.info("First message sent")
                
                # Log every 100 messages
                if self.message_count % 100 == 0:
                    logger.debug(f"Sent {self.message_count} messages")
                
            except Exception as e:
                logger.error(f"Error sending message: {e}")
            
            # Maintain rate
            time.sleep(interval)
        
        self.producer.flush()
        elapsed_total = time.time() - self.start_time
        logger.info(f"Pattern complete. Total messages sent: {self.message_count} in {elapsed_total:.1f}s")
        logger.info(f"Average rate: {self.message_count/elapsed_total:.1f} msg/s")


def main():
    parser = argparse.ArgumentParser(description='Dynamic Load Generator for Kafka')
    parser.add_argument('--broker', default='kafka:29092', help='Kafka broker address')
    parser.add_argument('--topic', default='cic-topic', help='Kafka topic name')
    parser.add_argument('--config', help='Load pattern configuration file (YAML)')
    parser.add_argument('--dataset', help='Override parquet dataset path (absolute or relative to config file)')
    parser.add_argument('--pattern', 
                       choices=['increasing', 'decreasing', 'fluctuating', 'burst', 'step'],
                       default='step', 
                       help='Load pattern (ignored if --config is provided)')
    parser.add_argument('--duration', type=int, default=120, help='Duration in seconds')
    parser.add_argument('--repeat', type=int, default=1, help='Number of times to repeat pattern')
    
    args = parser.parse_args()
    
    generator = DynamicLoadGenerator(broker=args.broker, topic=args.topic, config_file=args.config)

    if args.dataset:
        if not generator.load_config:
            generator.load_config = {}
        if 'dataset' not in generator.load_config:
            generator.load_config['dataset'] = {}
        generator.load_config['dataset']['path'] = args.dataset
        generator._load_dataset_if_configured()
    
    for i in range(args.repeat):
        logger.info(f"Iteration {i+1}/{args.repeat}")
        generator.run_pattern(args.pattern, args.duration)
        if i < args.repeat - 1:
            logger.info("Waiting 10s before next iteration...")
            time.sleep(10)
    
    logger.info("Load generation complete")
    generator.producer.close()


if __name__ == '__main__':
    main()
