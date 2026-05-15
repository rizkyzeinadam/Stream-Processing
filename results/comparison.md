# Flink vs Spark Comparison

| Metric | Flink | Spark | Better |
|---|---:|---:|---|
| Throughput (Avg) | 198.25 | 25.46 | Flink |
| Throughput (Peak) | 539.73 | 230.0 | Flink |
| Latency (Avg) | 92.97 | 530.64 | Flink |
| Latency (Peak) | 170.96 | 1691.3 | Flink |
| Kafka Lag (Max) | 4526.0 | 100.0 | Spark |
| CPU Usage (Avg) | 9.56 | 25.55 | Flink |
| Memory Usage (Avg) | 70.76 | 69.54 | Spark |
| Stabilization Interval (s) | 366.52 | 0.0 | Spark |
| Data Loss (%) | 0.0 | 74.46 | Flink |
| Duplicate Events (%) | 98.41 | 0.0 | Spark |

Comparisons are descriptive and depend on configuration, telemetry coverage, and threshold definitions.
