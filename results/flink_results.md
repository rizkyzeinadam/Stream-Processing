# Flink Experiment Report

## Core Metrics

- Throughput average: 198.25 msg/s
- Throughput peak: 539.73 msg/s
- Latency average: 92.97 ms
- Latency peak: 170.96 ms
- Max Kafka lag: 4526.00
- CPU average: 9.56%
- Memory average: 70.76%

## Recovery and Correctness

- Threshold-based stabilization interval: 366.52 s
- Data loss: 0.00%
- Duplicate percentage: 98.41%
- Correctness note: No permanent loss detected; surplus likely reflects transient replay behavior.
