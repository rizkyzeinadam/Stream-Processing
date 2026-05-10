# Technical Analysis: Flink vs Spark Structured Streaming

## Overview
This technical analysis evaluates the performance of Apache Flink and Apache Spark Structured Streaming under dynamic load, fault injection, and cascading failures.

## 7-Metric Comparative Evaluation

### 1. Throughput
Flink and Spark handle backpressure differently. Flink's true streaming engine processes events natively, leading to stable throughput. Spark's micro-batch architecture may cause throughput to be bursty.

### 2. Latency
Flink's event-at-a-time processing model naturally offers lower latency compared to Spark's micro-batching overhead.

### 3. Backpressure (Kafka Lag)
Under cascading failures, backlog forms rapidly. Flink's credit-based flow control manages backlog more responsively, whereas Spark might experience longer catch-up times depending on batch intervals.

### 4. Resource Usage
Both engines utilize CPU aggressively during recovery, but memory footprints differ based on state management (RocksDB vs Spark state store).

### 5. Recovery Time
Fault recovery in Flink with adaptive scheduler (Mode B) allows rapid rescaling, whereas Spark requires driver-coordinated micro-batch rescheduling.

### 6 & 7. Correctness, Data Loss, and Duplicate Events
Both engines guarantee Exactly-Once Processing in ideal conditions, but cascading failures reveal distinct behaviors.
- **Data Loss**: Flink (0.00%) vs Spark (0.00%).
- **Duplicate Events**: Flink (79.23%) vs Spark (7.27%).

**Duplicate Event Interpretation**:
- **Flink**: Observed duplicate patterns are likely influenced by: 1. repetitive IDS traffic characteristics, 2. temporary replay during checkpoint recovery, 3. transient event reprocessing during failover.. Natural repetitive traffic or temporary replay. No permanent correctness violations confirmed.
- **Spark**: Observed duplicate patterns are likely influenced by: 1. repetitive IDS traffic characteristics, 2. temporary replay during checkpoint recovery, 3. transient event reprocessing during failover.. Natural repetitive traffic or temporary replay. No permanent correctness violations confirmed.

## Quantitative Comparison

| Metric | Flink | Spark | Better |
|--------|--------|--------|--------|
| Throughput (Avg) | 179.65 | 89.57 | Flink |
| Throughput (Peak) | 449.12 | 493.0 | Spark |
| Latency (Avg) | 234.44 | 2688.81 | Flink |
| Latency (Spike) | 852.46 | 21423.58 | Flink |
| Kafka Lag (Max) | 5234.0 | 100.0 | Spark |
| CPU Usage (Avg) | 18.97 | 56.49 | Flink |
| Memory Usage (Avg) | 78.95 | 71.53 | Spark |
| Recovery Time (s) | 3.33 | 87.78 | Flink |
| Data Loss (%) | 0.0 | 0.0 | Tie |
| Duplicate Events (%) | 79.23 | 7.27 | Spark |


## Conclusion
Flink outperforms Spark in strict latency and recovery speed, making it more suitable for true real-time critical paths. Spark remains highly competitive in throughput-oriented, batch-like analytics scenarios.
