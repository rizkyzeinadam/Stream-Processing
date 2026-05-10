# SPARK Stream Processing - Fault Recovery Experiment
## Executive Summary
This report presents a scientific evaluation of SPARK stream processing under dynamic load, burst escalation, and cascading fault conditions. The analysis emphasizes replay semantics, output correctness, data loss, and recovery behavior in a distributed systems context.
## Experiment Setup
**System**: SPARK
**Total Duration**: 900 seconds
**Data Points Collected**: 468
## Dynamic Load and Phase Contextualization
The experiment follows a predefined load pattern from config/load.yaml, which is mapped to baseline, escalation, burst, and recovery phases. This contextualization enables phase-aware interpretation of throughput, latency, and backlog metrics.
| Time Window | Duration | Load Rate | Description |
|------------|------------|------------|------------|
| 0-180s | 180s | 100 msg/s | Baseline load |
| 180-360s | 180s | 300 msg/s | Increasing load |
| 360-540s | 180s | 700 msg/s | High load |
| 540-600s | 60s | 1000 msg/s | Burst load peak |
| 600-900s | 300s | 200 msg/s | Recovery phase |

## Cascading Fault Timeline
The following faults were injected according to config/fault.yaml, marking the transition to the recovery phase:
| Time | Fault Action | Description |
|------------|------------|------------|
| t=300s | switchover | Switchover: TM1 and TM2 failover (TM1 -> TM2) |
| t=330s | kill_worker | Kill TM1 after switchover |
| t=660s | restart_node | Restart TM1 for recovery after long TM2 takeover |

## Performance Metrics Analysis
### Throughput (messages/sec)
| Metric | Value |
|------------|------------|
| Min | 0.00 |
| Max | 493.00 |
| Mean | 89.57 |
| Median | 65.00 |
| Std Dev | 78.77 |

### Latency (milliseconds)
| Metric | Value |
|------------|------------|
| Min | -407.89 |
| Max | 21423.58 |
| Mean | 2508.87 |
| Median | 976.74 |
| Std Dev | 4203.12 |

### Resource Utilization
| Resource | Min (%) | Max (%) | Mean (%) | Std Dev (%) |
|------------|------------|------------|------------|------------|
| CPU | 35.6 | 89.6 | 56.5 | 14.9 |
| Memory | 63.9 | 78.7 | 71.5 | 5.0 |

### Kafka Consumer Lag
| Metric | Value |
|------------|------------|
| Min | 1 |
| Max | 100 |
| Mean | 33 |
| Median | 35 |

## Output Correctness Analysis
This section analyzes output correctness in the presence of replay-induced event amplification, checkpoint replay, and transient failover reprocessing. The observed event reprocessing ratio is interpreted in the context of internal state consistency versus externally observed amplification at the sink.
Replay-induced event amplification is a temporary artifact of checkpoint restoration and failover. Internal state consistency is preserved, and observed amplification is limited to the external sink during recovery. This does not indicate a violation of Exactly-Once semantics.
<div class='caption'>*Observed event reprocessing ratio reflects replay amplification and transient recovery effects. For Flink, this includes temporary checkpoint restoration replay and repetitive IDS traffic, not a systemic correctness failure.*</div>

## Data Loss Analysis
This section evaluates data loss by comparing input and output event counts, reconciling Kafka lag, and distinguishing between delayed processing and true loss. The analysis considers backlog stabilization and recovery limitations, especially under micro-batch processing.
High data loss ratios, particularly in Spark, reflect the inability to reconcile Kafka backlog during cascading failures and burst load, rather than permanent loss of all unprocessed events. This is a limitation of micro-batch recovery under severe fault conditions.
<div class='caption'>*Data loss analysis highlights the impact of backlog reconciliation and recovery limitations. Flink achieves full event reconciliation via checkpoint replay, while Spark’s micro-batch model may leave a significant portion of the backlog unprocessed during cascading failures.*</div>

## Fault Recovery Analysis
**Fault Detection Time**: 300.6s
**Recovery Time**: 87.8s
**Recovery Completion Time**: 388.4s
Recovery analysis integrates throughput stabilization, Kafka lag normalization, and latency recovery to provide a more robust interpretation of system stabilization after fault injection.
## Peak Performance Values
| Metric | Peak Value |
|------------|------------|
| Throughput | 493.00 msg/s |
| Latency | 21423.58 ms |
| CPU Usage | 89.6 % |
| Memory Usage | 78.7 % |
| Kafka Lag | 100 |

## System Stability Assessment
**Throughput Stability (Std Dev)**: 78.77
**Latency Stability (Std Dev)**: 4203.12
**Assessment**: Moderate throughput variability observed
## Conclusions
SPARK demonstrated distributed stream processing characteristics under dynamic load and cascading failures, including:
- Recovery Time: 87.8s from first fault
- Peak Throughput: 493.00 msg/s
- Maximum Latency Spike: 21423.58 ms
- Resource Utilization: CPU 89.6%, Memory 78.7%
The results should be interpreted in the context of replay semantics, backlog reconciliation, and architectural tradeoffs between true-stream and micro-batch processing.
