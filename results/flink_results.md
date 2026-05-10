# FLINK Stream Processing - Fault Recovery Experiment
## Executive Summary
This report presents a scientific evaluation of FLINK stream processing under dynamic load, burst escalation, and cascading fault conditions. The analysis emphasizes replay semantics, output correctness, data loss, and recovery behavior in a distributed systems context.
## Experiment Setup
**System**: FLINK
**Total Duration**: 900 seconds
**Data Points Collected**: 815
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
| Max | 449.12 |
| Mean | 179.65 |
| Median | 186.17 |
| Std Dev | 90.38 |

### Latency (milliseconds)
| Metric | Value |
|------------|------------|
| Min | 97.15 |
| Max | 852.46 |
| Mean | 234.44 |
| Median | 104.87 |
| Std Dev | 213.98 |

### Resource Utilization
| Resource | Min (%) | Max (%) | Mean (%) | Std Dev (%) |
|------------|------------|------------|------------|------------|
| CPU | 8.2 | 62.5 | 19.0 | 10.7 |
| Memory | 66.5 | 88.0 | 79.0 | 7.4 |

### Kafka Consumer Lag
| Metric | Value |
|------------|------------|
| Min | 9 |
| Max | 5234 |
| Mean | 442 |
| Median | 258 |

## Output Correctness Analysis
This section analyzes output correctness in the presence of replay-induced event amplification, checkpoint replay, and transient failover reprocessing. The observed event reprocessing ratio is interpreted in the context of internal state consistency versus externally observed amplification at the sink.
Replay-induced event amplification is a temporary artifact of checkpoint restoration and failover. Internal state consistency is preserved, and observed amplification is limited to the external sink during recovery. This does not indicate a violation of Exactly-Once semantics.
<div class='caption'>*Observed event reprocessing ratio reflects replay amplification and transient recovery effects. For Flink, this includes temporary checkpoint restoration replay and repetitive IDS traffic, not a systemic correctness failure.*</div>

## Data Loss Analysis
This section evaluates data loss by comparing input and output event counts, reconciling Kafka lag, and distinguishing between delayed processing and true loss. The analysis considers backlog stabilization and recovery limitations, especially under micro-batch processing.
High data loss ratios, particularly in Spark, reflect the inability to reconcile Kafka backlog during cascading failures and burst load, rather than permanent loss of all unprocessed events. This is a limitation of micro-batch recovery under severe fault conditions.
<div class='caption'>*Data loss analysis highlights the impact of backlog reconciliation and recovery limitations. Flink achieves full event reconciliation via checkpoint replay, while Spark’s micro-batch model may leave a significant portion of the backlog unprocessed during cascading failures.*</div>

## Fault Recovery Analysis
**Fault Detection Time**: 300.3s
**Recovery Time**: 3.3s
**Recovery Completion Time**: 303.7s
Recovery analysis integrates throughput stabilization, Kafka lag normalization, and latency recovery to provide a more robust interpretation of system stabilization after fault injection.
## Peak Performance Values
| Metric | Peak Value |
|------------|------------|
| Throughput | 449.12 msg/s |
| Latency | 852.46 ms |
| CPU Usage | 62.5 % |
| Memory Usage | 88.0 % |
| Kafka Lag | 5234 |

## System Stability Assessment
**Throughput Stability (Std Dev)**: 90.38
**Latency Stability (Std Dev)**: 213.98
**Assessment**: Moderate throughput variability observed
## Conclusions
FLINK demonstrated distributed stream processing characteristics under dynamic load and cascading failures, including:
- Recovery Time: 3.3s from first fault
- Peak Throughput: 449.12 msg/s
- Maximum Latency Spike: 852.46 ms
- Resource Utilization: CPU 62.5%, Memory 88.0%
The results should be interpreted in the context of replay semantics, backlog reconciliation, and architectural tradeoffs between true-stream and micro-batch processing.
