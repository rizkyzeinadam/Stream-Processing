# Flink Experiment Report

## Core Metrics

- Throughput average: 538.88 msg/s
- Throughput peak: 1899.23 msg/s
- Latency average (non-zero samples): 102.83 ms
- Latency peak (whole run, includes warm-up): 267.91 ms
- CPU average: 16.00%
- Memory average: 1127.68 MB
- Memory peak: 1416.20 MB
- Peak backlog (whole run): 825.38 msg/s
- Peak backlog (fault window): 672.58 msg/s
- Raw kafka_lag column maximum: 271309.00 (collector column, NOT validated as backlog)

## Recovery

- Fault injected at: 300.0 s
- Reporter-down window: tidak terdeteksi
- T1: 341.8 s
- T2: 358.6
- Stabilization interval (T2 - T1): 16.82 s
- Throughput deficit (whole run): 8.14%

## Event Balance (estimate from the throughput column, NOT a loss measurement)

- Input events (integral of load_rate): 308500
- Output events (integral of throughput): 283452
- Difference: 25048 (8.12%)
- Note: the throughput deficit already contains a supply-side component and the
  first-60-s warm-up, so this difference is NOT a data-loss claim.
