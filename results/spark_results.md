# Spark Experiment Report

## Core Metrics

- Throughput average: 539.82 msg/s
- Throughput peak: 1316.28 msg/s
- Latency average (non-zero samples): 183.18 ms
- Latency peak (whole run, includes warm-up): 444.00 ms
- CPU average: 67.42%
- Memory average: 2791.22 MB
- Memory peak: 3756.03 MB
- Peak backlog (whole run): 404.14 msg/s
- Peak backlog (fault window): 404.14 msg/s
- Raw kafka_lag column maximum: 750.00 (collector column, NOT validated as backlog)

## Recovery

- Fault injected at: 300.0 s
- Reporter-down window: tidak terdeteksi
- T1: 300.0 s
- T2: 327.3
- Stabilization interval (T2 - T1): 27.34 s
- Throughput deficit (whole run): 7.26%

## Event Balance (estimate from the throughput column, NOT a loss measurement)

- Input events (integral of load_rate): 302100
- Output events (integral of throughput): 279625
- Difference: 22475 (7.44%)
- Note: the throughput deficit already contains a supply-side component and the
  first-60-s warm-up, so this difference is NOT a data-loss claim.
