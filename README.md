# Fault Recovery Evaluation: Apache Flink vs. Apache Spark Structured Streaming

A comparative experimental framework and data analysis pipeline evaluating fault recovery mechanisms in distributed stream processing engines (**Apache Flink** and **Apache Spark Structured Streaming**) under dynamic workload profiles and cascading failure conditions.

This repository contains the end-to-end pipeline: Docker cluster deployment, synchronized fault injection, 1-second telemetry collection, and post-processing analytics for producing manuscript verification figures (TELKOMNIKA journal standards) and comprehensive benchmark scenario charts.

---

## 📋 Experimental Overview

The benchmark evaluates system resiliency and recovery across three failure scenarios:

1. **Multi-Fault Cascading (900 s):**
   - Dynamic step workload: 100 → 1000 msg/s.
   - Cascading failures: Worker 2 switchover ($t = 300\text{ s}$), primary Worker 1 forced termination ($t = 330\text{ s}$), and Worker 1 recovery restart ($t = 660\text{ s}$).
2. **Single-Node Failure (600 s):**
   - Monotonic workload ramp: 100 → 1000 msg/s.
   - Forced termination of one active worker at $t = 120\text{ s}$ (remains down).
3. **Central Component Failure (600 s):**
   - Monotonic workload ramp: 100 → 1000 msg/s.
   - Coordinator failure (Flink JobManager / Spark Driver) at $t = 120\text{ s}$ and recovery restart at $t = 180\text{ s}$ (60-second unmeasurable outage window).

Experiments are conducted across **3 replications** per scenario and engine ($3 \times 3 \times 2 = 18\text{ independent runs}$). Full environment sanitization (stack reset, Kafka topic recreation, and state/checkpoint cleanup) is enforced between runs to eliminate state contamination.

---

## 🎯 Core Evaluation Metrics

All metrics are canonically defined in [`scripts/analysis_recovery_metrics.py`](scripts/analysis_recovery_metrics.py) as the single source of truth:

| No | Metric | Unit | Mathematical Definition & Description |
|:---:|:---|:---:|:---|
| **1** | **Throughput** | msg/s | Output ingestion rate successfully processed per second. Reported as mean and peak rates. |
| **2** | **Latency** | ms | End-to-end event processing latency (mean of non-zero telemetry samples and peak). Evaluated as internal trend characteristics. |
| **3** | **Backlog Proxy Rate** | msg/s | Backlog accumulation rate computed as $\max(\text{offered load} - \text{throughput}, 0)$ per second (replaces raw Kafka lag to prevent driver/coordinator restart artifacts). |
| **4** | **CPU Utilization** | % | Mean container/node CPU consumption over the benchmark duration. |
| **5** | **Memory Utilization** | MB | Peak container memory consumption per run, reported as sample mean and standard deviation (Table 2 in manuscript). |
| **6** | **Recovery Stabilization Interval** | seconds | Duration $T_2 - T_1$ where:<br>- $T_1$: first sample where throughput drops below $50\%$ of the pre-fault baseline (mean of 60 seconds prior to fault injection).<br>- $T_2$: first sample of 5 consecutive samples sustaining throughput $\ge 90\%$ of the fault-time load, evaluated from the conclusion of the active fault window ($t = 180\text{ s}$ for central failure). |
| **7** | **Throughput Deficit** | % | Proportion of unserved capacity over the entire benchmark:<br>$1 - \frac{\int \text{throughput}\,dt}{\int \text{offered load}\,dt} \times 100\%$<br>Samples during the reporting coordinator outage (central failure) are assigned zero throughput. Collection gaps $> 30\text{ s}$ are capped at $dt = 1\text{ s}$ without extrapolation. |

---

## 📊 Manuscript Verification Benchmark (Table 1 & Table 2)

All statistical figures are computed directly from the 18 raw CSV telemetry files without hardcoding:

| Scenario | Framework | Recovery Interval (s) | Per Run (rep1 / rep2 / rep3) | Throughput Deficit (%) | Peak Backlog (msg/s) | Peak Memory (MB) |
|:---|:---|:---:|:---:|:---:|:---:|:---:|
| **Multi-Fault** | **Flink** | **50.0 ± 4.8** | 52.5 / 52.8 / 44.5 | **9.18 ± 1.45** | 841 | 1391 ± 24 |
| **Multi-Fault** | **Spark** | **35.3 ± 11.4** | 47.5 / 33.5 / 25.0 | **8.11 ± 0.74** | 439 | 3914 ± 139 |
| **Single-Node** | **Flink** | **37.7 ± 2.4** | 40.5 / 36.3 / 36.3 | **10.59 ± 1.28** | 275 | 1233 ± 8 |
| **Single-Node** | **Spark** | **27.5 ± 9.9** | 22.7 / 38.9 / 20.9 | **8.11 ± 1.66** | 297 | 2826 ± 29 |
| **Central** | **Flink** | **68.6 ± 1.2** | 69.5 / 67.2 / 69.0 | **16.19 ± 2.61** | 190 | 1233 ± 1 |
| **Central** | **Spark** | **68.4 ± 1.8** | 67.9 / 66.9 / 70.4 | **19.35 ± 1.43** | 300 | 2567 ± 164 |

*Note: Numerical rounding strictly adheres to ROUND_HALF_UP on the last reported digit. Dispersion is reported as sample standard deviation ($ddof = 1$).*

---

## 📁 Repository Structure

```
Thesis/
├── run_all_analysis.py          # Master CLI: executes complete post-processing pipeline
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── cic-collection.parquet       # Raw network telemetry dataset (CIC-DDoS)
│
├── scripts/                     # Execution, analytics, and figure generation scripts
│   ├── analysis_recovery_metrics.py  # [Canonical] Metric computation & manuscript verification
│   ├── generate_charts_bab4_3x.py   # Scenario benchmark chart generator (timeseries & summaries)
│   ├── make_fig1_scenarios.py       # Architecture diagram generator (300 DPI print)
│   ├── make_figs_print.py           # Publication figures generator (300 DPI print)
│   ├── generate_report.py           # Comparative Markdown report generator
│   ├── generate_publication_figures.py # Publication figure pack and catalog generator
│   ├── run_all_replications.sh      # Master replication runner (18 runs)
│   ├── run_experiment.sh            # Framework experiment orchestrator
│   ├── fault_injection.sh           # Cascading failure controller
│   ├── metrics_collector.py         # 1-second Prometheus telemetry collector
│   └── producer.py                  # Synthetic Kafka workload generator
│
├── results/                     # 18-run experimental CSVs & publication print figures
│   ├── flink_multi_rep[1-3].csv
│   ├── spark_multi_rep[1-3].csv
│   ├── flink_single_rep[1-3].csv
│   ├── spark_single_rep[1-3].csv
│   ├── flink_central_rep[1-3].csv
│   ├── spark_central_rep[1-3].csv
│   └── figs_print/              # 300 DPI publication figures (Figure 1, 2, 3, 4)
│
├── output_charts_bab4/          # 14 benchmark scenario figures
│   ├── fig41_tp_multi.png to fig412_backlog_central.png   (Timeseries overlays)
│   ├── fig413_recovery_summary.png                       (Recovery Interval summary)
│   └── fig414_deficit_summary.png                        (Throughput Deficit summary)
│
├── docker-compose/              # Container topologies for Flink, Spark, Kafka, Prometheus
├── config/                      # Workload ramp profiles, fault schedules, monitoring configs
├── flink-jobs/                  # Java source & compiled JAR for Flink streaming job
├── spark-jobs/                  # Java source & compiled JAR for Spark streaming job
├── metrics/                     # Active run telemetry files
└── archive/                     # Historical prompt notes and scratch files
```

---

## 🚀 Execution Guide

### 1. Post-Processing & Visualizations (From Existing CSV Data)

When the 18 experimental CSV files are present in `results/`, run the unified master runner:

```bash
# Execute verification, publication figures, scenario charts, and comparative reports
python3 run_all_analysis.py
```

#### Selective Options:
* **Verify metrics against manuscript tables only:**
  ```bash
  python3 run_all_analysis.py --check
  ```
  *(Outputs line-by-line checks and confirms `hasil: SEMUA COCOK`).*
* **Generate publication print figures only (300 DPI):**
  ```bash
  python3 run_all_analysis.py --print
  ```
* **Generate scenario benchmark charts only:**
  ```bash
  python3 run_all_analysis.py --bab4
  ```
* **Generate comparative Markdown reports only:**
  ```bash
  python3 run_all_analysis.py --report
  ```

---

### 2. Executing New Docker Experiments (Optional)

#### Prerequisites:
1. Docker Desktop running.
2. Python environment active:
   ```bash
   pip install -r requirements.txt
   ```

#### Launching Replications:
```bash
# Run ALL scenarios and engines (18 runs total):
bash scripts/run_all_replications.sh all all 3

# Or execute specific scenario / engine combinations:
bash scripts/run_all_replications.sh flink multi 3     # 3x Flink multi-fault
bash scripts/run_all_replications.sh spark multi 3     # 3x Spark multi-fault
bash scripts/run_all_replications.sh flink single 3    # 3x Flink single node
bash scripts/run_all_replications.sh spark central 3   # 3x Spark central failure
```

After replication completion, run:
```bash
python3 run_all_analysis.py
```
All charts, tables, and reports will update automatically with the newly collected data.

---

## 🖼️ Generated Visualizations

1. **Publication Print Pack (`results/figs_print/`):**
   - `fig1_scenarios_print.png`: Architecture diagram of failure scenarios (Normal vs Fault states).
   - `fig2_throughput_print.png`: Multi-Fault Throughput comparison (2 panels, Flink vs Spark).
   - `fig3_latency_print.png`: Multi-Fault Latency comparison (2 panels).
   - `fig4_central_print.png`: Central Component Failure throughput comparative timeline.
   *(Sized precisely for standard A4 page text width at 300 DPI: 6.10 inches).*

2. **Benchmark Scenario Charts (`output_charts_bab4/`):**
   - `Figures 4.1 – 4.4`: Multi-Fault Cascading (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Figures 4.5 – 4.8`: Single-Node Failure (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Figures 4.9 – 4.12`: Central Component Failure (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Figure 4.13`: Mean Recovery Stabilization Interval summary across scenarios (Bar Chart Mean ± SD).
   - `Figure 4.14`: Mean Throughput Deficit summary across scenarios (Bar Chart Mean ± SD).
