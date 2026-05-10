# Project File Index

Master inventory of all project files and their purposes.

## 📂 Directory Structure

```
/Users/rizkyzeinadam/Documents/Percobaan Tesis/
│
├── 📄 Documentation Files
│   ├── README.md                       - Main comprehensive guide
│   ├── QUICKSTART.md                   - 5-minute quick start
│   ├── SYNCHRONIZATION.md              - Experiment synchronization verification
│   ├── DEPLOYMENT_CHECKLIST.md         - Pre-deployment validation
│   ├── PROJECT_SUMMARY.md              - Completion summary
│   └── FILE_INDEX.md                   - This file
│
├── 📁 docker-compose/ (Docker configurations)
│   ├── docker-compose-flink.yml        - Flink ecosystem (JobManager + 2 TaskManagers)
│   └── docker-compose-spark.yml        - Spark ecosystem (Master + 2 Workers)
│
├── 📁 config/ (Configuration files)
│   ├── load.yaml                       - Dynamic load pattern (5 phases)
│   ├── fault.yaml                      - Cascading fault timeline (6 faults)
│   ├── monitoring.yaml                 - Metrics collection settings (1-second interval)
│   ├── prometheus-flink.yml            - Prometheus config for Flink
│   ├── prometheus-spark.yml            - Prometheus config for Spark
│   └── flink-conf.yaml                 - Flink runtime configuration
│
├── 📁 scripts/ (Automation and analysis)
│   ├── run_experiment.sh               - Main orchestration script
│   ├── producer.py                     - Dynamic load generator (Kafka)
│   ├── fault_injection.sh              - Cascading fault controller
│   ├── metrics_collector.py            - Prometheus metrics collector
│   └── generate_report.py              - Report and analysis generator
│
├── 📁 metrics/ (Results - auto-created)
│   ├── flink_metrics.csv               - Flink metrics (1-second intervals, ~120 rows)
│   └── spark_metrics.csv               - Spark metrics (1-second intervals, ~120 rows)
│
└── 📁 results/ (Reports - auto-created)
    ├── flink_results.md                - Detailed Flink analysis
    ├── spark_results.md                - Detailed Spark analysis
    └── comparison.md                   - Comparative evaluation with scoring
```

## 📋 File Descriptions

### 🎯 Start Here
| File | Size | Purpose | Read Time |
|------|------|---------|-----------|
| QUICKSTART.md | 4KB | Get running in 5 minutes | 5 min |
| README.md | 20KB | Complete documentation | 20 min |

### 📚 Reference Documentation
| File | Size | Purpose | Read Time |
|------|------|---------|-----------|
| SYNCHRONIZATION.md | 15KB | Verify synchronization mechanisms | 15 min |
| DEPLOYMENT_CHECKLIST.md | 18KB | Prerequisites and validation | 10 min |
| PROJECT_SUMMARY.md | 25KB | Complete project overview | 15 min |
| FILE_INDEX.md (this) | 6KB | File inventory and guide | 5 min |

### ⚙️ Docker & Configuration
| File | Lines | Purpose | Key Content |
|------|-------|---------|------------|
| docker-compose-flink.yml | 160 | Flink environment | JobManager, 2 TaskManagers, Kafka, Prometheus, Grafana |
| docker-compose-spark.yml | 150 | Spark environment | Master, 2 Workers, Kafka, Prometheus, Grafana |
| load.yaml | 25 | Load pattern | 5 phases: 100→300→700→1000→200 msg/sec |
| fault.yaml | 40 | Fault timeline | 6 cascading faults: kill→burst→delay→restart→loss→clear |
| monitoring.yaml | 35 | Metrics config | 1-second collection interval, 9 metrics |
| prometheus-flink.yml | 30 | Prometheus (Flink) | Scrape configs for Flink components |
| prometheus-spark.yml | 30 | Prometheus (Spark) | Scrape configs for Spark components |
| flink-conf.yaml | 30 | Flink runtime | State backend, metrics, network tuning |

### 🐍 Python Scripts
| File | Lines | Purpose | Key Functions |
|------|-------|---------|---------------|
| producer.py | 250 | Load generation | YAML config support, 5 load patterns, Kafka integration |
| metrics_collector.py | 200 | Metrics collection | Prometheus queries, 1-sec intervals, CSV export |
| generate_report.py | 400 | Report generation | Statistics, scoring, comparison, Markdown output |

### 🔧 Bash Scripts
| File | Lines | Purpose | Key Functions |
|------|-------|---------|---------------|
| run_experiment.sh | 220 | Main orchestration | Flink→Spark sequential, report generation |
| fault_injection.sh | 180 | Fault control | YAML parsing, Docker manipulation, timing control |

### 📊 Generated Output Files (at runtime)
| File | When Created | Purpose | Format |
|------|--------------|---------|--------|
| flink_metrics.csv | After Flink run | Metrics for Flink | CSV: 9 columns × 120 rows |
| spark_metrics.csv | After Spark run | Metrics for Spark | CSV: 9 columns × 120 rows |
| flink_results.md | After analysis | Flink report | Markdown with tables |
| spark_results.md | After analysis | Spark report | Markdown with tables |
| comparison.md | After analysis | Comparison | Markdown with scoring |

## 🔍 Key File Dependencies

### Execution Order
```
1. config/load.yaml & config/fault.yaml
   ↓ (read by)
2. scripts/run_experiment.sh
   ├─→ docker-compose/docker-compose-flink.yml
   ├─→ scripts/producer.py (--config config/load.yaml)
   ├─→ scripts/fault_injection.sh (config/fault.yaml)
   ├─→ scripts/metrics_collector.py
   ├─→ docker-compose/docker-compose-spark.yml (repeat with Spark)
   └─→ scripts/generate_report.py
       ↓ (produces)
3. metrics/*.csv
   ↓ (processed by)
4. results/*.md
```

### Configuration Propagation
```
config/load.yaml
├─→ scripts/producer.py (load pattern)
├─→ Docker containers (via environment)
└─→ results/*.md (documented)

config/fault.yaml
├─→ scripts/fault_injection.sh (fault timeline)
├─→ Docker containers (via execution)
└─→ results/*.md (documented)

config/monitoring.yaml
├─→ scripts/metrics_collector.py (collection settings)
└─→ metrics/*.csv (1-second intervals)
```

## 🎯 Use Cases

### Run Complete Experiment
```bash
bash scripts/run_experiment.sh both
# Reads: config/*.yaml, docker-compose*.yml
# Produces: metrics/*.csv, results/*.md
```

### Analyze Existing Data
```bash
python3 scripts/generate_report.py \
  --flink-metrics metrics/flink_metrics.csv \
  --spark-metrics metrics/spark_metrics.csv
# Reads: metrics/*.csv
# Produces: results/*.md
```

### Custom Load Pattern
```
1. Edit config/load.yaml
2. Run: bash scripts/run_experiment.sh both
# Uses new load.yaml automatically
```

### Custom Fault Sequence
```
1. Edit config/fault.yaml
2. Run: bash scripts/run_experiment.sh both
# Uses new fault.yaml automatically
```

### Manual Metrics Collection
```bash
python3 scripts/metrics_collector.py \
  --output metrics/custom.csv \
  --duration 60
# Reads: config/monitoring.yaml (implicitly)
# Produces: metrics/custom.csv
```

## 📈 Typical File Flow

### Input Phase (Pre-Experiment)
```
config/load.yaml (5-phase load pattern)
config/fault.yaml (6-fault sequence)
config/monitoring.yaml (collection interval)
docker-compose-flink.yml (container definitions)
docker-compose-spark.yml (container definitions)
↓
Ready for execution
```

### Execution Phase (Experiment Running)
```
scripts/run_experiment.sh (orchestration)
├─ Starts Flink via docker-compose-flink.yml
├─ Runs scripts/producer.py --config config/load.yaml
├─ Runs scripts/fault_injection.sh config/fault.yaml
├─ Runs scripts/metrics_collector.py
├─ Collects metrics → metrics/flink_metrics.csv
├─ Stops Flink
├─ Starts Spark via docker-compose-spark.yml
├─ Repeats for Spark → metrics/spark_metrics.csv
└─ Proceeds to analysis
```

### Analysis Phase (Post-Experiment)
```
scripts/generate_report.py
├─ Reads metrics/flink_metrics.csv
├─ Reads metrics/spark_metrics.csv
├─ Calculates statistics
├─ Generates scores
└─ Produces:
   ├─ results/flink_results.md
   ├─ results/spark_results.md
   └─ results/comparison.md
```

## 🔐 Critical Files for Reproducibility

These files MUST NOT change between experiment runs to ensure reproducibility:

| File | Reason | Change Impact |
|------|--------|---------------|
| config/load.yaml | Load pattern | Different throughput/latency ranges |
| config/fault.yaml | Fault timing | Different recovery times |
| config/monitoring.yaml | Collection interval | Different granularity of data |
| docker-compose-*.yml | Container specs | Different resource allocation |
| scripts/producer.py | Message generation | Different data characteristics |
| scripts/metrics_collector.py | Collection algorithm | Different statistics |

## 📊 Output File Specifications

### CSV Format (metrics/*.csv)
```
Fields: timestamp, elapsed_seconds, load_rate, throughput, latency, 
        cpu, memory, kafka_lag, recovery_time

Sample Row:
2024-04-09T14:31:45.123456,1,100,98.5,14.2,22.3,45.1,12,0.0
```

### Markdown Format (results/*.md)
```
# Experiment Report
## Setup
## Load Pattern
## Faults  
## Metrics Analysis
  ### Throughput
  ### Latency
  ### Resources
## Recovery Analysis
## Conclusions
```

## 🎛️ Parameter Reference

### Load Phases (config/load.yaml)
| Phase | Rate | Duration | Use Case |
|-------|------|----------|----------|
| 1 | 100 msg/sec | 30s | Baseline |
| 2 | 300 msg/sec | 30s | Ramp-up |
| 3 | 700 msg/sec | 30s | High load |
| 4 | 1000 msg/sec | 5s | Burst |
| 5 | 200 msg/sec | 25s | Recovery |

### Fault Events (config/fault.yaml)
| Time | Action | Purpose |
|------|--------|---------|
| 60s | Kill worker | Node failure during high load |
| 65s | Burst load | Stress during recovery |
| 70s | Network delay | Cascading effect |
| 75s | Restart node | Recovery mechanism test |
| 80s | Packet loss | Transport-level disruption |
| 90s | Clear faults | System stabilization |

### Collection Settings (config/monitoring.yaml)
| Setting | Value | Reason |
|---------|-------|--------|
| Interval | 1 second | Capture fault effects |
| Duration | 130 seconds | Covers experiment + margin |
| Metrics | 9 types | Comprehensive view |
| Retention | 24 hours | Sufficient for analysis |

## ✅ Verification Checklist

Use this to verify all files are in place:

```bash
# Check documentation
test -f README.md && echo "✓ README"
test -f QUICKSTART.md && echo "✓ QUICKSTART"
test -f SYNCHRONIZATION.md && echo "✓ SYNCHRONIZATION"
test -f DEPLOYMENT_CHECKLIST.md && echo "✓ CHECKLIST"
test -f PROJECT_SUMMARY.md && echo "✓ SUMMARY"
test -f FILE_INDEX.md && echo "✓ INDEX"

# Check configurations
test -f config/load.yaml && echo "✓ load.yaml"
test -f config/fault.yaml && echo "✓ fault.yaml"
test -f config/monitoring.yaml && echo "✓ monitoring.yaml"

# Check Docker
test -f docker-compose/docker-compose-flink.yml && echo "✓ flink compose"
test -f docker-compose/docker-compose-spark.yml && echo "✓ spark compose"

# Check scripts (must be executable)
test -x scripts/run_experiment.sh && echo "✓ run_experiment.sh"
test -x scripts/producer.py && echo "✓ producer.py"
test -x scripts/fault_injection.sh && echo "✓ fault_injection.sh"
test -x scripts/metrics_collector.py && echo "✓ metrics_collector.py"
test -x scripts/generate_report.py && echo "✓ generate_report.py"
```

---

## 📞 Quick Reference

| Need | File | Command |
|------|------|---------|
| Get started | QUICKSTART.md | Read 5-10 min |
| Run experiment | scripts/run_experiment.sh | `bash scripts/run_experiment.sh both` |
| View results | results/comparison.md | `cat results/comparison.md` |
| Understand sync | SYNCHRONIZATION.md | Read for verification |
| Troubleshoot | DEPLOYMENT_CHECKLIST.md | Follow diagnosis section |
| Full overview | PROJECT_SUMMARY.md | Read for complete picture |

---

**Index Version**: 1.0  
**Last Updated**: April 9, 2024  
**Status**: Complete & Production-Ready
