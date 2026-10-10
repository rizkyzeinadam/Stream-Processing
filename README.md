# Stream Processing Fault Recovery Evaluation Framework

A comprehensive experimental framework for evaluating fault recovery and cascading failure handling in Apache Flink and Apache Spark under dynamic load conditions.

## 📋 Overview

This framework implements thesis-compliant synchronized experiments that evaluate how stream processing systems handle:

- **Dynamic Load Patterns**: Progressive load changes simulating real-world scenarios
- **Cascading Failures**: Sequential faults triggering system recovery mechanisms
- **Performance Metrics**: Real-time monitoring of throughput, latency, resource usage, and recovery time
- **Reproducibility**: YAML-based configuration ensures identical conditions for both systems

### Key Features

✅ **Synchronized Experiments**: Both Flink and Spark run with identical load patterns and fault timelines
✅ **1-Second Metrics**: Fine-grained performance data collection
✅ **Configuration-Driven**: Fully parameterized via YAML files
✅ **Automated Orchestration**: Single command to run complete experiment suite
✅ **Comprehensive Analysis**: Scoring system and comparison metrics
✅ **Reproducible Results**: All experiment parameters stored and logged

## 📁 Directory Structure

```
Percobaan Tesis/
├── docker-compose/          # Docker Compose configurations
│   ├── docker-compose-flink.yml
│   ├── docker-compose-spark.yml
│   ├── prometheus-flink.yml
│   ├── prometheus-spark.yml
│   └── flink-conf.yaml
├── scripts/                 # Experiment automation scripts
│   ├── run_experiment.sh    # Main orchestration script
│   ├── producer.py          # Dynamic load generator
│   ├── fault_injection.sh   # Fault injection controller
│   ├── metrics_collector.py # Prometheus metrics collector
│   └── generate_report.py   # Report and analysis generator
├── config/                  # Experiment configuration
│   ├── load.yaml           # Dynamic load patterns
│   ├── fault.yaml          # Cascading fault timeline
│   ├── monitoring.yaml     # Metrics collection settings
│   └── flink-conf.yaml
├── metrics/                # Collected metrics (CSV files)
│   ├── flink_metrics.csv
│   └── spark_metrics.csv
└── results/               # Generated reports
    ├── flink_results.md
    ├── spark_results.md
    └── comparison.md
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.7+
- Required Python packages: `kafka-python`, `pyyaml`, `requests`

### Installation

```bash
# Install Python dependencies
pip3 install kafka-python pyyaml requests

# Navigate to project directory
cd /Users/rizkyzeinadam/Documents/Percobaan\ Tesis

# Make scripts executable
chmod +x scripts/*.sh
chmod +x scripts/*.py
```

### Running Experiments

```bash
# Run both experiments (recommended)
bash scripts/run_experiment.sh both

# Or run individual systems
bash scripts/run_experiment.sh flink
bash scripts/run_experiment.sh spark
```

## ⚙️ Configuration Files

### load.yaml - Dynamic Load Pattern

Defines the message rate timeline for the experiment:

```yaml
load_pattern:
  - rate: 100        # 100 msg/sec
    duration: 30     # for 30 seconds
    description: "Baseline load"
  - rate: 300
    duration: 30
    description: "Increasing load"
  - rate: 700
    duration: 30
    description: "High load"
  - rate: 1000
    duration: 5
    description: "Burst load peak"
  - rate: 200
    duration: 25
    description: "Recovery phase"

total_duration: 120  # Total experiment duration
```

### fault.yaml - Cascading Fault Timeline

Defines the sequence of faults injected during the experiment:

```yaml
cascading_faults:
  - time: 60
    action: "kill_worker"          # Kill primary worker
    target: "worker1"
  - time: 65
    action: "burst_load"           # Simultaneous burst load
  - time: 70
    action: "network_delay"        # Add 100ms latency
    target: "worker2"
    delay_ms: 100
  - time: 75
    action: "restart_node"         # Restart killed worker
    target: "worker1"
  - time: 80
    action: "packet_loss"          # Inject packet loss (cascade)
    target: "kafka"
    loss_percentage: 5
  - time: 90
    action: "clear_faults"         # Remove network faults
```

### monitoring.yaml - Metrics Configuration

Specifies which metrics to collect and how frequently:

```yaml
metrics_collection:
  interval: 1        # Collect every 1 second
  prometheus_url: "http://localhost:9090"
  
  metrics:
    - name: "throughput"
      unit: "msg/s"
    - name: "latency"
      unit: "ms"
    - name: "cpu"
      unit: "%"
    - name: "memory"
      unit: "%"
    - name: "kafka_lag"
      unit: "messages"
```

## 📊 Experiment Flow

### Sequential Execution

1. **Setup Phase** (Automatic)
   - Create Docker containers
   - Wait for service startup (30s)
   - Copy configuration files

2. **Flink Experiment** (~130s)
   ```
   ├─ Start Flink JobManager + 2 TaskManagers
   ├─ Start Metrics Collection (every 1s)
   ├─ Start Fault Injection (at predefined times)
   ├─ Run Producer (dynamic load from load.yaml)
   └─ Stop & Save Metrics
   ```

3. **Spark Experiment** (~130s) - IDENTICAL CONDITIONS
   ```
   ├─ Stop Flink Containers
   ├─ Start Spark Master + 2 Workers
   ├─ Start Metrics Collection (same 1s interval)
   ├─ Start Fault Injection (SAME timeline)
   ├─ Run Producer (SAME load pattern)
   └─ Stop & Save Metrics
   ```

4. **Report Generation**
   - Compare metrics
   - Calculate scores
   - Generate Markdown reports

## 📈 Metrics Collected

### Per-Second Metrics

| Metric | Unit | Source | Description |
|--------|------|--------|-------------|
| `throughput` | msg/s | Kafka | Messages processed per second |
| `latency` | ms | Task Manager/Worker | End-to-end processing latency |
| `cpu` | % | Node Exporter | CPU utilization |
| `memory` | % | Node Exporter | Memory utilization |
| `kafka_lag` | messages | Prometheus | Consumer lag (backlog) |
| `recovery_time` | seconds | Derived | Time to recover from fault |

### Output Format (CSV)

```csv
timestamp,elapsed_seconds,load_rate,throughput,latency,cpu,memory,kafka_lag,recovery_time
2024-04-09T10:30:00,0,100,95.2,12.5,15.3,42.1,0,0.0
2024-04-09T10:30:01,1,100,98.7,13.1,16.1,42.5,5,0.0
...
```

## 📄 Report Output

### flink_results.md

Comprehensive Flink experiment analysis including:
- Experiment setup details
- Dynamic load pattern summary
- Cascading fault timeline
- Performance metrics tables
- Recovery time analysis
- System stability assessment
- Peak values and conclusions

### spark_results.md

Identical structure to Flink report for direct comparison.

### comparison.md

Comparative analysis with:
- **Recovery Time Comparison**: Direct measurement & scoring
- **Throughput Consistency**: Stability during faults
- **Latency Spike Analysis**: Peak latency and recovery
- **System Stability Scoring**: Weighted metric evaluation
- **Category Winners**: Which system excels in each area
- **Overall Ranking**: Final scored comparison

#### Scoring Metrics

```
Recovery Time Score:
- Lower recovery = Higher score (0-100)
- Baseline normalized to 30s maximum

Throughput Consistency:
- Lower std deviation = Higher score
- Measures stability during fault window

Latency Spike Score:
- Lower peak latency = Higher score
- Percentage of baseline increase

System Stability:
- Weighted average of three factors
- Throughput consistency: 40%
- Latency stability: 40%
- CPU efficiency: 20%
```

## 🔧 Advanced Usage

### Custom Load Pattern

Edit `config/load.yaml` to define custom rate timeline:

```yaml
load_pattern:
  - rate: 500
    duration: 60
  - rate: 1500
    duration: 30
  - rate: 100
    duration: 30
```

Then run:
```bash
bash scripts/run_experiment.sh both
```

### Custom Fault Injection

Edit `config/fault.yaml` to change fault sequence:

```yaml
cascading_faults:
  - time: 30
    action: "kill_worker"
    target: "worker1"
  - time: 40
    action: "network_delay"
    target: "worker2"
    delay_ms: 500
```

### Manual Metrics Collection

Collect metrics separately without full experiment:

```bash
python3 scripts/metrics_collector.py \
  --prometheus "http://localhost:9090" \
  --output metrics/manual_metrics.csv \
  --duration 60 \
  --interval 1
```

### Manual Report Generation

Generate reports from existing CSV files:

```bash
python3 scripts/generate_report.py \
  --flink-metrics metrics/flink_metrics.csv \
  --spark-metrics metrics/spark_metrics.csv \
  --flink-report results/flink_custom.md \
  --spark-report results/spark_custom.md \
  --comparison-report results/comparison_custom.md
```

## 🔍 Monitoring Dashboards

While experiments run, access:

- **Flink WebUI**: http://localhost:8083
- **Spark WebUI**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## 🐛 Troubleshooting

### Containers won't start
```bash
# Check Docker daemon
docker ps

# Free up ports
docker-compose -f docker-compose-flink.yml down -v
docker-compose -f docker-compose-spark.yml down -v
```

### Metrics not collecting
```bash
# Verify Prometheus is accessible
curl http://localhost:9090/api/v1/status

# Check metrics_collector logs
tail -f /tmp/metrics_collector.log
```

### Fault injection timing issues
```bash
# Check fault injection log
tail -f /tmp/fault_injection.log

# Verify container names
docker ps
```

### Producer not sending messages
```bash
# Check producer log
tail -f /tmp/producer.log

# Verify Kafka is accessible
docker exec flink-kafka kafka-topics --list --bootstrap-server localhost:29092
```

## 📝 Log Files

All major components generate logs:

| Component | Log File |
|-----------|----------|
| Producer | `/tmp/producer.log` |
| Fault Injection | `/tmp/fault_injection.log` |
| Metrics Collection | `/tmp/metrics_collector.log` |

## ✅ Experiment Validation Checklist

Before running experiments:

- [ ] Docker & Docker Compose installed
- [ ] Python 3.7+ with required packages
- [ ] All configuration files present in `config/`
- [ ] Scripts have execute permissions
- [ ] No containers running on ports 2181, 9092, 3000, 8080-8082

## 📚 References

### Configuration Parameters

- **Load Pattern**: Configurable in `config/load.yaml`
- **Fault Timeline**: Configurable in `config/fault.yaml`
- **Metrics Interval**: Set to 1 second (fine-grained data collection)
- **Experiment Duration**: 120s load + 10s margin = 130s total

### Performance Benchmarks

Expected metrics ranges:

| Metric | Typical Range |
|--------|---------------|
| Throughput | 100-1000 msg/s |
| Latency | 10-50 ms |
| CPU | 20-80% |
| Memory | 30-60% |
| Recovery Time | 5-15 seconds |

## 🎯 Key Design Decisions

1. **1-Second Metrics**: Captures detailed behavior during faults
2. **YAML Configuration**: Ensures reproducibility
3. **Sequential Execution**: Prevents resource contention
4. **Identical Conditions**: Fair comparison between systems
5. **Automated Scoring**: Objective performance evaluation
6. **Markdown Reports**: Version-control friendly documentation

## 📞 Support

For issues or questions about the framework:

1. Check log files in `/tmp/`
2. Verify configuration files match expected format
3. Ensure Docker containers are running: `docker ps`
4. Review metrics collected: Check CSV files in `metrics/`

## 📜 License

This experimental framework is for thesis research purposes.

## 🙏 Acknowledgments

Framework designed for thesis evaluation of stream processing fault recovery under cascading failures.

---

**Last Updated**: April 2024
**Framework Version**: 1.0
**Experiment Environment**: Synchronized, YAML-driven, thesis-compliant
# Thesis
