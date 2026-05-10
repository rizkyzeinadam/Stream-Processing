# Deployment & Verification Checklist

## ✅ Installation Verification

### Directory Structure
```
✓ docker-compose/
  ✓ docker-compose-flink.yml
  ✓ docker-compose-spark.yml
  
✓ scripts/
  ✓ run_experiment.sh (executable)
  ✓ producer.py (executable)
  ✓ fault_injection.sh (executable)
  ✓ metrics_collector.py (executable)
  ✓ generate_report.py (executable)
  
✓ config/
  ✓ load.yaml
  ✓ fault.yaml
  ✓ monitoring.yaml
  ✓ prometheus-flink.yml
  ✓ prometheus-spark.yml
  ✓ flink-conf.yaml
  
✓ metrics/ (auto-created)
✓ results/ (auto-created)
```

### Documentation Files
```
✓ README.md                    - Main documentation
✓ SYNCHRONIZATION.md           - Synchronization verification
✓ DEPLOYMENT_CHECKLIST.md      - This file
```

---

## 🔧 Prerequisites Check

### System Requirements
```bash
# Check Docker
docker --version
# Expected: Docker version 20.10+ 

# Check Docker Compose
docker-compose --version
# Expected: Docker Compose version 1.29+

# Check Python
python3 --version
# Expected: Python 3.7+

# Check pip
pip3 --version
# Expected: pip 20.0+
```

### Python Dependencies
```bash
# Install required packages
pip3 install kafka-python pyyaml requests

# Verify installations
python3 -c "import kafka; import yaml; import requests; print('All dependencies OK')"
```

### Disk Space
```bash
# Check available disk (recommend ≥10GB)
df -h /

# Check Docker storage
du -sh /var/lib/docker
```

### Ports Availability
```bash
# Check ports not in use
sudo lsof -i :2181      # Zookeeper
sudo lsof -i :9092      # Kafka
sudo lsof -i :8080-8082 # Spark/Flink Web UI
sudo lsof -i :9090      # Prometheus
sudo lsof -i :3000      # Grafana
sudo lsof -i :6123      # Flink JobManager
```

---

## 📦 Pre-Experiment Checklist

### 1. Docker Cleanup
```bash
# Clean up any previous containers/volumes
docker-compose -f docker-compose/docker-compose-flink.yml down -v
docker-compose -f docker-compose/docker-compose-spark.yml down -v

# Remove any hanging containers
docker container prune -f

# Verify clean state
docker ps
# Expected: No containers running
```

### 2. Configuration Validation
```bash
# Verify all YAML files exist
ls -l config/*.yaml

# Check YAML syntax
python3 -c "import yaml; yaml.safe_load(open('config/load.yaml')); print('load.yaml: OK')"
python3 -c "import yaml; yaml.safe_load(open('config/fault.yaml')); print('fault.yaml: OK')"
python3 -c "import yaml; yaml.safe_load(open('config/monitoring.yaml')); print('monitoring.yaml: OK')"
```

### 3. Script Permissions
```bash
# Check scripts are executable
ls -l scripts/*.sh scripts/*.py

# Expected output: -rwxr-xr-x (all scripts executable)

# Fix if needed
chmod +x scripts/*.sh scripts/*.py
```

### 4. Log File Cleanup
```bash
# Clear old logs
rm -f /tmp/producer.log
rm -f /tmp/fault_injection.log
rm -f /tmp/metrics_collector.log

# Verify clean
ls -la /tmp/*.log
```

### 5. Metrics Directory Check
```bash
# Clear old metrics
rm -f metrics/*.csv

# Verify empty
ls metrics/
```

---

## 🚀 Deployment Steps

### Step 1: Validate Environment
```bash
cd /Users/rizkyzeinadam/Documents/Percobaan\ Tesis

# Run verification script
bash -c 'echo "=== System Check ===" && \
docker --version && \
docker-compose --version && \
python3 --version && \
echo "=== All checks passed ===" '
```

### Step 2: Pre-warming (Optional)
```bash
# Pull Docker images in advance
docker pull confluentinc/cp-zookeeper:7.5.0
docker pull confluentinc/cp-kafka:7.5.0
docker pull prom/node-exporter:latest
docker pull prom/prometheus:latest
docker pull grafana/grafana:latest
docker pull flink:1.17.1-scala_2.12-java11
docker pull bitnami/spark:3.5.0
```

### Step 3: Run Initial Test (Flink Only)
```bash
# Test with Flink first
bash scripts/run_experiment.sh flink

# Monitor in separate terminal
tail -f /tmp/producer.log
tail -f /tmp/fault_injection.log
tail -f /tmp/metrics_collector.log

# Expected: Completes in ~140 seconds
```

### Step 4: Verify Test Results
```bash
# Check metrics collected
ls -lh metrics/flink_metrics.csv

# Sample data
head -5 metrics/flink_metrics.csv
wc -l metrics/flink_metrics.csv

# Expected: 120+ rows of metrics
```

### Step 5: Full Experiment Run
```bash
# Run both Flink and Spark (recommended)
bash scripts/run_experiment.sh both

# This will take ~280 seconds (140s each + overhead)
# Monitor progress in separate terminal

# Or run sequentially
bash scripts/run_experiment.sh flink
bash scripts/run_experiment.sh spark
bash scripts/run_experiment.sh both
```

### Step 6: Verify Results
```bash
# Check generated metrics
ls -lh metrics/
# Expected: flink_metrics.csv, spark_metrics.csv

# Check generated reports
ls -lh results/
# Expected: flink_results.md, spark_results.md, comparison.md

# View comparison report
cat results/comparison.md
```

---

## 📊 Post-Experiment Verification

### Metrics CSV Structure
```bash
# Verify CSV headers
head -1 metrics/flink_metrics.csv

# Expected columns:
# timestamp,elapsed_seconds,load_rate,throughput,latency,cpu,memory,kafka_lag,recovery_time
```

### Data Quality Checks
```bash
# Count records
wc -l metrics/flink_metrics.csv metrics/spark_metrics.csv

# Expected: ~120-130 rows each (1 per second)

# Check for NaN/missing values
grep -c "nan\|NaN" metrics/flink_metrics.csv || echo "No NaN values - Good!"

# Verify numeric fields
tail -1 metrics/flink_metrics.csv | awk -F, '{print $4, $5, $6, $7}'
```

### Report Generation Verification
```bash
# Check report files exist and have content
wc -l results/*.md

# Expected: 200+ lines in each report

# Verify comparison scores
grep -i "score\|winner" results/comparison.md

# Should show scoring metrics and winners
```

---

## 🔍 Common Issues & Solutions

### Issue: Ports Already in Use
```bash
# Solution: Kill processes on specific port
lsof -ti:9092 | xargs kill -9

# Or use Docker to clean up
docker-compose -f docker-compose/docker-compose-flink.yml down -v
docker-compose -f docker-compose/docker-compose-spark.yml down -v
```

### Issue: Kafka Connection Refused
```bash
# Solution: Verify Kafka is running
docker logs flink-kafka

# Check Kafka health
docker exec flink-kafka kafka-broker-api-versions.sh --bootstrap-server localhost:29092

# Restart if needed
docker-compose -f docker-compose/docker-compose-flink.yml restart flink-kafka
```

### Issue: Metrics Collection Fails
```bash
# Check Prometheus accessibility
curl http://localhost:9090/api/v1/status

# Verify containers running
docker ps | grep prometheus

# Check Prometheus logs
docker logs flink-prometheus
```

### Issue: Fault Injection Not Running
```bash
# Check fault configuration
cat config/fault.yaml | head -20

# Verify bash scripts
bash -n scripts/fault_injection.sh
# Should output no syntax errors

# Test container access
docker exec flink-taskmanager1 ls /
```

### Issue: Producer Slow/Hung
```bash
# Check producer log
tail -50 /tmp/producer.log

# Verify Kafka has space
docker exec flink-kafka df -h

# Check producer CPU/Memory
docker stats --no-stream flink-kafka
```

---

## 📈 Performance Baselines

After successful deployment, expect:

| Metric | Expected Range | Notes |
|--------|---|---|
| Throughput | 95-105% of target | Small variance normal |
| Latency | 10-30 ms | Depends on load phase |
| CPU Usage | 20-70% | Peak during burst |
| Memory | 40-60% | Stable allocation |
| Recovery Time | 5-15 seconds | After fault injection |
| Metrics Points | ~120 | 1 per second for 120s |

---

## 🆘 Emergency Procedures

### Complete Reset
```bash
# Stop all containers
docker-compose -f docker-compose/docker-compose-flink.yml down -v
docker-compose -f docker-compose/docker-compose-spark.yml down -v

# Clean all data
rm -rf metrics/*
rm -rf results/*
rm -f /tmp/*.log

# Restart Docker daemon
sudo systemctl restart docker

# Verify clean
docker ps
```

### Partial Reset (Keep Data)
```bash
# Keep data but reset containers
docker-compose -f docker-compose/docker-compose-flink.yml down
docker-compose -f docker-compose/docker-compose-spark.yml down

# Restart when ready
bash scripts/run_experiment.sh both
```

### Log Diagnostics
```bash
# Collect all logs for diagnosis
mkdir -p diagnostics
cp /tmp/*.log diagnostics/
cp results/* diagnostics/ 2>/dev/null || true
cp metrics/* diagnostics/ 2>/dev/null || true
docker logs flink-kafka > diagnostics/flink-kafka.log 2>&1
docker logs spark-kafka > diagnostics/spark-kafka.log 2>&1

# Archive for analysis
tar czf diagnostics.tar.gz diagnostics/
```

---

## ✨ Success Indicators

### Successful Flink Experiment
```
✓ All 7 Flink containers up
✓ flink_metrics.csv created with ~120 rows
✓ No ERROR messages in /tmp/producer.log
✓ Faults applied at expected times (seen in /tmp/fault_injection.log)
✓ Recovery visible in metrics (throughput recovery after t=60s fault)
```

### Successful Spark Experiment
```
✓ All 4 Spark containers up
✓ spark_metrics.csv created with ~120 rows
✓ Metrics match pattern from Flink (but different absolute values)
✓ Same fault timing applied
✓ Comparison scores generated
```

### Successful Report Generation
```
✓ flink_results.md: 300+ lines with Flink metrics
✓ spark_results.md: 300+ lines with Spark metrics
✓ comparison.md: Shows scoring and winner determination
✓ All tables render correctly in Markdown
```

---

## 📝 Sign-Off Checklist

After successful deployment:

- [ ] Environment requirements met (Docker, Python, Kafka, etc.)
- [ ] All config files validated (YAML syntax correct)
- [ ] Initial Flink experiment completed successfully
- [ ] Initial Spark experiment completed successfully
- [ ] Metrics collected (120+ rows per system)
- [ ] Reports generated with comparison scores
- [ ] No errors in log files
- [ ] Recovery time detected correctly in metrics
- [ ] Fault timeline applied as configured
- [ ] Load pattern followed expected progression
- [ ] System ready for thesis experiments

---

**Checklist Version**: 1.0
**Last Updated**: April 9, 2024
**Status**: Ready for Production Experiments
