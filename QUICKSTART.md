# 🚀 Quick Start Guide

Get your fault recovery experiments running in 5 minutes!

## 📋 Prerequisites (2 minutes)

```bash
# 1. Check Docker is installed
docker --version          # Need v20.10+
docker-compose --version  # Need v1.29+

# 2. Install Python packages
pip3 install kafka-python pyyaml requests

# 3. Navigate to project
cd /Users/rizkyzeinadam/Documents/Percobaan\ Tesis

# 4. Make scripts executable (already done, but verify)
chmod +x scripts/*.sh scripts/*.py
```

## ▶️ Run Experiments (3 minutes)

### Option A: Run Both Systems (Full Experiment - 5 minutes)
```bash
bash scripts/run_experiment.sh both
```

This will:
1. Auto-start Flink + run synchronized experiment (~130s)
2. Stop Flink, start Spark + run identical experiment (~130s)
3. Generate comparative reports

### Option B: Test Flink First
```bash
bash scripts/run_experiment.sh flink
# Runs only Flink experiment (~140s including startup)
```

### Option C: Test Spark First
```bash
bash scripts/run_experiment.sh spark
# Runs only Spark experiment (~140s including startup)
```

## 📊 View Results (1 minute)

After experiments complete, check:

```bash
# View metrics collected
ls -lh metrics/
# Shows: flink_metrics.csv, spark_metrics.csv (120+ rows each)

# View reports
ls -lh results/
# Shows: flink_results.md, spark_results.md, comparison.md

# View comparison (shows which system is better)
cat results/comparison.md
```

## 📈 Real-Time Monitoring (Optional)

While experiments run, in a separate terminal:

```bash
# Watch Flink WebUI
open http://localhost:8083

# Watch Prometheus metrics
open http://localhost:9090

# Watch Grafana dashboards
open http://localhost:3000
# Login: admin / admin
```

## 📝 What Happens

The framework automatically:

1. **Flink Phase**:
   - Start Zookeeper, Kafka, Prometheus, Grafana
   - Start Flink JobManager + 2 TaskManagers
   - Generate messages at varying rates (100-1000 msg/sec)
   - Inject 6 cascading faults (kills, delays, restarts)
   - Collect 120 performance metrics
   - Measure recovery time

2. **Spark Phase** (Identical):
   - Stop Flink completely
   - Start Spark Master + 2 Workers
   - Same load pattern
   - Same fault sequence
   - Same metrics collection

3. **Analysis**:
   - Compare both systems
   - Score each on 4 dimensions
   - Determine winner

## 🔧 Customization (Optional)

### Change Load Pattern
Edit `config/load.yaml`:
```yaml
load_pattern:
  - rate: 500       # Custom rate
    duration: 60    # Custom duration
```

### Change Fault Timing
Edit `config/fault.yaml`:
```yaml
- time: 45          # Different timing
  action: kill_worker
```

Then run again:
```bash
bash scripts/run_experiment.sh both
```

## 🆘 Troubleshooting

### Port Conflict
```bash
# Kill containers and volume data
docker-compose -f docker-compose/docker-compose-flink.yml down -v
docker-compose -f docker-compose/docker-compose-spark.yml down -v

# Try again
bash scripts/run_experiment.sh both
```

### Out of Disk Space
```bash
df -h /
# If < 10GB free, clean Docker
docker system prune -a
```

### Experiment Hangs
```bash
# Check logs in new terminal
tail -f /tmp/producer.log
tail -f /tmp/fault_injection.log
tail -f /tmp/metrics_collector.log

# Manual cleanup
docker ps
docker kill <container_name>
```

## 📊 Output Examples

### flink_results.md
```markdown
# Flink Stream Processing - Fault Recovery Experiment

## Throughput Performance
| Metric | Value |
|--------|-------|
| Min    | 95.42 msg/s |
| Max    | 1002.15 msg/s |
| Mean   | 650.33 msg/s |

## Latency
| Metric | Value |
|--------|-------|
| Peak   | 42.5 ms |
| During Fault | 125.2 ms |
| Recovery Time | 8.3 seconds |
```

### comparison.md (Excerpt)
```markdown
# Flink vs Spark - Comparison Results

## Overall Winner: Flink

### Recovery Time
- Flink: 8.3 seconds  
- Spark: 12.1 seconds
- **Winner: Flink** (3.8s faster)

### Throughput Consistency
- Flink Score: 87/100
- Spark Score: 72/100
- **Winner: Flink** (more stable)

### Final Score
- Flink: 87.5/100
- Spark: 79.8/100
```

## 💡 Tips

- **First Time**: Run `bash scripts/run_experiment.sh flink` to test
- **Full Experiment**: Run `bash scripts/run_experiment.sh both` for comparison
- **Reproducibility**: Don't edit config files unless you want different results
- **Fast Rerun**: Run same command again to compare with previous results
- **Logs**: Check `/tmp/*.log` if anything goes wrong

## 📖 For More Details

- **Full Setup**: See `README.md`
- **Synchronization Details**: See `SYNCHRONIZATION.md`
- **Deployment Checklist**: See `DEPLOYMENT_CHECKLIST.md`
- **Project Overview**: See `PROJECT_SUMMARY.md`

---

**Estimated Times**:
- Full experiment (both systems): ~4.5 minutes
- Flink only: ~2.5 minutes
- Spark only: ~2.5 minutes
- Report generation: ~30 seconds

**Next Steps**:
1. Run: `bash scripts/run_experiment.sh both`
2. Wait ~5 minutes
3. Review: `cat results/comparison.md`
4. Done! 🎉

