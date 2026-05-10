#!/bin/bash

# Spark Fault Injection Test Script
# Tests queue buildup scenario by stopping Spark job (simulates worker unavailability)
# Monitors via Prometheus and Grafana

set -e

PROJECT_ROOT="/Users/rizkyzeinadam/Documents/Percobaan Tesis"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
warning() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

query_prom() {
    local query="$1"
    curl -sG 'http://localhost:9090/api/v1/query' --data-urlencode "query=$query" 2>/dev/null | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "N/A"
}

log "=== Spark Fault Injection Test Suite ==="
log "Testing queue buildup when Spark job stopped\n"

# Initial state
log "Step 1: Get baseline metrics"
w1_base=$(query_prom "spark_streaming_throughput_worker1")
w2_base=$(query_prom "spark_streaming_throughput_worker2")
p0_base=$(query_prom "kafka_topic_partition_current_offset{partition=\"0\"}")
p1_base=$(query_prom "kafka_topic_partition_current_offset{partition=\"1\"}")

log "  Worker1 throughput: ${w1_base} msg/s"
log "  Worker2 throughput: ${w2_base} msg/s"
log "  Partition 0 offset: ${p0_base}"
log "  Partition 1 offset: ${p1_base}\n"

# Step 2: Stop Spark job
log "Step 2: Stopping Spark job (simulating worker unavailability)"
docker exec spark-driver bash -lc 'if [[ -f /tmp/cic-spark-job.pid ]] && kill -0 $(cat /tmp/cic-spark-job.pid) 2>/dev/null; then kill $(cat /tmp/cic-spark-job.pid); sleep 2; fi'
success "Spark job stopped\n"

# Step 3: Wait and observe metrics changing
log "Step 3: Observing metrics for 30 seconds (queue should pile up)"
for i in {1..6}; do
    sleep 5
    w1=$(query_prom "spark_streaming_throughput_worker1")
    w2=$(query_prom "spark_streaming_throughput_worker2")
    p0=$(query_prom "kafka_topic_partition_current_offset{partition=\"0\"}")
    p1=$(query_prom "kafka_topic_partition_current_offset{partition=\"1\"}")
    
    p0_delta=$((${p0:-0} - ${p0_base:-0}))
    p1_delta=$((${p1:-0} - ${p1_base:-0}))
    
    log "  At +${i}*5s: W1=${w1} W2=${w2} | P0_offset=${p0} (+${p0_delta}) P1_offset=${p1} (+${p1_delta})"
done

log "\nStep 4: Analysis (Queue Buildup)"
p0_final=$(query_prom "kafka_topic_partition_current_offset{partition=\"0\"}")
p1_final=$(query_prom "kafka_topic_partition_current_offset{partition=\"1\"}")

p0_buildup=$((${p0_final:-0} - ${p0_base:-0}))
p1_buildup=$((${p1_final:-0} - ${p1_base:-0}))

warning "Partition 0 (Worker1 queue): +${p0_buildup} messages piled up"
warning "Partition 1 (Worker2 queue): +${p1_buildup} messages piled up"
warning "Total messages in queue: $((p0_buildup + p1_buildup)) messages\n"

# Step 5: Restart Spark job
log "Step 5: Restarting Spark job (simulation of worker recovery)"
docker cp "${PROJECT_ROOT}/spark-jobs/target/cic-spark-job-1.0.0.jar" spark-driver:/tmp/cic-spark-job.jar
docker exec spark-driver bash -lc "mkdir -p /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/.ivy2/cache"
docker exec spark-driver bash -lc "nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --class thesis.stream.CicSparkStreamingJob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /tmp/cic-spark-job.jar kafka:29092 cic-topic spark-group > /tmp/cic-spark-job.log 2>&1 & echo \$! > /tmp/cic-spark-job.pid"

success "Spark job restarted\n"

log "Step 6: Waiting for recovery initialization (30s)"
sleep 5
log "  Waiting for job to become ready and start processing..."
sleep 20

log "Step 7: Recovery complete - final metrics"
w1_recovered=$(query_prom "spark_streaming_throughput_worker1")
w2_recovered=$(query_prom "spark_streaming_throughput_worker2")
p0_recovered=$(query_prom "kafka_topic_partition_current_offset{partition=\"0\"}")
p1_recovered=$(query_prom "kafka_topic_partition_current_offset{partition=\"1\"}")

success "Worker1 throughput recovered: ${w1_recovered} msg/s"
success "Worker2 throughput recovered: ${w2_recovered} msg/s"
success "Partition 0 offset: ${p0_recovered}"
success "Partition 1 offset: ${p1_recovered}\n"

log "=== Test Complete ==="
log "Check Grafana dashboard for visual confirmation of:"
log "  1. Throughput drop to 0 during outage"
log "  2. Queue buildup in 'Kafka Offset per Partition' panel"
log "  3. Positive queue buildup in 'Queue Buildup' panel"
log "  4. Recovery after job restart"
