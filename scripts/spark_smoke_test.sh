#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/Users/rizkyzeinadam/Documents/Percobaan Tesis"
DOCKER_COMPOSE_DIR="${PROJECT_ROOT}/docker-compose"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"
TOPIC="${KAFKA_TOPIC:-cic-topic}"
BROKER="${BROKER:-localhost:9092}"
PATTERN="${PATTERN:-step}"
DURATION="${DURATION:-20}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv-1/bin/python}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

wait_for_http() {
    local name="$1"
    local url="$2"
    local timeout="${3:-180}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if curl -fsS "$url" >/dev/null 2>&1; then
            log "$name is ready"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo "ERROR: $name did not become ready within ${timeout}s" >&2
    return 1
}

wait_for_kafka() {
    local timeout="${1:-180}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if docker exec spark-kafka /usr/bin/kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; then
            log "Kafka is ready"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo "ERROR: Kafka did not become ready within ${timeout}s" >&2
    return 1
}

wait_for_spark_active() {
    local timeout="${1:-180}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if curl -fsS http://localhost:9091/metrics >/dev/null 2>&1; then
            if curl -fsS http://localhost:4040/StreamingQuery/ 2>/dev/null | grep -Eqi 'active|running|status'; then
                log "Spark app is active and metrics endpoint is live"
                return 0
            fi
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo "ERROR: Spark job did not become ACTIVE within ${timeout}s" >&2
    return 1
}

main() {
    log "Starting Spark smoke test"
    cd "$DOCKER_COMPOSE_DIR"
    docker-compose -f docker-compose-spark.yml up -d --remove-orphans

    wait_for_http "Spark Master UI" "http://localhost:8080" 180
    wait_for_http "Prometheus" "http://localhost:9090/-/healthy" 180
    wait_for_http "Grafana" "http://localhost:3002/api/health" 180
    wait_for_kafka 180

    log "Creating Kafka topic if needed"
    docker exec spark-kafka kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic "$TOPIC" --partitions 2 --replication-factor 1 >/dev/null 2>&1 || true

    log "Building Spark jar"
    cd "$PROJECT_ROOT/spark-jobs"
    mvn -q -DskipTests package

    log "Copying jar to spark-driver"
    docker cp "$PROJECT_ROOT/spark-jobs/target/cic-spark-job-1.0.0.jar" spark-driver:/tmp/cic-spark-job.jar
    docker exec --user root spark-driver bash -lc "rm -rf /tmp/spark-query-checkpoints /tmp/spark-local /tmp/.ivy2/cache /tmp/.ivy2/jars 2>/dev/null || true; mkdir -p /tmp/spark-query-checkpoints /tmp/spark-local /tmp/.ivy2/cache /tmp/.ivy2/jars /tmp/spark-query-checkpoints/data; chmod -R 777 /tmp/spark-query-checkpoints /tmp/spark-local /tmp/.ivy2 2>/dev/null || true; chown -R 185:0 /tmp/spark-query-checkpoints /tmp/spark-local /tmp/.ivy2 2>/dev/null || true; rm -f /tmp/cic-spark-job.log /tmp/cic-spark-job.pid"

    log "Submitting Spark job"
    docker exec spark-driver bash -lc "nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.executor.memory=700m --repositories https://repo.maven.apache.org/maven2 --class thesis.stream.CicSparkStreamingJob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /tmp/cic-spark-job.jar spark-kafka:29092 ${TOPIC} spark-group > /tmp/cic-spark-job.log 2>&1 & echo \$! > /tmp/cic-spark-job.pid"

    wait_for_spark_active 180

    log "Starting producer only after Spark is ACTIVE"
    "$PYTHON_BIN" "$SCRIPTS_DIR/producer.py" \
        --broker "$BROKER" \
        --topic "$TOPIC" \
        --pattern "$PATTERN" \
        --duration "$DURATION" \
        > /tmp/spark_smoke_producer.log 2>&1 &
    PRODUCER_PID=$!

    sleep 8

    echo "--- producer log ---"
    tail -40 /tmp/spark_smoke_producer.log 2>/dev/null || true

    echo "--- spark metrics ---"
    curl -fsS http://localhost:9091/metrics | grep -E 'spark_streaming_(throughput|latency|backlog)' || echo 'METRICS_UNCHANGED_OR_EMPTY'

    echo "--- streaming query ---"
    curl -fsS http://localhost:4040/StreamingQuery/ 2>/dev/null | grep -Eio 'active|running|status' | head -5 || echo 'QUERY_STATUS_UNAVAILABLE'

    kill "$PRODUCER_PID" 2>/dev/null || true
    log "Spark smoke test finished"
}

main "$@"
