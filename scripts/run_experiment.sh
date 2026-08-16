#!/bin/bash

# Main Experiment Orchestration Script
# Executes synchronized fault recovery evaluation for both Flink and Spark
# Uses YAML configuration for reproducible, identical experiments

set -e

PROJECT_ROOT="/Users/rizkyzeinadam/Documents/Percobaan Tesis"
DOCKER_COMPOSE_DIR="${PROJECT_ROOT}/docker-compose"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"
METRICS_DIR="${PROJECT_ROOT}/metrics"
RESULTS_DIR="${PROJECT_ROOT}/results"
CONFIG_DIR="${PROJECT_ROOT}/config"
DATASET_PATH="${PROJECT_ROOT}/cic-collection.parquet"
FLINK_JOB_JAR="${PROJECT_ROOT}/flink-jobs/target/cic-flink-job.jar"
SPARK_JOB_JAR="${PROJECT_ROOT}/spark-jobs/target/cic-spark-job-1.0.0.jar"
WARMUP_SECONDS="${WARMUP_SECONDS:-5}"
EXPERIMENT_DURATION="${EXPERIMENT_DURATION:-900}"
METRICS_DURATION="${METRICS_DURATION:-895}"
SERVICE_TIMEOUT_SECONDS="${SERVICE_TIMEOUT_SECONDS:-150}"
KAFKA_TOPIC="${KAFKA_TOPIC:-cic-topic}"
PRODUCER_READY_TIMEOUT_SECONDS="${PRODUCER_READY_TIMEOUT_SECONDS:-300}"
NUM_PRODUCERS="${NUM_PRODUCERS:-10}"
FLINK_MODE="${FLINK_MODE:-B}"
FLINK_GRAFANA_PORT="${FLINK_GRAFANA_PORT:-3001}"
SPARK_GRAFANA_PORT="${SPARK_GRAFANA_PORT:-3002}"
LOCK_DIR="/tmp/flink_spark_experiment.lock"

acquire_lock() {
    if mkdir "$LOCK_DIR" 2>/dev/null; then
        echo "$$" > "$LOCK_DIR/pid"
        return 0
    fi

    if [ -f "$LOCK_DIR/pid" ]; then
        local existing_pid
        existing_pid=$(cat "$LOCK_DIR/pid" 2>/dev/null || true)
        if [ -n "$existing_pid" ] && ! kill -0 "$existing_pid" 2>/dev/null; then
            warning "Found stale lock for PID $existing_pid, removing..."
            rm -rf "$LOCK_DIR"
            mkdir "$LOCK_DIR"
            echo "$$" > "$LOCK_DIR/pid"
            return 0
        fi
    fi

    error "Another experiment is already running. Lock: $LOCK_DIR"
    if [ -f "$LOCK_DIR/pid" ]; then
        error "Running PID: $(cat "$LOCK_DIR/pid" 2>/dev/null || echo unknown)"
    fi
    exit 1
}

release_lock() {
    rm -rf "$LOCK_DIR" 2>/dev/null || true
}

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

wait_for_http_service() {
    local name="$1"
    local url="$2"
    local timeout="$3"
    local elapsed=0

    log "Waiting for $name at $url (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if curl -fsS "$url" >/dev/null 2>&1; then
            success "$name is ready"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    error "$name did not become ready within ${timeout}s"
    return 1
}

wait_for_spark_streaming_query_active() {
    local timeout="${1:-180}"
    local elapsed=0

    log "Waiting for Spark Structured Streaming query to become ACTIVE (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if curl -fsS "http://localhost:4040/StreamingQuery/" 2>/dev/null | grep -Eqi "active|running|status"; then
            success "Spark Structured Streaming query is ACTIVE"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    error "Spark Structured Streaming query did not become ACTIVE within ${timeout}s"
    return 1
}

wait_for_kafka_service() {
    local container_name="$1"
    local timeout="$2"
    local elapsed=0

    log "Waiting for Kafka in container $container_name (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if docker exec "$container_name" /usr/bin/kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; then
            success "Kafka is ready in $container_name"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    error "Kafka did not become ready within ${timeout}s"
    return 1
}

wait_for_kafka_host_listener() {
    local timeout="$1"
    local elapsed=0

    log "Waiting for Kafka host listener at localhost:9092 (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if docker exec flink-kafka /usr/bin/kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            success "Kafka host listener is ready at localhost:9092"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    error "Kafka host listener did not become ready within ${timeout}s"
    return 1
}

create_kafka_topic() {
    local kafka_container="$1"
    log "Creating Kafka topic ${KAFKA_TOPIC} with 2 partitions on ${kafka_container}..."
    if docker exec "$kafka_container" kafka-topics --bootstrap-server localhost:29092 --delete --topic "$KAFKA_TOPIC" >/dev/null 2>&1; then
        log "Existing topic ${KAFKA_TOPIC} deletion requested"
        sleep 5
    fi

    docker exec "$kafka_container" kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 2 --replication-factor 1 >/tmp/kafka_topic_create.log 2>&1
    docker exec "$kafka_container" kafka-topics --bootstrap-server localhost:29092 --describe --topic "$KAFKA_TOPIC" >/tmp/kafka_topic_describe.log 2>&1 || true

    success "Kafka topic ${KAFKA_TOPIC} ready with 2 partitions"
}

warmup_or_wait() {
    if [ "$WARMUP_SECONDS" -gt 0 ]; then
        log "Applying warmup delay for ${WARMUP_SECONDS}s before starting experiment..."
        sleep "$WARMUP_SECONDS"
    fi
}

ensure_flink_stack_ready() {
    wait_for_http_service "Flink JobManager" "http://localhost:8083/v1/overview" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Prometheus" "http://localhost:9090/-/healthy" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Grafana" "http://localhost:${FLINK_GRAFANA_PORT}/api/health" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_kafka_service "flink-kafka" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_kafka_host_listener "$SERVICE_TIMEOUT_SECONDS"
}

ensure_spark_stack_ready() {
    wait_for_http_service "Spark Master UI" "http://localhost:8080" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Prometheus" "http://localhost:9090/-/healthy" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Grafana" "http://localhost:${SPARK_GRAFANA_PORT}/api/health" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_kafka_service "spark-kafka" "$SERVICE_TIMEOUT_SECONDS"
}

build_flink_job_jar() {
    log "Building Flink streaming job jar..."
    bash "$SCRIPTS_DIR/build_flink_job.sh"
    if [ ! -f "$FLINK_JOB_JAR" ]; then
        error "Flink job jar not found after build: $FLINK_JOB_JAR"
        exit 1
    fi
    success "Flink job jar ready: $FLINK_JOB_JAR"
}

deploy_flink_streaming_job() {
    log "Deploying Flink streaming job from jar..."
    docker cp "$FLINK_JOB_JAR" flink-jobmanager:/tmp/cic-flink-job.jar
    docker exec flink-jobmanager flink run -d /tmp/cic-flink-job.jar kafka:29092 "$KAFKA_TOPIC" flink-group >/tmp/flink_job_submit.log 2>&1
    success "Flink streaming job deployed (cic-topic, group flink-group)"
}

build_spark_job_jar() {
    log "Building Spark streaming job jar..."
    cd "$PROJECT_ROOT/spark-jobs"
    mvn -q -DskipTests package
    if [ ! -f "$SPARK_JOB_JAR" ]; then
        error "Spark job jar not found after build: $SPARK_JOB_JAR"
        exit 1
    fi
    success "Spark job jar ready: $SPARK_JOB_JAR"
}

deploy_spark_streaming_job() {
    log "Deploying Spark streaming job..."

    if [ ! -f "$SPARK_JOB_JAR" ]; then
        error "Spark job jar not found: $SPARK_JOB_JAR"
        exit 1
    fi

    docker cp "$SPARK_JOB_JAR" spark-driver:/tmp/cic-spark-job.jar
    rm -rf "$DOCKER_COMPOSE_DIR/spark-checkpoints" 2>/dev/null || true
    docker exec --user root spark-driver bash -lc "rm -rf /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/spark-local 2>/dev/null || true; mkdir -p /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/spark-local /tmp/.ivy2/cache /tmp/.ivy2/jars /tmp/spark-query-checkpoints/data; chmod -R 777 /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/spark-local /tmp/.ivy2 2>/dev/null || true; chown -R 185:0 /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/spark-local /tmp/.ivy2 2>/dev/null || true"

    # Kill any stale Spark streaming job from a previous run so port 9091 is free.
    log "Clearing stale Spark streaming job processes (not the Master)..."
    docker exec spark-driver bash -lc "pkill -9 -f CicSparkStreamingJob 2>/dev/null || true; pkill -9 -f SparkSubmit 2>/dev/null || true; sleep 3" || true

    local attempt=1
    local max_attempts=3
    while [ "$attempt" -le "$max_attempts" ]; do
        log "Submitting Spark streaming job (attempt ${attempt}/${max_attempts})..."
        docker exec spark-driver bash -lc "rm -f /tmp/cic-spark-job.log /tmp/cic-spark-job.pid && rm -rf /tmp/.ivy2/cache/org.apache.hadoop/hadoop-client-runtime /tmp/.ivy2/jars/hadoop-client-runtime* 2>/dev/null || true && nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.executor.memory=700m --repositories https://repo.maven.apache.org/maven2 --class thesis.stream.CicSparkStreamingJob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /tmp/cic-spark-job.jar spark-kafka:29092 ${KAFKA_TOPIC} spark-group > /tmp/cic-spark-job.log 2>&1 & echo \$! > /tmp/cic-spark-job.pid"

        sleep 10
        if docker exec spark-driver bash -lc "kill -0 \$(cat /tmp/cic-spark-job.pid 2>/dev/null || echo 0)" >/dev/null 2>&1; then
            log "Waiting for Spark job to initialize and download dependencies (this may take up to 2 minutes)..."
            local elapsed=0
            local timeout=180
            while [ "$elapsed" -lt "$timeout" ]; do
                if docker exec spark-driver curl -s http://localhost:9091/metrics >/dev/null 2>&1; then
                    if docker exec spark-driver curl -fsS "http://localhost:4040/StreamingQuery/" 2>/dev/null | grep -Eqi "active|running|status"; then
                        success "Spark job HTTP initialized and Structured Streaming query is ACTIVE. Waiting another 15s for Kafka wiring to settle..."
                        sleep 15
                        success "Spark streaming job deployed (cic-topic, group spark-group)"
                        return 0
                    fi
                fi
                if ! docker exec spark-driver bash -lc "kill -0 \$(cat /tmp/cic-spark-job.pid 2>/dev/null || echo 0)" >/dev/null 2>&1; then
                    break
                fi
                sleep 5
                elapsed=$((elapsed + 5))
            done
        fi

        if docker exec spark-driver bash -lc "grep -q 'download failed: org.apache.hadoop#hadoop-client-runtime' /tmp/cic-spark-job.log 2>/dev/null || grep -q 'hadoop-client-runtime' /tmp/cic-spark-job.log 2>/dev/null"; then
            warning "Dependency download failed during Spark startup; retrying after clearing Ivy cache (attempt ${attempt}/${max_attempts})..."
            attempt=$((attempt + 1))
            continue
        fi

        if docker exec spark-driver bash -lc "grep -q 'Exception in thread \"main\"' /tmp/cic-spark-job.log 2>/dev/null"; then
            error "Spark streaming job JVM crashed. Check spark-driver:/tmp/cic-spark-job.log"
            exit 1
        fi

        if [ "$attempt" -lt "$max_attempts" ]; then
            warning "Spark job did not become ready; retrying submission (attempt ${attempt}/${max_attempts})..."
            attempt=$((attempt + 1))
            continue
        fi

        error "Spark job failed to initialize within ${timeout}s after ${max_attempts} attempts"
        exit 1
    done
}

wait_for_producer_ready() {
    local log_file="$1"
    local producer_pid="$2"
    local timeout="${3:-$PRODUCER_READY_TIMEOUT_SECONDS}"
    local elapsed=0

    log "Waiting for producer readiness marker in $log_file (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if [ -f "$log_file" ] && grep -q "First message sent" "$log_file"; then
            success "Producer is active (first message has been sent)"
            return 0
        fi

        if ! kill -0 "$producer_pid" 2>/dev/null; then
            error "Producer exited before becoming ready. Check log: $log_file"
            return 1
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    error "Producer did not become ready within ${timeout}s. Check log: $log_file"
    return 1
}

start_load_producers() {
    local system="$1"
    local single_log="$2"
    pkill -9 -f "producer.py" 2>/dev/null || true
    sleep 2
    rm -f /tmp/producer_*.log /tmp/producer.log

    if [ "$NUM_PRODUCERS" -gt 1 ]; then
        log "Starting $NUM_PRODUCERS synchronized producers (round-robin partition, aggregate = target rate)..."
        cd "$SCRIPTS_DIR"
        BROKER="127.0.0.1:9092" \
        TOPIC="$KAFKA_TOPIC" \
        NUM_PRODUCERS="$NUM_PRODUCERS" \
        CONFIG="$CONFIG_DIR/load.yaml" \
        LOG_DIR="/tmp" \
        KAFKA_CONTAINER="${system}-kafka" \
        bash "$SCRIPTS_DIR/run_producers.sh" > "/tmp/${system}_producers_launcher.log" 2>&1 &
        PRODUCER_PID=$!
        wait_for_producer_ready "/tmp/producer_0.log" "$PRODUCER_PID"
        sleep 3
        actual=$(pgrep -af producer.py | wc -l | tr -d ' ')
        if [ "$actual" != "$NUM_PRODUCERS" ]; then
            error "Expected $NUM_PRODUCERS producers, found $actual. Aborting."
            exit 1
        fi
        success "Verified $actual producers running"
    else
        log "Starting single producer with dynamic load pattern (from config)..."
        cd "$SCRIPTS_DIR"
        python3 producer.py \
            --broker "127.0.0.1:9092" \
            --topic "$KAFKA_TOPIC" \
            --config "$CONFIG_DIR/load.yaml" \
            > "$single_log" 2>&1 &
        PRODUCER_PID=$!
        wait_for_producer_ready "$single_log" "$PRODUCER_PID"
    fi
}

# Verify environment
verify_environment() {
    log "Verifying environment..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed"
        exit 1
    fi
    
    if ! command -v python3 &> /dev/null; then
        error "Python3 is not installed"
        exit 1
    fi
    
    # Check required directories
    if [ ! -d "$DOCKER_COMPOSE_DIR" ] || [ ! -d "$SCRIPTS_DIR" ] || [ ! -d "$CONFIG_DIR" ]; then
        error "Required directories not found"
        exit 1
    fi
    
    # Check that config files exist
    if [ ! -f "$CONFIG_DIR/load.yaml" ] || [ ! -f "$CONFIG_DIR/fault.yaml" ] || [ ! -f "$CONFIG_DIR/monitoring.yaml" ]; then
        error "Configuration files not found in $CONFIG_DIR"
        exit 1
    fi

    if [ ! -f "$DATASET_PATH" ]; then
        error "Dataset not found: $DATASET_PATH"
        exit 1
    fi
    
    success "Environment verified"
    log "Dataset verified: $DATASET_PATH"
}

# Create necessary directories
setup_directories() {
    log "Setting up directories..."
    mkdir -p "$METRICS_DIR"
    mkdir -p "$RESULTS_DIR"
    success "Directories ready"
}

# Copy configuration files to docker-compose directory
apply_flink_mode() {
    local mode_upper
    mode_upper=$(printf '%s' "$FLINK_MODE" | tr '[:lower:]' '[:upper:]')

    case "$mode_upper" in
        A|STATIC|NO_REBALANCE)
            # Mode A: remove scheduler overrides, keep default non-adaptive scheduling.
            awk '!/^jobmanager\.scheduler:/ && !/^scheduler-mode:/' "$DOCKER_COMPOSE_DIR/flink-conf.yaml" > "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp"
            mv "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp" "$DOCKER_COMPOSE_DIR/flink-conf.yaml"
            FLINK_MODE="A"
            success "Flink Mode A enabled (tanpa rebalance)"
            ;;
        B|ADAPTIVE|REACTIVE)
            # Mode B: force adaptive scheduler so returning TM can rebalance faster.
            awk '!/^jobmanager\.scheduler:/ && !/^scheduler-mode:/' "$DOCKER_COMPOSE_DIR/flink-conf.yaml" > "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp"
            printf 'jobmanager.scheduler: adaptive\n' >> "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp"
            cat "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp" > "$DOCKER_COMPOSE_DIR/flink-conf.yaml"
            rm -f "$DOCKER_COMPOSE_DIR/flink-conf.yaml.tmp"
            FLINK_MODE="B"
            success "Flink Mode B enabled (adaptive rebalance dinamis)"
            ;;
        *)
            error "Invalid FLINK_MODE: $FLINK_MODE"
            echo "Valid options: A, B"
            exit 1
            ;;
    esac
}

setup_configs() {
    log "Setting up configuration files..."
    cp "$CONFIG_DIR/prometheus-flink.yml" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/prometheus-spark.yml" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/metrics.properties" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/flink-conf.yaml" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/load.yaml" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/fault.yaml" "$DOCKER_COMPOSE_DIR/"
    cp "$CONFIG_DIR/monitoring.yaml" "$DOCKER_COMPOSE_DIR/"
    apply_flink_mode
    success "Configuration files copied"
}

# Stop all running containers
stop_all() {
    log "Stopping all containers..."
    cd "$DOCKER_COMPOSE_DIR"
    docker-compose -f docker-compose-flink.yml down --remove-orphans 2>/dev/null || true
    docker-compose -f docker-compose-spark.yml down --remove-orphans 2>/dev/null || true
    rm -rf "$DOCKER_COMPOSE_DIR/spark-checkpoints" 2>/dev/null || true
    sleep 5
    success "All containers stopped"
}

# Run synchronized Flink experiment
run_flink_experiment() {
    log "=========================================="
    log "Starting FLINK Experiment (Synchronized)"
    log "=========================================="
    
    cd "$DOCKER_COMPOSE_DIR"
    
    # Start containers
    log "Starting Flink containers..."
    docker-compose -f docker-compose-flink.yml up -d
    sleep 15
    success "Flink containers started"
    
    # Wait until all required services are ready
    ensure_flink_stack_ready
    create_kafka_topic "flink-kafka"
    build_flink_job_jar
    deploy_flink_streaming_job
    
    log "Experiment Configuration:"
    log "  Load Config: $CONFIG_DIR/load.yaml"
    log "  Fault Config: $CONFIG_DIR/fault.yaml"
    log "  Metrics Interval: 1 second"
    log "  Expected Duration: ~${EXPERIMENT_DURATION} seconds"
    log "  Dataset: $DATASET_PATH"
    
    # Start producer(s) first so warmup traffic exists before metrics begin.
    start_load_producers "flink" "/tmp/flink_producer.log"

    log "Waiting ${WARMUP_SECONDS}s warmup before metrics collection..."
    sleep "$WARMUP_SECONDS"

    # Start metrics collection once the stream is active
    log "Starting metrics collection (1s interval)..."
    python3 "$SCRIPTS_DIR/metrics_collector.py" \
        --prometheus "http://localhost:9090" \
        --output "$METRICS_DIR/flink_metrics.csv" \
        --duration "$METRICS_DURATION" \
        --interval 1 \
        --load-config "$CONFIG_DIR/load.yaml" &
    METRICS_PID=$!
    log "Metrics started"

    # Start fault injection after metrics are running
    log "Starting fault injection sequence (from config)..."
    bash "$SCRIPTS_DIR/fault_injection.sh" flink "$CONFIG_DIR/fault.yaml" &
    FAULT_PID=$!
    log "Fault injection started"

    # Wait for producer to finish, then collect remaining background work
    wait $PRODUCER_PID 2>/dev/null || true
    
    # Wait for background processes
    log "Waiting for fault injection and metrics collection to complete..."
    wait $FAULT_PID 2>/dev/null || true
    wait $METRICS_PID 2>/dev/null || true
    
    sleep 10
    
    success "Flink experiment complete"
}

# Run synchronized Spark experiment
run_spark_experiment() {
    log "=========================================="
    log "Starting SPARK Experiment (Synchronized)"
    log "=========================================="
    
    # Stop Flink
    log "Stopping Flink containers..."
    cd "$DOCKER_COMPOSE_DIR"
    docker-compose -f docker-compose-flink.yml down --remove-orphans
    sleep 15
    
    # Start Spark
    log "Starting Spark containers..."
    docker-compose -f docker-compose-spark.yml up -d
    sleep 15
    success "Spark containers started"
    
    # Wait until all required services are ready
    ensure_spark_stack_ready
    create_kafka_topic "spark-kafka"
    build_spark_job_jar
    deploy_spark_streaming_job
    
    log "Experiment Configuration (IDENTICAL to Flink):"
    log "  Load Config: $CONFIG_DIR/load.yaml"
    log "  Fault Config: $CONFIG_DIR/fault.yaml"
    log "  Metrics Interval: 1 second"
    log "  Expected Duration: ~${EXPERIMENT_DURATION} seconds"
    log "  Dataset: $DATASET_PATH"
    
    # Start producer(s) first so warmup traffic exists before metrics begin.
    start_load_producers "spark" "/tmp/spark_producer.log"

    log "Waiting ${WARMUP_SECONDS}s warmup before metrics collection..."
    sleep "$WARMUP_SECONDS"

    # Start metrics collection once the stream is active
    log "Starting metrics collection (1s interval)..."
    python3 "$SCRIPTS_DIR/metrics_collector.py" \
        --prometheus "http://localhost:9090" \
        --output "$METRICS_DIR/spark_metrics.csv" \
        --duration "$METRICS_DURATION" \
        --interval 1 \
        --load-config "$CONFIG_DIR/load.yaml" &
    METRICS_PID=$!
    log "Metrics started"

    # Start fault injection after metrics are running
    log "Starting fault injection sequence (SAME config as Flink)..."
    bash "$SCRIPTS_DIR/fault_injection.sh" spark "$CONFIG_DIR/fault.yaml" &
    FAULT_PID=$!
    log "Fault injection started"

    # Wait for producer to finish, then collect remaining background work
    wait $PRODUCER_PID 2>/dev/null || true
    
    # Wait for background processes
    log "Waiting for fault injection and metrics collection to complete..."
    wait $FAULT_PID 2>/dev/null || true
    wait $METRICS_PID 2>/dev/null || true
    
    sleep 10
    success "Spark experiment complete"
}

run_fault_scenario() {
    local system="$1"
    local fault_config="$2"
    local output_csv="$3"

    log "=========================================="
    log "Starting ${system^^} Fault Scenario"
    log "=========================================="

    stop_all
    cd "$DOCKER_COMPOSE_DIR"
    if [ "$system" = "flink" ]; then
        docker-compose -f docker-compose-flink.yml up -d
        sleep 15
        ensure_flink_stack_ready
        create_kafka_topic "flink-kafka"
        build_flink_job_jar
        deploy_flink_streaming_job
    else
        docker-compose -f docker-compose-spark.yml up -d
        sleep 15
        ensure_spark_stack_ready
        create_kafka_topic "spark-kafka"
        build_spark_job_jar
        deploy_spark_streaming_job
    fi

    log "Scenario output CSV: $output_csv"
    log "Scenario fault config: $fault_config"

    start_load_producers "$system" "/tmp/${system}_fault_producer.log"
    sleep "$WARMUP_SECONDS"

    python3 "$SCRIPTS_DIR/metrics_collector.py" \
        --prometheus "http://localhost:9090" \
        --output "$output_csv" \
        --duration "$METRICS_DURATION" \
        --interval 1 \
        --load-config "$CONFIG_DIR/load.yaml" &
    METRICS_PID=$!

    bash "$SCRIPTS_DIR/fault_injection.sh" "$system" "$fault_config" &
    FAULT_PID=$!

    wait $PRODUCER_PID 2>/dev/null || true
    wait $FAULT_PID 2>/dev/null || true
    wait $METRICS_PID 2>/dev/null || true

    cleanup
    success "${system^^} fault scenario complete"
}

run_single_node_failure() {
    local system="$1"
    run_fault_scenario "$system" "$CONFIG_DIR/fault_single_node.yaml" "$METRICS_DIR/${system}_single_node_failure.csv"
}

run_central_component_failure() {
    local system="$1"
    run_fault_scenario "$system" "$CONFIG_DIR/fault_central_component.yaml" "$METRICS_DIR/${system}_central_component_failure.csv"
}

# Generate reports
generate_reports() {
    log "Generating comparative reports..."
    
    cd "$PROJECT_ROOT"
    python3 "$SCRIPTS_DIR/generate_report.py" \
        --flink-metrics "$METRICS_DIR/flink_metrics.csv" \
        --spark-metrics "$METRICS_DIR/spark_metrics.csv" \
        --flink-report "$RESULTS_DIR/flink_results.md" \
        --spark-report "$RESULTS_DIR/spark_results.md" \
        --comparison-report "$RESULTS_DIR/comparison.md"
    
    success "Reports generated with scoring and analysis"
}

# Generate publication-grade scientific figures
generate_publication_figures() {
    log "Generating publication-grade scientific figures..."

    cd "$PROJECT_ROOT"
    python3 "$SCRIPTS_DIR/generate_publication_figures.py" \
        --project-root "$PROJECT_ROOT" \
        --results-dir "$RESULTS_DIR" \
        --config-dir "$CONFIG_DIR" \
        --metrics-dir "$METRICS_DIR"

    success "Publication-grade figures generated"
}

# Display results
display_results() {
    log "Experiment results saved to:"
    ls -lh "$RESULTS_DIR"
    log "Metrics data saved to:"
    ls -lh "$METRICS_DIR"
}

# Display synchronization summary
display_synchronization_summary() {
    log "=========================================="
    log "Experiment Synchronization Summary"
    log "=========================================="
    log "Both experiments used identical conditions:"
    log ""
    log "Load Pattern (from config/load.yaml):"
    grep -A 20 "load_pattern:" "$CONFIG_DIR/load.yaml" | head -10 || true
    log ""
    log "Fault Sequence (from config/fault.yaml):"
    grep -A 3 "cascading_faults:" "$CONFIG_DIR/fault.yaml" | head -8 || true
    log ""
    log "Metrics Collection: Every 1 second after ${WARMUP_SECONDS}s warmup"
    log "Experiment Duration: ${EXPERIMENT_DURATION} seconds load"
    log "Flink Mode: ${FLINK_MODE} (A=tanpa rebalance, B=adaptive)"
    log "=========================================="
}

# Cleanup
cleanup() {
    if [ "${SKIP_CLEANUP:-0}" = "1" ]; then
        warning "SKIP_CLEANUP=1, leaving containers running for live inspection"
        return 0
    fi

    log "Cleaning up..."
    cd "$DOCKER_COMPOSE_DIR"
    docker-compose -f docker-compose-flink.yml down --remove-orphans 2>/dev/null || true
    docker-compose -f docker-compose-spark.yml down --remove-orphans 2>/dev/null || true
    success "Cleanup complete"
}

# Main execution
usage() {
    echo "Usage: $0 [SCENARIO]"
    echo ""
    echo "Scenarios:"
    echo "  flink              - Flink dynamic load + multi-fault cascading"
    echo "  spark              - Spark dynamic load + multi-fault cascading"
    echo "  single-node-flink  - Flink single TaskManager failure (no restart)"
    echo "  single-node-spark  - Spark single Worker failure (no restart)"
    echo "  central-flink      - Flink JobManager failure + recovery"
    echo "  central-spark      - Spark Master/Driver failure + recovery"
    echo "  all-scenarios      - Run all scenarios sequentially"
    echo ""
    echo "Example: $0 central-flink"
}

main() {
    log "=========================================="
    log "Stream Processing Fault Recovery Evaluation"
    log "Synchronized Experiment Execution"
    log "=========================================="

    if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
        usage
        exit 0
    fi
    
    verify_environment
    setup_directories
    setup_configs
    display_synchronization_summary
    
    case "${1:-both}" in
        flink)
        stop_all
        run_flink_experiment
        cleanup
            ;;
        spark)
        stop_all
        run_spark_experiment
        cleanup
            ;;
        single-node-flink)
        run_single_node_failure "flink"
            ;;
        single-node-spark)
        run_single_node_failure "spark"
            ;;
        central-flink)
        run_central_component_failure "flink"
            ;;
        central-spark)
        run_central_component_failure "spark"
            ;;
        all-scenarios)
        stop_all
        run_flink_experiment
        run_spark_experiment
        run_single_node_failure "flink"
        run_single_node_failure "spark"
        run_central_component_failure "flink"
        run_central_component_failure "spark"
        cleanup
        generate_reports
        generate_publication_figures
        display_results
            ;;
        both)
        # Run both experiments with synchronized conditions
        stop_all
        run_flink_experiment
        run_spark_experiment
        cleanup
        generate_reports
        generate_publication_figures
        display_results
            ;;
        *)
        error "Unknown option: ${1:-}"
        usage
        exit 1
            ;;
    esac
    
    log "=========================================="
    log "Experiment execution complete"
    log "Results available in: $RESULTS_DIR"
    log "=========================================="
}

# Signal handlers
trap 'cleanup; release_lock' EXIT INT TERM

acquire_lock
main "$@"
