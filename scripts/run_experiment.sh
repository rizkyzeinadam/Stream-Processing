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
FLINK_MODE="${FLINK_MODE:-B}"
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
    wait_for_http_service "Grafana" "http://localhost:3000/api/health" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_kafka_service "flink-kafka" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_kafka_host_listener "$SERVICE_TIMEOUT_SECONDS"
}

ensure_spark_stack_ready() {
    wait_for_http_service "Spark Master UI" "http://localhost:8080" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Prometheus" "http://localhost:9090/-/healthy" "$SERVICE_TIMEOUT_SECONDS"
    wait_for_http_service "Grafana" "http://localhost:3000/api/health" "$SERVICE_TIMEOUT_SECONDS"
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
    docker exec spark-driver bash -lc "mkdir -p /tmp/spark-checkpoints /tmp/spark-query-checkpoints /tmp/spark-metrics-checkpoints /tmp/spark-local /tmp/.ivy2/cache /tmp/.ivy2/jars"
    docker exec spark-driver bash -lc "nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --class thesis.stream.CicSparkStreamingJob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /tmp/cic-spark-job.jar kafka:29092 ${KAFKA_TOPIC} spark-group > /tmp/cic-spark-job.log 2>&1 & echo \$! > /tmp/cic-spark-job.pid"

    sleep 5
    if ! docker exec spark-driver bash -lc "kill -0 \$(cat /tmp/cic-spark-job.pid)" >/dev/null 2>&1; then
        error "Spark streaming job failed to start. Check spark-driver:/tmp/cic-spark-job.log"
        exit 1
    fi

    log "Waiting for Spark job to initialize and download dependencies (this may take up to 2 minutes)..."
    local elapsed=0
    local timeout=180
    while [ "$elapsed" -lt "$timeout" ]; do
        if docker exec spark-driver curl -s http://localhost:9091/metrics >/dev/null 2>&1; then
            success "Spark job HTTP initialized. Waiting another 15s for Spark DAG and MicroBatch compiler to connect to Kafka..."
            sleep 15
            break
        fi
        if ! docker exec spark-driver bash -lc "kill -0 \$(cat /tmp/cic-spark-job.pid)" >/dev/null 2>&1; then
            error "Spark streaming job JVM crashed. Check spark-driver:/tmp/cic-spark-job.log"
            exit 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    if [ "$elapsed" -ge "$timeout" ]; then
        error "Spark job failed to initialize within ${timeout}s"
        exit 1
    fi

    success "Spark streaming job deployed (cic-topic, group spark-group)"
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
    docker-compose -f docker-compose-flink.yml down -v 2>/dev/null || true
    docker-compose -f docker-compose-spark.yml down -v 2>/dev/null || true
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
    
    # Start producer first so warmup traffic exists before metrics begin
    log "Starting producer with dynamic load pattern (from config)..."
    cd "$SCRIPTS_DIR"
    python3 producer.py \
        --broker "localhost:9092" \
        --topic "$KAFKA_TOPIC" \
        --config "$CONFIG_DIR/load.yaml" \
        > /tmp/flink_producer.log 2>&1 &
    PRODUCER_PID=$!
    log "Producer started"

    wait_for_producer_ready "/tmp/flink_producer.log" "$PRODUCER_PID"

    log "Waiting ${WARMUP_SECONDS}s warmup before metrics collection..."
    sleep "$WARMUP_SECONDS"

    # Start metrics collection once the stream is active
    log "Starting metrics collection (1s interval)..."
    python3 "$SCRIPTS_DIR/metrics_collector.py" \
        --prometheus "http://localhost:9090" \
        --output "$METRICS_DIR/flink_metrics.csv" \
        --duration "$METRICS_DURATION" \
        --interval 1 \
        --load-rate 100 &
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
    docker-compose -f docker-compose-flink.yml down -v
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
    
    # Start producer first so warmup traffic exists before metrics begin
    log "Starting producer with SAME dynamic load pattern..."
    cd "$SCRIPTS_DIR"
    python3 producer.py \
        --broker "localhost:9092" \
        --topic "$KAFKA_TOPIC" \
        --config "$CONFIG_DIR/load.yaml" \
        > /tmp/spark_producer.log 2>&1 &
    PRODUCER_PID=$!
    log "Producer started"

    wait_for_producer_ready "/tmp/spark_producer.log" "$PRODUCER_PID"

    log "Waiting ${WARMUP_SECONDS}s warmup before metrics collection..."
    sleep "$WARMUP_SECONDS"

    # Start metrics collection once the stream is active
    log "Starting metrics collection (1s interval)..."
    python3 "$SCRIPTS_DIR/metrics_collector.py" \
        --prometheus "http://localhost:9090" \
        --output "$METRICS_DIR/spark_metrics.csv" \
        --duration "$METRICS_DURATION" \
        --interval 1 \
        --load-rate 100 &
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
    docker-compose -f docker-compose-flink.yml down -v 2>/dev/null || true
    docker-compose -f docker-compose-spark.yml down -v 2>/dev/null || true
    success "Cleanup complete"
}

# Main execution
main() {
    log "=========================================="
    log "Stream Processing Fault Recovery Evaluation"
    log "Synchronized Experiment Execution"
    log "=========================================="
    
    verify_environment
    setup_directories
    setup_configs
    display_synchronization_summary
    
    # Check if user wants to run specific system
    if [ "$1" == "flink" ]; then
        stop_all
        run_flink_experiment
        cleanup
    elif [ "$1" == "spark" ]; then
        stop_all
        run_spark_experiment
        cleanup
    elif [ "$1" == "both" ] || [ -z "$1" ]; then
        # Run both experiments with synchronized conditions
        stop_all
        run_flink_experiment
        run_spark_experiment
        cleanup
        generate_reports
        generate_publication_figures
        display_results
    else
        error "Unknown option: $1"
        echo "Usage: FLINK_MODE=[A|B] $0 [flink|spark|both]"
        exit 1
    fi
    
    log "=========================================="
    log "Experiment execution complete"
    log "Results available in: $RESULTS_DIR"
    log "=========================================="
}

# Signal handlers
trap 'cleanup; release_lock' EXIT INT TERM

acquire_lock
main "$@"
