#!/bin/bash

# Fault Injection Script for Cascading Failures
# Uses YAML configuration for reproducibility
# Ensures identical faults for both Flink and Spark

set -e

LOG_FILE="/tmp/fault_injection.log"
SYSTEM="${1:-flink}"  # 'flink' or 'spark'
FAULT_CONFIG="${2:-/Users/rizkyzeinadam/Documents/Percobaan Tesis/config/fault.yaml}"
FLINK_JOB_UI="${FLINK_JOB_UI:-http://localhost:8083}"
FLINK_JOB_CONTAINER="${FLINK_JOB_CONTAINER:-flink-jobmanager}"
FLINK_JOB_JAR="${FLINK_JOB_JAR:-/tmp/cic-flink-job.jar}"
FLINK_KAFKA_BOOTSTRAP="${FLINK_KAFKA_BOOTSTRAP:-kafka:29092}"
FLINK_KAFKA_TOPIC="${FLINK_KAFKA_TOPIC:-cic-topic}"
FLINK_KAFKA_GROUP="${FLINK_KAFKA_GROUP:-flink-group}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

setup_logging() {
    > "$LOG_FILE"
    log "Fault injection started for $SYSTEM using config: $FAULT_CONFIG"
}

# Function to kill a container
kill_container() {
    local container=$1
    log "Killing container: $container"
    docker kill "$container" 2>/dev/null || log "Container $container not found"
    sleep 2
}

# Function to restart a container
restart_container() {
    local container=$1
    log "Restarting container: $container"
    docker restart "$container" >/dev/null 2>&1 || log "Container $container could not be restarted"
    sleep 5
}

restart_flink_job() {
    log "Restarting Flink job to force rebalance after recovery"

    local job_id
    job_id=$(python3 - "$FLINK_JOB_UI" <<'PY'
import json
import sys
import urllib.request

ui = sys.argv[1]
try:
    with urllib.request.urlopen(f"{ui}/jobs/overview", timeout=10) as response:
        payload = json.load(response)
    for job in payload.get('jobs', []):
        state = str(job.get('state', '')).upper()
        if state in {'RUNNING', 'RESTARTING', 'RECONCILING'}:
            print(job.get('jid') or job.get('jobId') or '')
            break
except Exception:
    pass
PY
)

    if [ -n "$job_id" ]; then
        log "Cancelling current Flink job: $job_id"
        docker exec "$FLINK_JOB_CONTAINER" flink cancel "$job_id" >/dev/null 2>&1 || log "Could not cancel Flink job $job_id"
        sleep 5
    else
        log "No running Flink job found to cancel before restart"
    fi

    log "Submitting Flink job again from $FLINK_JOB_JAR"
    docker exec "$FLINK_JOB_CONTAINER" flink run -d "$FLINK_JOB_JAR" "$FLINK_KAFKA_BOOTSTRAP" "$FLINK_KAFKA_TOPIC" "$FLINK_KAFKA_GROUP" >/tmp/flink_job_redeploy.log 2>&1 || {
        log "Could not restart Flink job. Check /tmp/flink_job_redeploy.log"
        return 1
    }

    sleep 5
    log "Flink job restart complete"
}

restart_spark_job() {
    log "Restarting Spark job to force rebalance after recovery"

    if docker exec spark-driver bash -lc "kill \$(cat /tmp/cic-spark-job.pid)" >/dev/null 2>&1; then
        log "Cancelled current Spark job"
    else
        log "No running Spark job found to cancel"
    fi
    sleep 5

    log "Submitting Spark job again from /tmp/cic-spark-job.jar"
    docker exec spark-driver bash -lc "nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --class thesis.stream.CicSparkStreamingJob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /tmp/cic-spark-job.jar kafka:29092 cic-topic spark-group > /tmp/cic-spark-job.log 2>&1 & echo \$! > /tmp/cic-spark-job.pid"

    sleep 5
    log "Spark job restart complete"
}

# Function to add network delay
ensure_tc_available() {
    local container=$1

    if docker exec "$container" sh -c "command -v tc >/dev/null 2>&1"; then
        return 0
    fi

    # Do not attempt package installation during an experiment because it can block
    # for a long time and distort fault timing (switchover/kill/restart sequence).
    log "tc not found in $container; skipping netem-based fault for timing stability"
    return 1
}

add_network_delay() {
    local container=$1
    local delay=$2  # in ms
    log "Adding ${delay}ms network delay to $container"
    if ! ensure_tc_available "$container"; then
        log "Could not add network delay: 'tc' command is not available in $container"
        return 0
    fi
    local err
    err=$(docker exec "$container" sh -c "tc qdisc add dev eth0 root netem delay ${delay}ms" 2>&1) || {
        log "Could not add network delay: ${err}"
        return 0
    }
}

# Function to remove network delay
remove_network_delay() {
    local container=$1
    log "Removing network delay from $container"
    docker exec "$container" sh -c "tc qdisc del dev eth0 root" >/dev/null 2>&1 || log "Could not remove network delay"
}

# Function to inject packet loss
inject_packet_loss() {
    local container=$1
    local loss=$2  # percentage
    log "Injecting ${loss}% packet loss to $container"
    if ! ensure_tc_available "$container"; then
        log "Could not inject packet loss: 'tc' command is not available in $container"
        return 0
    fi
    local err
    err=$(docker exec "$container" sh -c "tc qdisc add dev eth0 root netem loss ${loss}%" 2>&1) || {
        log "Could not inject packet loss: ${err}"
        return 0
    }
}

# Function to get container name based on system and target
get_container_name() {
    local system=$1
    local target=$2
    
    case "$system" in
        flink)
            case "$target" in
                worker1|taskmanager1) echo "flink-taskmanager1" ;;
                worker2|taskmanager2) echo "flink-taskmanager2" ;;
                jobmanager|central) echo "flink-jobmanager" ;;
                kafka) echo "flink-kafka" ;;
                *) echo "flink-jobmanager" ;;
            esac
            ;;
        spark)
            case "$target" in
                worker1) echo "spark-worker1" ;;
                worker2) echo "spark-worker2" ;;
                driver|master|central) echo "spark-driver" ;;
                kafka) echo "spark-kafka" ;;
                *) echo "spark-driver" ;;
            esac
            ;;
    esac
}

# Function to get docker-compose file path
get_compose_file() {
    local system=$1
    if [ "$system" == "flink" ]; then
        echo "docker-compose-flink.yml"
    else
        echo "docker-compose-spark.yml"
    fi
}

# Apply a single fault
apply_fault() {
    local time=$1
    local action=$2
    local target=$3
    local delay=$4
    local loss=$5
    local source=$6
    
    local container=$(get_container_name "$SYSTEM" "$target")
    log "Applying fault at t=$time: action=$action, target=$target ($container)"
    
    case "$action" in
        kill_worker)
            kill_container "$container"
            ;;
        network_delay)
            add_network_delay "$container" "$delay"
            ;;
        restart_node)
            restart_container "$container"
            ;;
        restart_job)
            if [ "$SYSTEM" == "flink" ]; then
                restart_flink_job
            else
                restart_spark_job
            fi
            ;;
        packet_loss)
            inject_packet_loss "$container" "$loss"
            ;;
        burst_load)
            log "Burst load signal at t=$time - producer will increase rate"
            ;;
        switchover)
            log "Switchover: $source -> $target (graceful failover)"
            # Graceful switchover marker: introduce mild degradation on source only.
            # Avoid container restart here so kill/restart events remain clearly visible.
            if [ -n "$source" ]; then
                local source_container=$(get_container_name "$SYSTEM" "$source")
                local target_container=$(get_container_name "$SYSTEM" "$target")
                log "Draining tasks from $source_container to $target_container"
                # Add delay to source to start failover
                add_network_delay "$source_container" "50"
                sleep 3
                log "Switchover complete: $source -> $target"
            fi
            ;;
        clear_faults)
            log "Clearing network faults at t=$time"
            remove_network_delay "$container" 2>/dev/null || true
            docker exec "$container" sh -c "tc qdisc del dev eth0 root 2>/dev/null" 2>/dev/null || log "No faults to clear on $container"
            ;;
    esac
}

# Parse and apply faults from YAML config using simple grep/awk
run_faults_from_yaml() {
    log "Starting cascading failure scenario from YAML config"
    
    if [ ! -f "$FAULT_CONFIG" ]; then
        log "ERROR: Config file not found: $FAULT_CONFIG"
        return 1
    fi
    
    local start_time=$(date +%s)

    # Parse each fault block into one line: time|action|target|delay|loss|source
    awk '
        function flush_fault() {
            if (in_fault && time != "") {
                print time "|" action "|" target "|" delay "|" loss "|" source
            }
        }

        /^[[:space:]]*-[[:space:]]*time:/ {
            flush_fault()
            in_fault = 1
            time = $3
            action = ""
            target = ""
            delay = ""
            loss = ""
            source = ""
            next
        }

        in_fault && /^[[:space:]]*action:/ {
            action = $2
            gsub(/"/, "", action)
            next
        }

        in_fault && /^[[:space:]]*target:/ {
            target = $2
            gsub(/"/, "", target)
            next
        }

        in_fault && /^[[:space:]]*source:/ {
            source = $2
            gsub(/"/, "", source)
            next
        }

        in_fault && /^[[:space:]]*delay_ms:/ {
            delay = $2
            next
        }

        in_fault && /^[[:space:]]*loss_percentage:/ {
            loss = $2
            next
        }

        END {
            flush_fault()
        }
    ' "$FAULT_CONFIG" | while IFS='|' read -r fault_time fault_action fault_target fault_delay fault_loss fault_source; do
        [ -z "$fault_time" ] && continue

        now_elapsed=$(($(date +%s) - start_time))
        wait_time=$((fault_time - now_elapsed))
        if [ $wait_time -gt 0 ]; then
            log "Waiting $wait_time seconds until t=$fault_time"
            sleep $wait_time
        fi

        apply_fault "$fault_time" "$fault_action" "$fault_target" "$fault_delay" "$fault_loss" "$fault_source"
    done
    
    log "All faults applied from configuration"
}

# Verify docker and compose are available
verify_environment() {
    if ! command -v docker &> /dev/null; then
        log "ERROR: Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log "ERROR: Docker Compose is not installed"
        exit 1
    fi
    
    if [ ! -f "$FAULT_CONFIG" ]; then
        log "ERROR: Configuration file not found: $FAULT_CONFIG"
        exit 1
    fi
    
    log "Environment verified"
}

main() {
    setup_logging
    verify_environment
    
    # Use unified YAML-based fault injection for both systems
    run_faults_from_yaml
}

main "$@"
