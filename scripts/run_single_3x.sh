#!/usr/bin/env bash
# run_single_3x.sh — 3× replikasi skenario SINGLE NODE (Flink + Spark), selang-seling
set -u
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"
mkdir -p results

PY="python3"
if [[ -x ".venv/Scripts/python.exe" ]]; then PY=".venv/Scripts/python.exe"; fi
if [[ -x ".venv/bin/python3" ]]; then PY=".venv/bin/python3"; fi

KAFKA_CONTAINER="$(docker ps --format '{{.Names}}' | grep -iE 'kafka' | grep -viE 'exporter|zookeeper' | head -1)"
TOPIC="cic-topic"

truncate_topic() {
    if [[ -z "$KAFKA_CONTAINER" ]]; then
        echo ">> WARNING: container Kafka tidak ketemu — SKIP truncate"
        return
    fi
    echo ">> Truncate topic '$TOPIC' di $KAFKA_CONTAINER"
    docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic "$TOPIC" >/dev/null 2>&1
    # loop sampai BENAR-BENAR hilang (delete async)
    for i in $(seq 1 30); do
        N=$(docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^${TOPIC}$" || true)
        [[ "$N" == "0" ]] && break
        sleep 2
    done
    docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 \
        --create --topic "$TOPIC" --partitions 2 --replication-factor 1 >/dev/null 2>&1
    for i in $(seq 1 30); do
        N=$(docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^${TOPIC}$" || true)
        [[ "$N" == "1" ]] && break
        sleep 2
    done
    echo ">> Topic siap."
}

run_one() {  # $1=flink|spark, $2=rep
    local fw="$1" rep="$2"
    local arg="single-node-${fw}"
    local src="metrics/${fw}_single_node_failure.csv"
    local dst="results/${fw}_single_rep${rep}.csv"
    echo ""
    echo "=============================================================="
    echo "  ${fw^^} — SINGLE — REPLIKASI $rep  ($(date '+%H:%M:%S'))"
    echo "=============================================================="
    pkill -9 -f "producer.py" 2>/dev/null || true
    sleep 2
    N=$(pgrep -af "producer.py" | wc -l)
    if [[ "$N" != "0" ]]; then echo "ERROR: masih ada $N producer"; exit 1; fi
    truncate_topic
    SKIP_CLEANUP=1 NUM_PRODUCERS=10 bash scripts/run_experiment.sh "$arg"
    RC=$?
    if [[ $RC -ne 0 ]]; then echo "ERROR: run_experiment.sh $arg gagal (rc=$RC)"; exit 1; fi
    [[ -f "$src" ]] && cp "$src" "$dst" && echo ">> CSV: $dst"
    if [[ -f "$dst" ]]; then
        "$PY" scripts/recovery_metrics_proposal.py --csv "$dst" --fault-time 120 2>/dev/null \
            | tee "results/${fw}_single_rep${rep}_recovery.txt"
    fi
    sleep 5
}

echo "=== Flink single 3× + Spark single 3× (selang-seling) ==="
echo "Project root: $PROJECT_ROOT | Kafka: $KAFKA_CONTAINER | Python: $PY"
for rep in 1 2 3; do
    if [[ -f "results/flink_single_rep${rep}.csv" && -f "results/spark_single_rep${rep}.csv" ]]; then
        echo ">> SKIP rep$rep (keduanya sudah ada)"; continue
    fi
    [[ ! -f "results/flink_single_rep${rep}.csv" ]] && run_one flink "$rep"
    [[ ! -f "results/spark_single_rep${rep}.csv" ]] && run_one spark "$rep"
done
echo "SELESAI — hasil di results/{flink,spark}_single_rep{1,2,3}.csv"
