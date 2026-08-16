#!/bin/bash
# Spawn N Kafka producers simultaneously, partitioning the dataset round-robin
# so the aggregate send rate reaches the target in load.yaml.
#
# Usage:
#   NUM_PRODUCERS=10 bash scripts/run_producers.sh
#
# Env overrides:
#   BROKER         (default localhost:9092 — host-mapped Kafka port)
#   TOPIC          (default cic-topic)
#   NUM_PRODUCERS  (default 10)
#   CONFIG         (default PROJECT_ROOT/config/load.yaml)
#   LOG_DIR        (default /tmp)

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS_DIR="$PROJECT_ROOT/scripts"

BROKER="${BROKER:-127.0.0.1:9092}"
TOPIC="${TOPIC:-cic-topic}"
NUM_PRODUCERS="${NUM_PRODUCERS:-10}"
CONFIG="${CONFIG:-$PROJECT_ROOT/config/load.yaml}"
LOG_DIR="${LOG_DIR:-/tmp}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-spark-kafka}"

# Prefer the project venv python (system python3 may lack `yaml`/`kafka`).
if [ -x "$PROJECT_ROOT/.venv-1/bin/python3" ]; then
  PYTHON="$PROJECT_ROOT/.venv-1/bin/python3"
else
  PYTHON="python3"
fi

echo "[run_producers] Starting $NUM_PRODUCERS producers (broker=$BROKER topic=$TOPIC)"
echo "[run_producers] Config: $CONFIG"

PIDS=()
for i in $(seq 0 $((NUM_PRODUCERS - 1))); do
  # Launch in a tight loop so all producers start within a few milliseconds.
  "$PYTHON" "$SCRIPTS_DIR/producer.py" \
    --broker "$BROKER" \
    --topic "$TOPIC" \
    --config "$CONFIG" \
    --producer-id "$i" \
    --num-producers "$NUM_PRODUCERS" \
    > "$LOG_DIR/producer_$i.log" 2>&1 &
  PIDS+=("$!")
done

echo "[run_producers] Launched PIDs: ${PIDS[*]}"
echo "[run_producers] Tail a single log:  tail -f $LOG_DIR/producer_0.log"
echo "[run_producers] Tail aggregate:      tail -f $LOG_DIR/producer_*.log"
echo ""
echo "[run_producers] VERIFY aggregate rate from the Kafka side (true throughput):"
echo "  docker exec $KAFKA_CONTAINER kafka-run-class kafka.tools.GetOffsetShell \\"
echo "      --broker-list localhost:29092 --topic $TOPIC --time -1"
echo "  # run the above twice ~5s apart; the offset delta / seconds = msg/s"
echo ""

# Keep this script alive so Ctrl-C can stop the whole set cleanly.
wait