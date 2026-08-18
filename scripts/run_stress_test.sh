#!/usr/bin/env bash
# ============================================================
# Stress Test Runner — buktikan zona terukur 50..1500 msg/s
# (menjawab masukan penguji poin 3: "buat stress test")
#
# Cara pakai:
#   bash run_stress_test.sh flink
#   bash run_stress_test.sh spark
#
# Prasyarat: stack framework sudah up (docker compose), Kafka healthy,
# dataset path benar di config/load_stress.yaml.
# ============================================================
set -u

FRAMEWORK="${1:-}"
if [[ "$FRAMEWORK" != "flink" && "$FRAMEWORK" != "spark" ]]; then
    echo "Usage: bash run_stress_test.sh flink|spark"
    exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VENV_PY="$PROJECT_ROOT/.venv/bin/python3"
if [[ ! -x "$VENV_PY" ]]; then
    VENV_PY="$PROJECT_ROOT/.venv/Scripts/python.exe"
fi

CONFIG="$PROJECT_ROOT/config/load_stress.yaml"
OUT_DIR="$PROJECT_ROOT/metrics"
mkdir -p "$OUT_DIR"

STAMP=$(date +%Y%m%d_%H%M%S)
OUT_CSV="$OUT_DIR/stress_${FRAMEWORK}_${STAMP}.csv"
PROD_LOG="/tmp/stress_producer.log"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---------- 1. Bersihkan producer lama ----------
log "Pembersihan producer lama..."
pkill -9 -f "producer.py" 2>/dev/null || true
sleep 1
N=$(pgrep -af "producer.py" | wc -l)
log "Producer aktif: $N (harus 0)"
[[ "$N" == "0" ]] || { echo "Masih ada producer!"; exit 1; }

# ---------- 2. Cek Kafka + Prometheus ----------
log "Cek Prometheus healthy..."
curl -s localhost:9090/-/healthy > /dev/null || { echo "Prometheus belum up"; exit 1; }

# Pre-flight 1 pesan (anti producerId race)
log "Pre-flight test Kafka (1 pesan)..."
PREFLIGHT=$(cat <<'PY'
import sys
try:
    from kafka import KafkaProducer
except ImportError:
    print("NO_KAFKA_MODULE"); sys.exit(2)
p = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda v: v)
f = p.send('cic-topic', b'preflight')
f.get(timeout=15)
p.flush(); p.close()
print("OK")
PY
)
if ! "$VENV_PY" -c "$PREFLIGHT" > /tmp/preflight_stress.log 2>&1; then
    echo "Pre-flight GAGAL — lihat /tmp/preflight_stress.log"
    exit 1
fi
log "Pre-flight OK"

# ---------- 3. Start collector ----------
log "Start stress collector -> $OUT_CSV"
"$VENV_PY" "$PROJECT_ROOT/scripts/stress_collector.py" \
    --framework "$FRAMEWORK" --duration 330 --out "$OUT_CSV" \
    > /tmp/stress_collector.log 2>&1 &
COLLECTOR_PID=$!
sleep 2

# ---------- 4. Start 10 producers (aggregate rate tetap per fase) ----------
log "Start 10 producers (fase 50/100/500/1000/1500 x 60s)..."
rm -f "$PROD_LOG" /tmp/stress_producer_*.log
for i in $(seq 0 9); do
    "$VENV_PY" "$PROJECT_ROOT/scripts/producer.py" \
        --broker 127.0.0.1:9092 \
        --topic cic-topic \
        --config "$CONFIG" \
        --producer-id "$i" \
        --num-producers 10 \
        > "/tmp/stress_producer_${i}.log" 2>&1 &
done
sleep 3
N=$(pgrep -af "producer.py" | wc -l)
log "Producer jalan: $N (harus 10)"
[[ "$N" == "10" ]] || { echo "Producer tidak lengkap!"; exit 1; }

# ---------- 5. Tunggu selesai ----------
log "Menunggu producer selesai (~300s + margin)..."
wait
log "Producer selesai."

# Tunggu collector selesai juga
wait "$COLLECTOR_PID" 2>/dev/null || true

# ---------- 6. Ringkasan ----------
log "=== RINGKASAN ==="
for i in $(seq 0 9); do
    TOTAL=$(grep -h "Total messages sent" "/tmp/stress_producer_${i}.log" 2>/dev/null | tail -1)
    [[ -n "$TOTAL" ]] && log "producer $i: $TOTAL"
done
log "CSV: $OUT_CSV"
log "Collector log: /tmp/stress_collector.log"
log "Selesai. Analisis otomatis: python scripts/analyze_stress.py $OUT_CSV"
