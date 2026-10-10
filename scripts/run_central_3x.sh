#!/usr/bin/env bash
# ============================================================
# run_central_3x.sh — 3× replikasi skenario CENTRAL COMPONENT (Flink + Spark)
#
# Skenario central: kill komponen pusat (Flink JobManager / Spark Master)
#   - fault t=120 (kill jobmanager/master)
#   - restart t=180 (komponen pusat di-restart otomatis/oleh fault script)
#   - load ramp 100→1000 (config/load_ramp.yaml)
#
# Fitur (sama seperti run_single_3x.sh):
#   - Selang-seling: rep1 Flink → rep1 Spark → rep2 Flink → rep2 Spark → ...
#   - Truncate topic SEBELUM tiap run (delete + loop describe kosong)
#   - pkill producer lama + verify bersih
#   - SKIP otomatis kalau results/<fw>_central_rep<N>.csv sudah ada
#   - Hitung recovery otomatis (fault-time=120) ke *_recovery.txt
#
# Cara pakai:
#   Letakkan file ini di ROOT folder project (sejajar dengan scripts/)
#   bash run_central_3x.sh
# ============================================================
set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"
mkdir -p results

# ---- python untuk recovery script (Windows venv beda path) ----
PY="python3"
if [[ -x ".venv/Scripts/python.exe" ]]; then PY=".venv/Scripts/python.exe"; fi
if [[ -x ".venv/bin/python3" ]]; then PY=".venv/bin/python3"; fi

# ---- deteksi container kafka ----
KAFKA_CONTAINER="$(docker ps --format '{{.Names}}' | grep -iE 'kafka' | grep -viE 'exporter|zookeeper' | head -1)"
TOPIC="cic-topic"

# ============================================================
truncate_topic() {
    if [[ -z "$KAFKA_CONTAINER" ]]; then
        echo ">> WARNING: container Kafka tidak ketemu — SKIP truncate"
        return
    fi
    echo ">> Truncate topic '$TOPIC' di $KAFKA_CONTAINER"

    docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic "$TOPIC" >/dev/null 2>&1

    # tunggu sampai topic BENAR-BENAR hilang (delete async)
    for i in $(seq 1 30); do
        N=$(docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^${TOPIC}$" || true)
        if [[ "$N" == "0" ]]; then break; fi
        sleep 2
    done

    # recreate (2 partition, RF 1)
    docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 \
        --create --topic "$TOPIC" --partitions 2 --replication-factor 1 >/dev/null 2>&1

    for i in $(seq 1 30); do
        N=$(docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^${TOPIC}$" || true)
        if [[ "$N" == "1" ]]; then break; fi
        sleep 2
    done
    echo ">> Topic siap."
}

# ============================================================
run_one() {  # $1=framework (flink|spark), $2=rep
    local fw="$1" rep="$2"
    local arg="central-${fw}"
    local src="metrics/${fw}_central_component_failure.csv"
    local dst="results/${fw}_central_rep${rep}.csv"

    echo ""
    echo "=============================================================="
    echo "  ${fw^^} — CENTRAL — REPLIKASI $rep  ($(date '+%H:%M:%S'))"
    echo "  arg: $arg | fault-time: 120"
    echo "=============================================================="

    # Bersihkan producer lama
    pkill -9 -f "producer.py" 2>/dev/null || true
    sleep 2
    N=$(pgrep -af "producer.py" | wc -l)
    if [[ "$N" != "0" ]]; then
        echo "ERROR: masih ada $N producer — berhenti."
        exit 1
    fi

    truncate_topic

    # Jalankan eksperimen
    SKIP_CLEANUP=1 NUM_PRODUCERS=10 bash scripts/run_experiment.sh "$arg"
    RC=$?
    if [[ $RC -ne 0 ]]; then
        echo "ERROR: run_experiment.sh $arg gagal (rc=$RC) — berhenti di ${fw} central rep$rep."
        exit 1
    fi

    # Simpan CSV
    if [[ -f "$src" ]]; then
        cp "$src" "$dst"
        echo ">> CSV tersimpan: $dst"
    else
        echo "WARNING: $src tidak ditemukan — cek metrics/"
    fi

    # Hitung recovery
    if [[ -f "$dst" ]] && [[ -f "scripts/recovery_metrics_proposal.py" ]]; then
        echo ">> Recovery metrics (fault-time=120):"
        "$PY" scripts/recovery_metrics_proposal.py --csv "$dst" --fault-time 120 2>/dev/null \
            | tee "results/${fw}_central_rep${rep}_recovery.txt"
    fi

    echo ">> Selesai: $dst ($(date '+%H:%M:%S'))"
    sleep 5
}

# ============================================================
echo "=== Rencana: Flink central 3× + Spark central 3× (selang-seling) ==="
echo "Project root : $PROJECT_ROOT"
echo "Kafka        : $KAFKA_CONTAINER"
echo "Python       : $PY"
echo ""

for rep in 1 2 3; do
    F="results/flink_central_rep${rep}.csv"
    S="results/spark_central_rep${rep}.csv"
    if [[ -f "$F" && -f "$S" ]]; then
        echo ">> SKIP rep$rep (keduanya sudah ada)"
        continue
    fi
    if [[ ! -f "$F" ]]; then run_one flink "$rep"; fi
    if [[ ! -f "$S" ]]; then run_one spark "$rep"; fi
done

echo ""
echo "=============================================================="
echo "  SELESAI — Flink central 3× + Spark central 3×"
echo "  File: results/{flink,spark}_central_rep{1,2,3}.csv"
echo "=============================================================="
