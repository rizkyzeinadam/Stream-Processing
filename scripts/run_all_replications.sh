#!/usr/bin/env bash
# ============================================================
# Run All Replications — jalankan replikasi untuk framework × skenario
# Menggunakan argumen run_experiment.sh yang SUDAH didukung:
#   flink / spark / single-node-flink / single-node-spark / central-flink / central-spark
#
# Mapping argumen & CSV output:
#   (flink, multi)   -> flink                        -> metrics/flink_metrics.csv
#   (spark, multi)   -> spark                        -> metrics/spark_metrics.csv
#   (flink, single)  -> single-node-flink            -> metrics/flink_single_node_failure.csv
#   (spark, single)  -> single-node-spark            -> metrics/spark_single_node_failure.csv
#   (flink, central) -> central-flink                -> metrics/flink_central_component_failure.csv
#   (spark, central) -> central-spark                -> metrics/spark_central_component_failure.csv
#
# ⚠️ RESET PENUH ANTAR REPLIKASI (v2 — fix kontaminasi rep2):
#   Tiap ganti replikasi, script melakukan:
#   1. pkill producer lama
#   2. restart container framework (matikan job lama yang masih running)
#   3. delete + recreate topic cic-topic (buang sisa data rep sebelumnya)
#   4. hapus checkpoint dirs (biar Flink/Spark mulai dari state bersih)
#
# Cara pakai:
#   bash run_all_replications.sh flink multi 3     # 3x Flink multi-fault
#   bash run_all_replications.sh spark single 3    # 3x Spark single node
#   bash run_all_replications.sh all all 3         # SEMUA (6 kombinasi x 3 = 18 run)
#
# Hasil:
#   results/<framework>_<skenario>_rep<N>.csv
#   results/<framework>_<skenario>_rep<N>_recovery.txt
# ============================================================
set -u

FRAMEWORK="${1:-all}"
SCENARIO="${2:-all}"
REPS="${3:-3}"

# ---- mapping ----
arg_for() {  # $1=framework $2=skenario -> argumen run_experiment.sh
    case "$1-$2" in
        flink-multi)   echo "flink" ;;
        spark-multi)   echo "spark" ;;
        flink-single)  echo "single-node-flink" ;;
        spark-single)  echo "single-node-spark" ;;
        flink-central) echo "central-flink" ;;
        spark-central) echo "central-spark" ;;
        *) echo "" ;;
    esac
}
csv_for() {  # $1=framework $2=skenario -> file CSV yang dihasilkan run
    case "$1-$2" in
        flink-multi)   echo "metrics/flink_metrics.csv" ;;
        spark-multi)   echo "metrics/spark_metrics.csv" ;;
        flink-single)  echo "metrics/flink_single_node_failure.csv" ;;
        spark-single)  echo "metrics/spark_single_node_failure.csv" ;;
        flink-central) echo "metrics/flink_central_component_failure.csv" ;;
        spark-central) echo "metrics/spark_central_component_failure.csv" ;;
        *) echo "" ;;
    esac
}
fault_time_for() {
    case "$2" in
        multi)   echo "330" ;;
        single|central) echo "120" ;;
        *) echo "330" ;;
    esac
}
kafka_container_for() {  # $1=framework -> nama container Kafka
    case "$1" in
        flink) echo "flink-kafka" ;;
        spark) echo "spark-kafka" ;;
        *) echo "flink-kafka" ;;
    esac
}
compose_file_for() {
    # Compose files ada di subfolder docker-compose/, bukan project root.
    case "$1" in
        flink) echo "docker-compose/docker-compose-flink.yml" ;;
        spark) echo "docker-compose/docker-compose-spark.yml" ;;
        *) echo "docker-compose/docker-compose-flink.yml" ;;
    esac
}


# ---- framework & skenario list ----
FRAMEWORKS=()
if [[ "$FRAMEWORK" == "all" ]]; then FRAMEWORKS=(flink spark); else FRAMEWORKS=("$FRAMEWORK"); fi
SCENARIOS=()
if [[ "$SCENARIO" == "all" ]]; then SCENARIOS=(multi single central); else SCENARIOS=("$SCENARIO"); fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
mkdir -p results

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ============================================================
# RESET PENUH antar replikasi — fix kontaminasi
# ============================================================
reset_stack() {
    local fw="$1"
    local KAFKA_CONTAINER
    KAFKA_CONTAINER="$(kafka_container_for "$fw")"
    local COMPOSE
    COMPOSE="$(compose_file_for "$fw")"

    log "=== RESET stack $fw antar replikasi ==="

    # 1. Matikan semua producer lama
    pkill -9 -f "producer.py" 2>/dev/null || true
    sleep 2
    local N
    N=$(pgrep -af "producer.py" | wc -l)
    if [[ "$N" != "0" ]]; then
        log "ERROR: masih ada $N producer — berhenti."
        exit 1
    fi

    # 2. Restart container framework (matikan job lama yang masih running)
    #    Flink: jobmanager + taskmanager; Spark: driver + master + workers
    log "Restart container framework ($COMPOSE)..."
    if command -v docker compose &>/dev/null; then
        docker compose -f "$COMPOSE" restart \
            > /tmp/reset_stack.log 2>&1 || docker-compose -f "$COMPOSE" restart >> /tmp/reset_stack.log 2>&1
    else
        docker-compose -f "$COMPOSE" restart >> /tmp/reset_stack.log 2>&1
    fi
    sleep 10

    # 3. Delete + recreate topic cic-topic (buang sisa data rep sebelumnya).
    #    Delete Kafka itu ASYNC — loop sampai beneran hilang, bukan sleep statis.
    log "Reset topic cic-topic di $KAFKA_CONTAINER (delete + loop sampai kosong)..."
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
        --delete --topic cic-topic >> /tmp/reset_stack.log 2>&1
    local i n
    for i in $(seq 1 30); do
        n=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^cic-topic$" || true)
        [[ "$n" == "0" ]] && break
        sleep 2
    done
    if [[ "$n" != "0" ]]; then
        log "ERROR: topic cic-topic tidak hilang setelah 60s — truncate GAGAL. Berhenti."
        exit 1
    fi
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
        --create --topic cic-topic --partitions 2 --replication-factor 1 >> /tmp/reset_stack.log 2>&1
    for i in $(seq 1 30); do
        n=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^cic-topic$" || true)
        [[ "$n" == "1" ]] && break
        sleep 2
    done
    if [[ "$n" != "1" ]]; then
        log "ERROR: topic cic-topic tidak muncul setelah create — berhenti."
        exit 1
    fi
    log "Topic cic-topic siap (bersih)."


    # 4. Hapus checkpoint dirs (state bersih)
    log "Reset checkpoint dirs..."
    if [[ "$fw" == "flink" ]]; then
        docker exec flink-jobmanager rm -rf /tmp/flink-checkpoints 2>/dev/null || true
        docker exec flink-taskmanager1 rm -rf /tmp/flink-checkpoints 2>/dev/null || true
        docker exec flink-taskmanager2 rm -rf /tmp/flink-checkpoints 2>/dev/null || true
    else
        docker exec spark-driver rm -rf /tmp/spark-checkpoints 2>/dev/null || true
        docker exec spark-master rm -rf /tmp/spark-checkpoints 2>/dev/null || true
    fi

    # 5. Verifikasi Kafka healthy
    sleep 5
    log "Reset selesai."
}

# ============================================================
# MAIN LOOP
# ============================================================
echo "=== Rencana run ==="
TOTAL=0
for fw in "${FRAMEWORKS[@]}"; do
    for sc in "${SCENARIOS[@]}"; do
        echo "  $fw $sc x$REPS  (arg: $(arg_for "$fw" "$sc"), fault-time: $(fault_time_for "$fw" "$sc"))"
        TOTAL=$((TOTAL + REPS))
    done
done
echo "Total: $TOTAL run (tiap replikasi di-reset penuh)"
echo ""

for fw in "${FRAMEWORKS[@]}"; do
    for sc in "${SCENARIOS[@]}"; do
        ARG="$(arg_for "$fw" "$sc")"
        if [[ -z "$ARG" ]]; then
            echo "ERROR: kombinasi tidak dikenal: $fw $sc"; exit 1
        fi
        SRC_CSV="$(csv_for "$fw" "$sc")"
        FT="$(fault_time_for "$fw" "$sc")"

        for ((rep = 1; rep <= REPS; rep++)); do
            DST="results/${fw}_${sc}_rep${rep}.csv"
            if [[ -f "$DST" ]]; then
                echo ">> SKIP: $DST sudah ada"
                continue
            fi

            echo ""
            echo "=============================================================="
            echo "  ${fw^^} — ${sc^^} — REPLIKASI $rep  ($(date '+%H:%M:%S'))"
            echo "  arg: $ARG | fault-time: $FT"
            echo "=============================================================="

            # RESET PENUH sebelum tiap replikasi
            reset_stack "$fw"

            # Jalankan eksperimen (argumen resmi run_experiment.sh)
            SKIP_CLEANUP=1 NUM_PRODUCERS=10 bash scripts/run_experiment.sh "$ARG"
            RC=$?
            if [[ $RC -ne 0 ]]; then
                echo "ERROR: run_experiment.sh $ARG gagal (rc=$RC) — berhenti di ${fw} ${sc} rep$rep."
                exit 1
            fi

            # Simpan CSV (nama output beda per skenario — pakai mapping)
            if [[ -f "$SRC_CSV" ]]; then
                cp "$SRC_CSV" "$DST"
                echo ">> CSV tersimpan: $DST"
            else
                echo "WARNING: $SRC_CSV tidak ditemukan — cek metrics/"
            fi

            # Hitung recovery (opsional)
            REC_SCRIPT="scripts/recovery_metrics_proposal.py"
            if [[ -f "$REC_SCRIPT" ]] && [[ -f "$DST" ]]; then
                echo ">> Recovery metrics (fault-time=$FT):"
                "$PROJECT_ROOT/.venv/bin/python3" "$REC_SCRIPT" --csv "$DST" --fault-time "$FT" 2>/dev/null \
                    | tee "results/${fw}_${sc}_rep${rep}_recovery.txt"
            fi

            echo ">> Selesai: $DST ($(date '+%H:%M:%S'))"
            sleep 5
        done
    done
done

echo ""
echo "=============================================================="
echo "  SELESAI — semua replikasi"
echo "  File: results/<framework>_<skenario>_rep*.csv"
echo "=============================================================="

# ============================================================
# FINAL CLEANUP — bersihkan state setelah SEMUA replikasi selesai
# ============================================================
if [[ "${FINAL_CLEANUP:-1}" == "1" ]]; then
    log "=== FINAL CLEANUP (semua replikasi selesai) ==="
    # Matikan semua producer yang tersisa
    pkill -9 -f "producer.py" 2>/dev/null || true
    sleep 2
    # Reset topic cic-topic (buang semua sisa data) — delete + loop sampai kosong
    for KAFKA_CONTAINER in "$(kafka_container_for "${FRAMEWORKS[0]}")"; do
        docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
            --delete --topic cic-topic >> /tmp/reset_stack.log 2>&1
        local i n
        for i in $(seq 1 30); do
            n=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^cic-topic$" || true)
            [[ "$n" == "0" ]] && break
            sleep 2
        done
        docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
            --create --topic cic-topic --partitions 2 --replication-factor 1 >> /tmp/reset_stack.log 2>&1
        for i in $(seq 1 30); do
            n=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -c "^cic-topic$" || true)
            [[ "$n" == "1" ]] && break
            sleep 2
        done
    done
    log "FINAL CLEANUP selesai — semua state dibersihkan."

fi

echo "=============================================================="
echo "  DONE — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================================="