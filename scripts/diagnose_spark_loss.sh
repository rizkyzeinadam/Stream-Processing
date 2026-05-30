#!/bin/bash
# Script diagnosa penyebab data loss Spark
# Jalankan SETELAH eksperimen Spark selesai, SEBELUM container di-stop

set -e

RESULTS_DIR="${1:-./results/spark_diagnosis}"
mkdir -p "$RESULTS_DIR"

echo "=== DIAGNOSA DATA LOSS SPARK ==="
echo "Output disimpan ke: $RESULTS_DIR"
echo ""

# 1. Error dari spark-driver (Master)
echo "[1/6] Mengekstrak error dari spark-driver..."
docker logs spark-driver 2>&1 \
  | grep -iE "error|exception|lost|failed|killed|timeout" \
  | grep -v "INFO" \
  > "$RESULTS_DIR/driver_errors.txt" || true
echo "      Found: $(wc -l < "$RESULTS_DIR/driver_errors.txt") lines"

# 2. Job application log
echo "[2/6] Mengekstrak application log..."
if docker exec spark-driver cat /tmp/cic-spark-job.log 2>/dev/null \
  | grep -iE "error|exception|lost|failed|checkpoint|kafka|offset" \
  > "$RESULTS_DIR/job_errors.txt"; then
  :
else
  echo "(log tidak ditemukan)" > "$RESULTS_DIR/job_errors.txt"
fi
echo "      Found: $(wc -l < "$RESULTS_DIR/job_errors.txt") lines"

# 3. Status checkpoint directory
echo "[3/6] Memeriksa status checkpoint..."
if docker exec spark-driver ls -laR /tmp/spark-checkpoints/ 2>/dev/null > "$RESULTS_DIR/checkpoint_state.txt"; then
  :
else
  echo "CHECKPOINT DIR KOSONG ATAU TIDAK ADA" > "$RESULTS_DIR/checkpoint_state.txt"
fi
echo "      Status: $(head -1 "$RESULTS_DIR/checkpoint_state.txt")"

# 4. Worker1 error saat kill
echo "[4/6] Mengekstrak error dari spark-worker1..."
docker logs spark-worker1 2>&1 \
  | grep -iE "error|lost|killed|exception|disconnected" \
  > "$RESULTS_DIR/worker1_errors.txt" || true
echo "      Found: $(wc -l < "$RESULTS_DIR/worker1_errors.txt") lines"

# 5. Kafka consumer group offset
echo "[5/6] Memeriksa Kafka consumer group lag..."
if docker exec spark-kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe \
  --group spark-group 2>/dev/null \
  > "$RESULTS_DIR/kafka_consumer_group.txt"; then
  :
else
  echo "Tidak bisa query Kafka consumer group" > "$RESULTS_DIR/kafka_consumer_group.txt"
fi
echo "      $(head -3 "$RESULTS_DIR/kafka_consumer_group.txt")"

# 6. Summary diagnosis
echo "[6/6] Membuat ringkasan diagnosis..."
cat > "$RESULTS_DIR/DIAGNOSIS_SUMMARY.txt" << EOF
=== DIAGNOSIS DATA LOSS SPARK ===
Generated: $(date)

CHECKPOINT STATUS:
$(cat "$RESULTS_DIR/checkpoint_state.txt" | head -5)

TOP ERRORS (driver):
$(cat "$RESULTS_DIR/driver_errors.txt" | head -10)

TOP ERRORS (job log):
$(cat "$RESULTS_DIR/job_errors.txt" | head -10)

KAFKA LAG:
$(cat "$RESULTS_DIR/kafka_consumer_group.txt")

KESIMPULAN:
$(if grep -qi "No such file\|KOSONG" "$RESULTS_DIR/checkpoint_state.txt"; then
  echo "ROOT CAUSE TERIDENTIFIKASI: Checkpoint directory kosong/hilang."
  echo "Saat spark-worker1 di-kill, /tmp/spark-checkpoints hilang bersama container."
  echo "Batch recomputation gagal karena tidak ada checkpoint valid."
  echo "FIX: Mount checkpoint ke host volume (sudah dilakukan di docker-compose-spark.yml)"
elif grep -qi "executor.*lost\|lost.*executor" "$RESULTS_DIR/driver_errors.txt" "$RESULTS_DIR/job_errors.txt" 2>/dev/null; then
  echo "ROOT CAUSE TERIDENTIFIKASI: Executor loss."
  echo "Spark driver kehilangan koneksi ke executor saat worker di-kill."
  echo "Tanpa checkpoint valid, recomputation tidak bisa dimulai dari state terakhir."
else
  echo "Root cause perlu analisis manual dari file di direktori ini."
fi)
EOF

echo ""
echo "=== SELESAI ==="
echo "Baca DIAGNOSIS_SUMMARY.txt untuk temuan utama:"
echo ""
cat "$RESULTS_DIR/DIAGNOSIS_SUMMARY.txt"
