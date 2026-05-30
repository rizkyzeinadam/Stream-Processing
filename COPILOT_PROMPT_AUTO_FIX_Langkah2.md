COPILOT PROMPT — FASE 2: SAMBUNGKAN SKENARIO FAULT + SELESAIKAN MASALAH SISA
Lanjutan dari COPILOT_PROMPT_AUTO_FIX.md (Fase 1 sudah selesai & validasi bersih)
Copy seluruh prompt ini ke Copilot Chat / Cursor AI

STATUS FASE 1 (SUDAH SELESAI)
Validasi berikut sudah lolos:

mcp_pylance tidak menemukan syntax error di metrics_collector.py
bash -n bersih untuk run_experiment.sh dan fault_injection.sh
Volume mount checkpoint Spark sudah ditambahkan di docker-compose-spark.yml
Kafka-exporter scraping sudah ditambahkan di prometheus-flink.yml dan prometheus-spark.yml
fault_single_node.yaml dan fault_central_component.yaml sudah dibuat


TUGAS FASE 2 — 4 PEKERJAAN BERURUTAN
TUGAS 1 — Sambungkan Skenario Fault Baru ke CLI run_experiment.sh
Konteks: Fungsi run_single_node_failure() dan run_central_component_failure()
sudah ada di run_experiment.sh tapi belum terhubung ke argument parsing / menu CLI.
Yang harus dilakukan:

Baca seluruh isi run_experiment.sh untuk memahami pola CLI yang sudah ada
Temukan bagian argument parsing atau case/menu statement di bagian bawah script
Tambahkan entri baru yang konsisten dengan pola yang sudah ada

Pola yang harus diikuti (sesuaikan dengan pola eksisting di file):
bash# Jika menggunakan case statement dengan argumen posisional:
case "${1}" in
  # ... yang sudah ada ...
  "single-node-flink")
    run_single_node_failure "flink"
    ;;
  "single-node-spark")
    run_single_node_failure "spark"
    ;;
  "central-flink")
    run_central_component_failure "flink"
    ;;
  "central-spark")
    run_central_component_failure "spark"
    ;;
  "all-scenarios")
    # Jalankan semua skenario secara berurutan
    run_flink_dynamic_load
    run_spark_dynamic_load
    run_single_node_failure "flink"
    run_single_node_failure "spark"
    run_central_component_failure "flink"
    run_central_component_failure "spark"
    ;;
esac

# Jika menggunakan getopts atau flag --scenario:
# Tambahkan case "single-node-flink"|"single-node-spark"|"central-flink"|"central-spark"
# dengan pemanggilan fungsi yang sesuai
Tambahkan juga di bagian usage/help:
bashusage() {
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
Validasi setelah selesai:
bashbash -n run_experiment.sh          # Harus bersih, tidak ada syntax error
./run_experiment.sh --help         # Harus tampilkan usage yang diupdate
grep -n "single-node\|central" run_experiment.sh  # Harus ada entri baru

TUGAS 2 — Tambahkan cAdvisor untuk CPU Per-Container
Konteks: CPU saat ini diukur host-level via node_exporter. Reviewer mempertanyakan
validitas karena tidak terisolasi per container. cAdvisor memberikan CPU per-container
yang lebih akurat.
Yang harus dilakukan di docker-compose-flink.yml:
yaml# Tambahkan service baru setelah grafana:
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.47.0
    container_name: flink-cadvisor
    privileged: true
    ports:
      - "8088:8080"
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    networks:
      - flink-network
    mem_limit: 256m
    command:
      - '--housekeeping_interval=5s'
      - '--docker_only=true'
      - '--disable_metrics=disk,network,tcp,udp,percpu,sched,process'
Lakukan hal sama di docker-compose-spark.yml dengan container_name spark-cadvisor
dan port 8089:8080 serta network spark-network.
Tambahkan scraping cAdvisor di prometheus-flink.yml:
yaml  - job_name: 'cadvisor'
    static_configs:
      - targets: ['flink-cadvisor:8080']
    scrape_interval: 5s
    metrics_path: /metrics
Tambahkan query CPU per-container di metrics_collector.py:
pythondef get_cpu_per_container(self, container_name: str) -> float:
    """Get CPU usage untuk container spesifik via cAdvisor metrics."""
    query = f'rate(container_cpu_usage_seconds_total{{name="{container_name}"}}[1m]) * 100'
    result = self.query_prometheus(query)
    if result and result.get('data', {}).get('result'):
        try:
            return float(result['data']['result'][0]['value'][1])
        except (IndexError, ValueError):
            return 0.0
    return 0.0

def get_framework_cpu(self, framework: str) -> dict:
    """Get CPU usage untuk semua container framework."""
    containers = {
        'flink': ['flink-jobmanager', 'flink-taskmanager1', 'flink-taskmanager2'],
        'spark': ['spark-driver', 'spark-worker1', 'spark-worker2']
    }
    result = {}
    for container in containers.get(framework, []):
        result[container] = self.get_cpu_per_container(container)
    result['total'] = sum(result.values())
    return result
Validasi:
bashgrep -n "cadvisor" docker-compose-flink.yml    # Harus ada
grep -n "cadvisor" docker-compose-spark.yml    # Harus ada
grep -n "cadvisor" prometheus-flink.yml        # Harus ada
grep -n "get_cpu_per_container" metrics_collector.py  # Harus ada

TUGAS 3 — Tambahkan Script Diagnosa Log Spark
Konteks: Data loss Spark 74,46% belum terjelaskan secara mekanis di thesis.
Perlu script yang mengekstrak bukti konkret dari log runtime Spark.
Buat file baru scripts/diagnose_spark_loss.sh:
bash#!/bin/bash
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
docker exec spark-driver cat /tmp/cic-spark-job.log 2>/dev/null \
  | grep -iE "error|exception|lost|failed|checkpoint|kafka|offset" \
  > "$RESULTS_DIR/job_errors.txt" || echo "(log tidak ditemukan)" \
  > "$RESULTS_DIR/job_errors.txt"
echo "      Found: $(wc -l < "$RESULTS_DIR/job_errors.txt") lines"

# 3. Status checkpoint directory
echo "[3/6] Memeriksa status checkpoint..."
docker exec spark-driver ls -laR /tmp/spark-checkpoints/ 2>/dev/null \
  > "$RESULTS_DIR/checkpoint_state.txt" \
  || echo "CHECKPOINT DIR KOSONG ATAU TIDAK ADA" \
  > "$RESULTS_DIR/checkpoint_state.txt"
echo "      Status: $(head -1 "$RESULTS_DIR/checkpoint_state.txt")"

# 4. Worker1 error saat kill
echo "[4/6] Mengekstrak error dari spark-worker1..."
docker logs spark-worker1 2>&1 \
  | grep -iE "error|lost|killed|exception|disconnected" \
  > "$RESULTS_DIR/worker1_errors.txt" || true
echo "      Found: $(wc -l < "$RESULTS_DIR/worker1_errors.txt") lines"

# 5. Kafka consumer group offset
echo "[5/6] Memeriksa Kafka consumer group lag..."
docker exec spark-kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe \
  --group spark-group 2>/dev/null \
  > "$RESULTS_DIR/kafka_consumer_group.txt" \
  || echo "Tidak bisa query Kafka consumer group" \
  > "$RESULTS_DIR/kafka_consumer_group.txt"
echo "      $(cat "$RESULTS_DIR/kafka_consumer_group.txt" | head -3)"

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
Buat executable:
bashchmod +x scripts/diagnose_spark_loss.sh
Validasi:
bashbash -n scripts/diagnose_spark_loss.sh   # Harus bersih
ls -la scripts/diagnose_spark_loss.sh    # Harus ada dan executable

TUGAS 4 — Rapikan Path di COPILOT_PROMPT_AUTO_FIX.md
Konteks: Path di dokumentasi menggunakan referensi absolut yang harus diganti
dengan path relatif dari root repo agar konsisten dengan struktur aktual.
Yang harus dilakukan:

Baca run_experiment.sh untuk mendapatkan nilai PROJECT_ROOT yang aktual
Baca build_flink_job.sh untuk verifikasi path FLINK_JOB_DIR
Update semua path di COPILOT_PROMPT_AUTO_FIX.md agar konsisten
Ganti semua referensi path hardcoded dengan variabel relatif atau path yang benar

Pattern yang harus dicari dan diupdate:
# Cari referensi absolut project root yang lama.

# Ganti dengan path relatif dari project root, contoh:
./config/
./scripts/
./metrics/
./results/
./results/

URUTAN EKSEKUSI
Kerjakan tugas dalam urutan ini:

Tugas 1 dulu (paling kritis — memungkinkan eksperimen berjalan)
Tugas 3 (buat script diagnosa — butuh sebelum cluster di-stop)
Tugas 2 (cAdvisor — perlu docker-compose restart, jadwalkan sebelum eksperimen berikutnya)
Tugas 4 (dokumentasi — bisa paling terakhir)


VALIDASI AKHIR KESELURUHAN
Setelah semua tugas selesai, jalankan validasi berikut:
bash# Syntax check semua script
bash -n run_experiment.sh && echo "run_experiment.sh OK"
bash -n scripts/fault_injection.sh && echo "fault_injection.sh OK"
bash -n scripts/diagnose_spark_loss.sh && echo "diagnose_spark_loss.sh OK"

# Cek entri CLI baru
./run_experiment.sh --help | grep -E "single-node|central"

# Cek cAdvisor di docker-compose
grep -c "cadvisor" docker-compose-flink.yml
grep -c "cadvisor" docker-compose-spark.yml

# Cek prometheus scraping
grep -c "cadvisor\|kafka-exporter" prometheus-flink.yml

# Cek fungsi baru di metrics_collector.py
python3 -c "from metrics_collector import MetricsCollector; \
  m = MetricsCollector(); \
  print('get_cpu_per_container:', hasattr(m, 'get_cpu_per_container'))"

# Cek script diagnosa ada dan executable
test -x scripts/diagnose_spark_loss.sh && echo "diagnose script OK"

# Cek path di dokumentasi sudah bersih
grep -c "PATH HARDCODED" COPILOT_PROMPT_AUTO_FIX_Langkah2.md && echo "MASIH ADA PATH HARDCODED" \
  || echo "Path sudah bersih"
Semua output harus OK atau 0 error.

CATATAN PENTING UNTUK COPILOT

Jangan ubah logika eksperimen yang sudah ada — hanya tambahkan yang kurang
Pertahankan gaya kode yang sudah ada (indentasi, naming convention, logging style)
Setiap perubahan harus backward-compatible — script lama tetap bisa dijalankan
Jika ada konflik antara instruksi di prompt ini dan kode eksisting, prioritaskan kode eksisting dan tanyakan konfirmasi