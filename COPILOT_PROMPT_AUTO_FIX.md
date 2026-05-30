# COPILOT PROMPT — THESIS EXPERIMENT AUTO-FIX
# Rizky Zein Adam | Apache Flink vs Spark Fault Recovery Research
# Copy seluruh prompt ini ke Copilot Chat / GitHub Copilot / Cursor AI

---

## KONTEKS PROYEK

Kamu adalah asisten riset untuk tesis S2 yang mengevaluasi fault recovery pada Apache Flink vs Apache Spark Structured Streaming. Proyek ini berjalan di Docker containerized cluster dan menggunakan Kafka sebagai message broker.

Struktur direktori proyek:
```
/Percobaan Tesis/
├── config/
│   ├── load.yaml          # Pola dynamic load (100→300→700→1000→200 msg/s)
│   ├── fault.yaml         # Skenario multi-fault cascading
│   ├── fault_no_network.yaml
│   └── flink-conf.yaml
├── scripts/
│   ├── producer.py        # Kafka producer dengan dynamic load
│   ├── metrics_collector.py  # Prometheus metrics collector
│   ├── fault_injection.sh # Fault injection via docker kill
│   ├── run_experiment.sh  # Main orchestrator
│   └── generate_report.py # Report generator dari CSV
├── metrics/               # Output CSV dari eksperimen
├── results/               # Output laporan dan JSON
├── docker-compose-flink.yml
├── docker-compose-spark.yml
└── flink-jobs/target/cic-flink-job.jar
```

---

## MASALAH YANG HARUS DIPERBAIKI

### MASALAH 1 — Producer Tidak Menggunakan load.yaml (KRITIS)

**File:** `scripts/run_experiment.sh`

**Masalah saat ini:** Producer dipanggil tanpa `--config` sehingga menggunakan flat 100 msg/s (default). Padahal `load.yaml` sudah mendefinisikan pola dinamis yang benar.

**Yang harus dilakukan:**
1. Cari semua pemanggilan `producer.py` di `run_experiment.sh`
2. Tambahkan argumen `--config "${CONFIG_DIR}/load.yaml"` ke setiap pemanggilan
3. Pastikan variabel `CONFIG_DIR` didefinisikan dengan benar di awal script

**Contoh perbaikan:**
```bash
# SEBELUM (salah):
python3 "${SCRIPTS_DIR}/producer.py" --broker localhost:9092 --topic cic-topic --pattern step --duration 900

# SESUDAH (benar):
python3 "${SCRIPTS_DIR}/producer.py" \
  --broker localhost:9092 \
  --topic cic-topic \
  --config "${CONFIG_DIR}/load.yaml" \
  --duration 900
```

---

### MASALAH 2 — Checkpoint Spark di /tmp (Penyebab Data Loss 74,46%)

**File:** `docker-compose-spark.yml`

**Masalah saat ini:** Checkpoint Spark disimpan di `/tmp/spark-checkpoints` di dalam container. Saat container di-kill, `/tmp` hilang bersama checkpoint → batch recomputation gagal → data loss.

**Yang harus dilakukan:**
1. Buat direktori `./spark-checkpoints` di host
2. Tambahkan volume mount untuk `spark-worker1` dan `spark-worker2`
3. Tambahkan volume mount untuk `spark-driver` (master)
4. Pastikan semua container Spark mount ke direktori host yang sama

**Contoh perbaikan di docker-compose-spark.yml:**
```yaml
spark-worker1:
  volumes:
    - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
    - ./metrics.properties:/opt/spark/conf/metrics.properties:ro
    - ./spark-checkpoints:/tmp/spark-checkpoints  # TAMBAHKAN INI
    - ./spark-checkpoints:/tmp/spark-query-checkpoints  # TAMBAHKAN INI

spark-worker2:
  volumes:
    - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
    - ./metrics.properties:/opt/spark/conf/metrics.properties:ro
    - ./spark-checkpoints:/tmp/spark-checkpoints  # TAMBAHKAN INI
    - ./spark-checkpoints:/tmp/spark-query-checkpoints  # TAMBAHKAN INI

spark-master:
  volumes:
    - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
    - ./metrics.properties:/opt/spark/conf/metrics.properties:ro
    - ./spark-checkpoints:/tmp/spark-checkpoints  # TAMBAHKAN INI
```

Juga tambahkan di bagian `volumes:` di akhir file:
```yaml
volumes:
  prometheus-spark-data:
  grafana-spark-data:
  # tidak perlu named volume untuk spark-checkpoints karena bind mount ke host
```

---

### MASALAH 3 — Kafka Exporter Tidak Di-Scrape Prometheus

**File:** `prometheus-flink.yml` dan `prometheus-spark.yml`

**Masalah saat ini:** `kafka-exporter` sudah ada di docker-compose tapi Prometheus belum dikonfigurasi untuk scrape port 9308. Akibatnya `kafka_consumergroup_lag` tidak tersedia dan `metrics_collector.py` fallback ke synthetic proxy.

**Yang harus dilakukan:**
1. Buka `prometheus-flink.yml` (atau buat jika belum ada)
2. Tambahkan job scraping untuk kafka-exporter di port 9308
3. Lakukan hal sama untuk `prometheus-spark.yml`

**Contoh penambahan di prometheus-flink.yml:**
```yaml
scrape_configs:
  # ... job yang sudah ada ...
  
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['flink-kafka-exporter:9308']
    scrape_interval: 5s
    
  - job_name: 'flink-taskmanager1'
    static_configs:
      - targets: ['flink-taskmanager1:9249']
    scrape_interval: 5s
    
  - job_name: 'flink-taskmanager2'
    static_configs:
      - targets: ['flink-taskmanager2:9250']
    scrape_interval: 5s
```

---

### MASALAH 4 — Skenario Fault yang Hilang

**File:** `scripts/run_experiment.sh` dan buat file baru `config/fault_single_node.yaml` dan `config/fault_central_component.yaml`

**Masalah saat ini:** Hanya ada satu skenario fault (multi-fault cascading). Thesis menjanjikan tiga skenario: Single Node Failure, Central Component Failure, dan Multi-Fault Cascading.

**Yang harus dilakukan:**

**A. Buat `config/fault_single_node.yaml`:**
```yaml
# Single Node Failure — matikan satu worker, tidak di-restart
cascading_faults:
  - time: 120
    action: "kill_worker"
    target: "worker1"
    description: "Kill single worker node, no restart (permanent failure)"
  
recovery_detection_threshold: 2.0
max_recovery_time: 300
check_interval: 1
```

**B. Buat `config/fault_central_component.yaml`:**
```yaml
# Central Component Failure — matikan JobManager/Master lalu restart
cascading_faults:
  - time: 120
    action: "kill_worker"
    target: "jobmanager"
    description: "Kill central component (JobManager/Master)"
  - time: 180
    action: "restart_node"
    target: "jobmanager"
    description: "Restart central component after 60s outage"

recovery_detection_threshold: 2.0
max_recovery_time: 300
check_interval: 1
```

**C. Di `scripts/fault_injection.sh`, tambahkan mapping untuk `jobmanager`:**
```bash
get_container_name() {
    local system=$1
    local target=$2
    
    case "$system" in
        flink)
            case "$target" in
                worker1|taskmanager1) echo "flink-taskmanager1" ;;
                worker2|taskmanager2) echo "flink-taskmanager2" ;;
                jobmanager|central)   echo "flink-jobmanager" ;;  # TAMBAHKAN INI
                kafka) echo "flink-kafka" ;;
                *) echo "flink-jobmanager" ;;
            esac
            ;;
        spark)
            case "$target" in
                worker1) echo "spark-worker1" ;;
                worker2) echo "spark-worker2" ;;
                driver|master|central) echo "spark-driver" ;;  # TAMBAHKAN INI
                kafka) echo "spark-kafka" ;;
                *) echo "spark-driver" ;;
            esac
            ;;
    esac
}
```

**D. Di `scripts/run_experiment.sh`, tambahkan fungsi untuk setiap skenario baru:**
```bash
# Tambahkan setelah fungsi yang sudah ada

run_single_node_failure() {
    local SYSTEM=$1  # 'flink' atau 'spark'
    log "Running Single Node Failure scenario for $SYSTEM"
    
    FAULT_CONFIG="${CONFIG_DIR}/fault_single_node.yaml"
    OUTPUT_CSV="${METRICS_DIR}/${SYSTEM}_single_node_failure.csv"
    
    # Reset environment
    reset_experiment_state "$SYSTEM"
    sleep 10
    
    # Start producer dengan load konstan untuk baseline yang jelas
    python3 "${SCRIPTS_DIR}/producer.py" \
        --broker localhost:9092 \
        --topic cic-topic \
        --config "${CONFIG_DIR}/load.yaml" \
        --duration 600 &
    PRODUCER_PID=$!
    
    # Start collector
    python3 "${SCRIPTS_DIR}/metrics_collector.py" \
        --prometheus http://localhost:9090 \
        --output "$OUTPUT_CSV" \
        --duration 600 \
        --interval 1 &
    COLLECTOR_PID=$!
    
    # Inject fault
    sleep 5
    bash "${SCRIPTS_DIR}/fault_injection.sh" "$SYSTEM" "$FAULT_CONFIG" &
    
    wait $PRODUCER_PID
    log "Single Node Failure experiment complete: $OUTPUT_CSV"
}

run_central_component_failure() {
    local SYSTEM=$1
    log "Running Central Component Failure scenario for $SYSTEM"
    
    FAULT_CONFIG="${CONFIG_DIR}/fault_central_component.yaml"
    OUTPUT_CSV="${METRICS_DIR}/${SYSTEM}_central_component_failure.csv"
    
    reset_experiment_state "$SYSTEM"
    sleep 10
    
    python3 "${SCRIPTS_DIR}/producer.py" \
        --broker localhost:9092 \
        --topic cic-topic \
        --config "${CONFIG_DIR}/load.yaml" \
        --duration 600 &
    PRODUCER_PID=$!
    
    python3 "${SCRIPTS_DIR}/metrics_collector.py" \
        --prometheus http://localhost:9090 \
        --output "$OUTPUT_CSV" \
        --duration 600 \
        --interval 1 &
    COLLECTOR_PID=$!
    
    sleep 5
    bash "${SCRIPTS_DIR}/fault_injection.sh" "$SYSTEM" "$FAULT_CONFIG" &
    
    wait $PRODUCER_PID
    log "Central Component Failure experiment complete: $OUTPUT_CSV"
}
```

---

### MASALAH 5 — metrics_collector.py Tidak Menerima Dynamic load_rate

**File:** `scripts/metrics_collector.py`

**Masalah saat ini:** `run_continuous_collection()` menerima satu nilai `load_rate` yang konstan. Saat producer menggunakan `load.yaml` dengan rate yang berubah, metrik `load_rate` di CSV tidak mencerminkan rate aktual.

**Yang harus dilakukan:**
1. Tambahkan argumen `--load-config` ke main()
2. Buat method `get_current_load_rate(elapsed)` yang membaca load.yaml
3. Panggil method tersebut di setiap iterasi collection

**Perbaikan di `metrics_collector.py`:**
```python
def get_current_load_rate_from_config(self, elapsed: float, load_config: dict) -> int:
    """Dapatkan load rate aktual berdasarkan elapsed time dan load.yaml."""
    if not load_config or 'load_pattern' not in load_config:
        return self.default_load_rate
    
    time_acc = 0
    for stage in load_config['load_pattern']:
        stage_duration = stage['duration']
        if elapsed < time_acc + stage_duration:
            return int(stage['rate'])
        time_acc += stage_duration
    
    # Return rate terakhir jika elapsed melebihi total duration
    return int(load_config['load_pattern'][-1]['rate'])

def run_dynamic_collection(self, duration: int = 900, interval: int = 1, 
                           load_config: dict = None):
    """Collection dengan load_rate dinamis dari load.yaml."""
    logger.info(f"Starting dynamic collection for {duration}s")
    self.start_collection()
    
    elapsed = 0
    while elapsed < duration:
        elapsed = time.time() - self.start_time
        
        # Dapatkan rate aktual saat ini
        current_rate = self.get_current_load_rate_from_config(elapsed, load_config)
        
        self.collect_metrics(current_rate)
        time.sleep(interval)
    
    logger.info(f"Dynamic collection complete: {len(self.metrics)} samples")
```

Tambahkan di `main()`:
```python
parser.add_argument('--load-config', help='Path ke load.yaml untuk dynamic load rate')

# Di bagian eksekusi:
load_config = None
if args.load_config:
    import yaml
    with open(args.load_config) as f:
        load_config = yaml.safe_load(f)

if load_config:
    collector.run_dynamic_collection(
        duration=args.duration,
        interval=args.interval,
        load_config=load_config
    )
else:
    collector.run_continuous_collection(
        duration=args.duration,
        interval=args.interval,
        load_rate=args.load_rate
    )
```

---

## INSTRUKSI UNTUK COPILOT

Setelah membaca semua masalah di atas, lakukan hal berikut secara berurutan:

1. **Baca semua file yang relevan** sebelum melakukan perubahan apapun
2. **Perbaiki Masalah 1** di `run_experiment.sh` — tambahkan `--config load.yaml`
3. **Perbaiki Masalah 2** di `docker-compose-spark.yml` — tambahkan volume mount checkpoint
4. **Perbaiki Masalah 3** di `prometheus-flink.yml` dan `prometheus-spark.yml` — tambahkan kafka-exporter scraping
5. **Buat file baru** `fault_single_node.yaml` dan `fault_central_component.yaml`
6. **Perbaiki Masalah 4** di `fault_injection.sh` — tambahkan mapping jobmanager dan spark-driver
7. **Perbaiki Masalah 4** di `run_experiment.sh` — tambahkan fungsi skenario baru
8. **Perbaiki Masalah 5** di `metrics_collector.py` — tambahkan dynamic load rate

Setelah semua perubahan:
- Verifikasi tidak ada syntax error
- Pastikan semua path file konsisten dengan struktur direktori yang ada
- Buat ringkasan singkat perubahan apa saja yang sudah dilakukan dan file mana yang dimodifikasi

---

## VALIDASI SETELAH PERBAIKAN

Copilot harus memastikan hal berikut setelah semua perbaikan:

```bash
# 1. Cek producer menggunakan load.yaml
grep -n "load.yaml" scripts/run_experiment.sh

# 2. Cek volume mount Spark checkpoint
grep -n "spark-checkpoints" docker-compose-spark.yml

# 3. Cek kafka-exporter di prometheus config
grep -n "kafka-exporter" prometheus-flink.yml

# 4. Cek fault config baru tersedia
ls config/fault_single_node.yaml config/fault_central_component.yaml

# 5. Cek mapping jobmanager di fault_injection.sh
grep -n "jobmanager" scripts/fault_injection.sh

# 6. Cek dynamic load rate di metrics_collector.py
grep -n "get_current_load_rate" scripts/metrics_collector.py
```

Semua perintah di atas harus menghasilkan output yang valid — tidak boleh ada yang kosong atau error.
