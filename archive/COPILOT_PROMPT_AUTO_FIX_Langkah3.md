COPILOT PROMPT — FASE 3: FIX PATH + SIAP EKSEKUSI NYATA
Dry run sudah lolos 6/6. Satu masalah path ditemukan sebelum eksekusi nyata.
Copy seluruh prompt ini ke Copilot Chat / Cursor AI

STATUS FASE 2 (SUDAH SELESAI)
Dry run python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --dry-run
menghasilkan 6/6 sukses dengan semua skenario terdaftar:

flink (900s)
spark (900s)
single-node-flink (600s)
single-node-spark (600s)
central-flink (600s)
central-spark (600s)


SATU MASALAH YANG DITEMUKAN
Path Output Salah — metrics/ ada di dalam scripts/
Masalah: Output CSV ditulis ke:
/Users/rizkyzeinadam/Documents/Percobaan Tesis/scripts/metrics/run01_flink_dynamic_load.csv
Yang benar seharusnya:
/Users/rizkyzeinadam/Documents/Percobaan Tesis/metrics/run01_flink_dynamic_load.csv
Root cause di experiment_orchestrator.py:
python# SEKARANG (salah) — PROJECT_ROOT menunjuk ke folder scripts/
PROJECT_ROOT  = Path(__file__).resolve().parent

# Kalau orchestrator.py ada di scripts/, maka:
# PROJECT_ROOT = .../Percobaan Tesis/scripts/
# METRICS_DIR  = .../Percobaan Tesis/scripts/metrics/  ← SALAH
Fix yang diperlukan:
python# BENAR — naik satu level ke root project
PROJECT_ROOT  = Path(__file__).resolve().parent.parent

# Atau alternatif — deteksi otomatis berdasarkan keberadaan docker-compose
def find_project_root() -> Path:
    """Cari project root dengan mencari docker-compose-flink.yml."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # maksimal 5 level ke atas
        if (current / 'docker-compose-flink.yml').exists():
            return current
        current = current.parent
    # Fallback ke parent dari lokasi script
    return Path(__file__).resolve().parent.parent

PROJECT_ROOT = find_project_root()
Yang harus dilakukan:

Baca experiment_orchestrator.py untuk cek di mana file ini berada
Cek apakah file ada di scripts/ atau di root project
Jika ada di scripts/: ganti Path(__file__).resolve().parent
menjadi find_project_root() dengan fungsi di atas
Jika sudah di root project: path sudah benar, tidak perlu diubah
Verifikasi dengan dry run ulang bahwa path output tidak ada scripts/metrics/

Validasi:
bashpython3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --dry-run \
  | grep "output"
# Output TIDAK boleh mengandung /scripts/metrics/
# Output HARUS mengarah ke /Percobaan Tesis/metrics/

TUGAS TAMBAHAN SEBELUM EKSEKUSI NYATA
Tugas A — Tambahkan Pre-flight Check
Sebelum eksperimen dijalankan, orchestrator harus memverifikasi semua
dependensi tersedia. Tambahkan fungsi preflight_check() di
experiment_orchestrator.py:
pythondef preflight_check(dry_run: bool = False) -> bool:
    """Verifikasi semua dependensi sebelum eksperimen dimulai."""
    if dry_run:
        return True

    checks = []
    errors = []

    # 1. Docker running
    result = subprocess.run(['docker', 'info'], capture_output=True, timeout=10)
    ok = result.returncode == 0
    checks.append(('Docker daemon', ok))
    if not ok:
        errors.append("Docker tidak running. Jalankan: open -a Docker")

    # 2. Config files ada
    for cfg in ['load.yaml', 'fault_no_network.yaml',
                'fault_single_node.yaml', 'fault_central_component.yaml']:
        path = CONFIG_DIR / cfg
        ok = path.exists()
        checks.append((f'Config: {cfg}', ok))
        if not ok:
            errors.append(f"Config tidak ditemukan: {path}")

    # 3. Scripts ada dan executable
    for script in ['producer.py', 'metrics_collector.py']:
        path = SCRIPTS_DIR / script
        ok = path.exists()
        checks.append((f'Script: {script}', ok))
        if not ok:
            errors.append(f"Script tidak ditemukan: {path}")

    for script in ['fault_injection.sh']:
        path = SCRIPTS_DIR / script
        ok = path.exists() and os.access(path, os.X_OK)
        checks.append((f'Script (exec): {script}', ok))
        if not ok:
            errors.append(f"Script tidak ada atau tidak executable: {path}")

    # 4. Flink JAR ada
    jar = PROJECT_ROOT / 'flink-jobs' / 'target' / 'cic-flink-job.jar'
    ok = jar.exists()
    checks.append(('Flink JAR', ok))
    if not ok:
        errors.append(f"Flink JAR tidak ditemukan: {jar}. Jalankan build_flink_job.sh")

    # 5. Spark JAR ada
    jar = PROJECT_ROOT / 'spark-jobs' / 'target' / 'cic-spark-job-1.0.0.jar'
    ok = jar.exists()
    checks.append(('Spark JAR', ok))
    if not ok:
        errors.append(f"Spark JAR tidak ditemukan: {jar}")

    # 6. Dataset parquet ada
    dataset = PROJECT_ROOT / 'cic-collection.parquet'
    ok = dataset.exists()
    checks.append(('Dataset parquet', ok))
    if not ok:
        errors.append(f"Dataset tidak ditemukan: {dataset}")

    # Print hasil check
    log.info("=== PRE-FLIGHT CHECK ===")
    for name, passed in checks:
        status = "OK" if passed else "FAIL"
        log.info(f"  [{status}] {name}")

    if errors:
        log.error("Pre-flight GAGAL:")
        for err in errors:
            log.error(f"  - {err}")
        return False

    log.info("Pre-flight semua OK — siap eksekusi")
    return True
Panggil preflight_check() di awal main() sebelum loop skenario:
pythondef main() -> None:
    args = parse_args()
    # ... setup log ...

    if not preflight_check(dry_run=args.dry_run):
        log.error("Batalkan eksperimen karena pre-flight gagal.")
        sys.exit(1)

    # ... lanjut ke loop skenario ...
Tugas B — Tambahkan Progress Estimasi Waktu
Tambahkan estimasi waktu tersisa di setiap iterasi:
python# Di dalam loop setelah result = run_scenario(...)
total_remaining = sum(
    s.duration for s in selected[current:]
) + (total - current) * args.cooldown

hours, rem = divmod(total_remaining, 3600)
mins = rem // 60
log.info(f"  Estimasi sisa waktu: {hours}j {mins}m")

VALIDASI AKHIR FASE 3
bash# 1. Dry run — pastikan path output benar (tidak ada /scripts/metrics/)
python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --dry-run \
  | grep "output" | grep -v "scripts/metrics" && echo "PATH OK"

# 2. Pre-flight check tanpa dry-run (tapi jangan eksekusi eksperimen)
python3 -c "
import sys; sys.argv = ['x']
from experiment_orchestrator import preflight_check, CONFIG_DIR, SCRIPTS_DIR
result = preflight_check(dry_run=False)
print('Pre-flight:', 'PASS' if result else 'FAIL — cek error di atas')
"

# 3. Syntax check final
python3 -m py_compile experiment_orchestrator.py && echo "SYNTAX OK"

SETELAH FASE 3 SELESAI — JALANKAN EKSPERIMEN NYATA
Urutan yang direkomendasikan:
bash# Step 1: Smoke test satu skenario pendek dulu (~10 menit)
python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --scenario central-flink

# Step 2: Cek CSV output terbuat
ls -lh metrics/run01_flink_central_component_failure.csv

# Step 3: Kalau step 1-2 OK, jalankan semua (~75 menit total)
python3 experiment_orchestrator.py --num-runs 1 --cooldown 30

# Step 4: Setelah selesai, jalankan diagnosa Spark
bash scripts/diagnose_spark_loss.sh ./results/spark_diagnosis

CATATAN PENTING

Jangan jalankan --num-runs 1 --cooldown 30 (semua skenario) sebelum
smoke test central-flink berhasil end-to-end
Kalau smoke test gagal di tengah, cek /tmp/orchestrator.log untuk detail error
Total waktu semua eksperimen: ~75 menit eksekusi + overhead cluster setup
Pastikan laptop tidak sleep selama eksperimen berjalan