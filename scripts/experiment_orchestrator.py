#!/usr/bin/env python3
"""
experiment_orchestrator.py
Orchestrator utama untuk menjalankan semua skenario eksperimen tesis.
Memanggil run_experiment.sh, producer.py, metrics_collector.py, dan fault_injection.sh
secara terkoordinasi untuk setiap skenario dan framework.
 
Usage:
    python3 experiment_orchestrator.py --num-runs 1 --cooldown 30
    python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --scenario central-flink
    python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --dry-run
"""
 
from __future__ import annotations
 
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional
 
# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/orchestrator.log'),
    ]
)
log = logging.getLogger(__name__)
 
# ─── Project Paths ────────────────────────────────────────────────────────────
def find_project_root() -> Path:
    """Cari project root yang memiliki direktori config/ dan docker-compose/.
    Naik dari lokasi script sampai menemukan keduanya.
    """
    current = Path(__file__).resolve().parent
    for _ in range(6):
        if (current / 'docker-compose').is_dir() and (current / 'config').is_dir():
            return current
        current = current.parent
    # fallback: assume parent of scripts
    return Path(__file__).resolve().parent.parent

PROJECT_ROOT  = find_project_root()
SCRIPTS_DIR   = PROJECT_ROOT / 'scripts'
CONFIG_DIR    = PROJECT_ROOT / 'config'
METRICS_DIR   = PROJECT_ROOT / 'metrics'
RESULTS_DIR   = PROJECT_ROOT / 'results'
COMPOSE_FLINK = PROJECT_ROOT / 'docker-compose' / 'docker-compose-flink.yml'
COMPOSE_SPARK = PROJECT_ROOT / 'docker-compose' / 'docker-compose-spark.yml'
 
METRICS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
 
# ─── Scenario Definitions ─────────────────────────────────────────────────────
@dataclass
class Scenario:
    """Definisi satu skenario eksperimen."""
    name: str                    # identifier CLI
    framework: str               # 'flink' atau 'spark'
    fault_config: str            # nama file di config/
    load_config: str             # nama file di config/
    duration: int                # detik
    output_csv: str              # nama file output di metrics/
    description: str
    compose_file: str            # nama docker-compose file
    requires_job_restart: bool = True
    extra_env: dict = field(default_factory=dict)
 
 
# Daftar semua skenario — tambah di sini untuk skenario baru
ALL_SCENARIOS: List[Scenario] = [
 
    # ── Multi-Fault Cascading (eksperimen utama) ──────────────────────────────
    Scenario(
        name='flink',
        framework='flink',
        fault_config='fault_no_network.yaml',
        load_config='load.yaml',
        duration=900,
        output_csv='flink_dynamic_load.csv',
        description='Flink — Dynamic Load + Multi-Fault Cascading',
        compose_file='docker-compose/docker-compose-flink.yml',
    ),
    Scenario(
        name='spark',
        framework='spark',
        fault_config='fault_no_network.yaml',
        load_config='load.yaml',
        duration=900,
        output_csv='spark_dynamic_load.csv',
        description='Spark — Dynamic Load + Multi-Fault Cascading',
        compose_file='docker-compose/docker-compose-spark.yml',
    ),
 
    # ── Single Node Failure ───────────────────────────────────────────────────
    Scenario(
        name='single-node-flink',
        framework='flink',
        fault_config='fault_single_node.yaml',
        load_config='load.yaml',
        duration=600,
        output_csv='flink_single_node_failure.csv',
        description='Flink — Single TaskManager Failure (no restart)',
        compose_file='docker-compose/docker-compose-flink.yml',
    ),
    Scenario(
        name='single-node-spark',
        framework='spark',
        fault_config='fault_single_node.yaml',
        load_config='load.yaml',
        duration=600,
        output_csv='spark_single_node_failure.csv',
        description='Spark — Single Worker Failure (no restart)',
        compose_file='docker-compose/docker-compose-spark.yml',
    ),
 
    # ── Central Component Failure ─────────────────────────────────────────────
    Scenario(
        name='central-flink',
        framework='flink',
        fault_config='fault_central_component.yaml',
        load_config='load.yaml',
        duration=600,
        output_csv='flink_central_component_failure.csv',
        description='Flink — JobManager Failure + Recovery',
        compose_file='docker-compose/docker-compose-flink.yml',
    ),
    Scenario(
        name='central-spark',
        framework='spark',
        fault_config='fault_central_component.yaml',
        load_config='load.yaml',
        duration=600,
        output_csv='spark_central_component_failure.csv',
        description='Spark — Master/Driver Failure + Recovery',
        compose_file='docker-compose/docker-compose-spark.yml',
    ),
]
 
SCENARIO_MAP = {s.name: s for s in ALL_SCENARIOS}
 
# ─── Helper Functions ─────────────────────────────────────────────────────────
 
def run_cmd(cmd: List[str], check: bool = True, timeout: int = 60,
            capture: bool = False) -> subprocess.CompletedProcess:
    """Jalankan shell command dengan logging."""
    log.info(f"CMD: {' '.join(str(c) for c in cmd)}")
    return subprocess.run(
        cmd, check=check, timeout=timeout,
        capture_output=capture, text=True
    )
 
 
def docker_compose_up(compose_file: Path, detach: bool = True) -> None:
    cmd = ['docker-compose', '-f', str(compose_file), 'up']
    if detach:
        cmd.append('-d')
    run_cmd(cmd, timeout=120)
    log.info(f"Cluster up: {compose_file.name}")
    time.sleep(20)  # tunggu container siap
 
 
def docker_compose_down(compose_file: Path) -> None:
    run_cmd(['docker-compose', '-f', str(compose_file), 'down', '--remove-orphans'],
            check=False, timeout=60)
    log.info(f"Cluster down: {compose_file.name}")
    time.sleep(5)


def stop_other_framework(current_framework: str) -> None:
    other = 'spark' if current_framework == 'flink' else 'flink'
    other_compose = PROJECT_ROOT / 'docker-compose' / f'docker-compose-{other}.yml'
    if other_compose.exists():
        subprocess.run(
            ['docker-compose', '-f', str(other_compose), 'down', '--remove-orphans'],
            check=False, timeout=60,
            capture_output=True,
            text=True,
        )
        log.info(f"Stopped {other} cluster (isolation cleanup)")


def resolve_compose_file(compose_file: str) -> Path:
    """Resolve compose file from the project root or docker-compose/ directory."""
    compose_path = PROJECT_ROOT / compose_file
    if compose_path.exists():
        return compose_path

    alt = PROJECT_ROOT / 'docker-compose' / compose_file
    if alt.exists():
        return alt

    found = next(PROJECT_ROOT.rglob(compose_file), None)
    if found is not None:
        return found

    raise FileNotFoundError(f"Compose file not found: {compose_file} (searched under {PROJECT_ROOT})")
 
 
def reset_kafka_topic(framework: str) -> None:
    """Hapus dan buat ulang Kafka topic untuk memastikan state bersih."""
    kafka_container = f'{framework}-kafka'
    topic = 'cic-topic'
 
    log.info(f"Resetting Kafka topic '{topic}' on {kafka_container}...")
    run_cmd([
        'docker', 'exec', kafka_container,
        'kafka-topics', '--bootstrap-server', 'localhost:29092',
        '--delete', '--topic', topic
    ], check=False, timeout=30)

    log.info("Waiting for topic deletion to complete...")
    for i in range(30):
        time.sleep(1)
        result = subprocess.run([
            'docker', 'exec', kafka_container,
            'kafka-topics', '--bootstrap-server', 'localhost:29092',
            '--list'
        ], capture_output=True, text=True, timeout=10)

        if topic not in result.stdout:
            log.info(f"Topic deleted after {i + 1}s")
            break

        if i == 29:
            log.warning("Topic deletion timeout — forcing via describe check")

    time.sleep(2)
 
    run_cmd([
        'docker', 'exec', kafka_container,
        'kafka-topics', '--bootstrap-server', 'localhost:29092',
        '--create', '--topic', topic,
        '--partitions', '2',
        '--replication-factor', '1'
    ], timeout=30)
    log.info(f"Kafka topic '{topic}' recreated with 2 partitions")
 
 
def deploy_flink_job() -> None:
    """Copy JAR dan submit Flink job."""
    jar_src = PROJECT_ROOT / 'flink-jobs' / 'target' / 'cic-flink-job.jar'
    if not jar_src.exists():
        log.warning(f"JAR not found at {jar_src}, trying build...")
        run_cmd(['bash', str(SCRIPTS_DIR / 'build_flink_job.sh')], timeout=300)
 
    run_cmd([
        'docker', 'cp', str(jar_src), 'flink-jobmanager:/tmp/cic-flink-job.jar'
    ], timeout=30)
 
    run_cmd([
        'docker', 'exec', 'flink-jobmanager',
        'flink', 'run', '-d',
        '/tmp/cic-flink-job.jar',
        'kafka:29092', 'cic-topic', 'flink-group'
    ], timeout=60)
    log.info("Flink job submitted")
    time.sleep(10)
 
 
def deploy_spark_job() -> None:
    """Copy JAR dan submit Spark job."""
    jar_src = PROJECT_ROOT / 'spark-jobs' / 'target' / 'cic-spark-job-1.0.0.jar'
    run_cmd([
        'docker', 'cp', str(jar_src), 'spark-driver:/tmp/cic-spark-job.jar'
    ], timeout=30)
 
    run_cmd([
        'docker', 'exec', 'spark-driver', 'bash', '-lc',
        'mkdir -p /tmp/spark-checkpoints /tmp/spark-query-checkpoints '
        '/tmp/spark-metrics-checkpoints /tmp/.ivy2/cache'
    ], timeout=30)
 
    submit_cmd = (
        'nohup env HOME=/tmp IVY_HOME=/tmp/.ivy2 '
        '/opt/spark/bin/spark-submit '
        '--master spark://spark-master:7077 '
        '--deploy-mode client '
        '--conf spark.jars.ivy=/tmp/.ivy2 '
        '--class thesis.stream.CicSparkStreamingJob '
        '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 '
        '/tmp/cic-spark-job.jar kafka:29092 cic-topic spark-group '
        '> /tmp/cic-spark-job.log 2>&1 & echo $! > /tmp/cic-spark-job.pid'
    )
    run_cmd(['docker', 'exec', 'spark-driver', 'bash', '-lc', submit_cmd],
            timeout=60)
    log.info("Spark job submitted")
    time.sleep(15)
 
 
def reset_checkpoints(framework: str) -> None:
    """Hapus checkpoint lama agar eksperimen mulai dari state bersih."""
    if framework == 'flink':
        run_cmd([
            'docker', 'exec', 'flink-jobmanager',
            'bash', '-c', 'rm -rf /tmp/flink-checkpoints/*'
        ], check=False, timeout=15)
    else:
        # Spark: hapus dari bind-mount host jika ada, atau dari container
        checkpoint_host = PROJECT_ROOT / 'spark-checkpoints'
        if checkpoint_host.exists():
            import shutil
            for item in checkpoint_host.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            log.info("Spark host checkpoints cleared")
        run_cmd([
            'docker', 'exec', 'spark-driver',
            'bash', '-c',
            'rm -rf /tmp/spark-checkpoints/* '
            '/tmp/spark-query-checkpoints/* '
            '/tmp/spark-metrics-checkpoints/*'
        ], check=False, timeout=15)
    log.info(f"Checkpoints reset for {framework}")
 
 
# ─── Run One Scenario ─────────────────────────────────────────────────────────
 
@dataclass
class RunResult:
    scenario_name: str
    run_index: int
    start_time: str
    end_time: str
    output_csv: str
    success: bool
    error: Optional[str] = None
    duration_actual: float = 0.0
 
 
def run_scenario(scenario: Scenario, run_index: int,
                 dry_run: bool = False) -> RunResult:
    """Jalankan satu skenario untuk satu run."""
 
    start_ts = datetime.now().isoformat()
    output_csv = METRICS_DIR / f"run{run_index:02d}_{scenario.output_csv}"
    log.info(f"{'[DRY RUN] ' if dry_run else ''}Starting: {scenario.description} (run {run_index})")
    compose_path = resolve_compose_file(scenario.compose_file)
    log.info(f"Using compose file: {compose_path}")
 
    if dry_run:
        log.info(f"  compose : {compose_path}")
        log.info(f"  fault   : {scenario.fault_config}")
        log.info(f"  load    : {scenario.load_config}")
        log.info(f"  output  : {output_csv}")
        log.info(f"  duration: {scenario.duration}s")
        return RunResult(
            scenario_name=scenario.name,
            run_index=run_index,
            start_time=start_ts,
            end_time=datetime.now().isoformat(),
            output_csv=str(output_csv),
            success=True,
        )
 
    t0 = time.time()
    try:
        # 1. Pastikan cluster yang benar running
        log.info("Step 1: Ensuring correct cluster is running...")
        stop_other_framework(scenario.framework)
        docker_compose_up(compose_path)
 
        # 2. Reset state
        log.info("Step 2: Resetting experiment state...")
        reset_checkpoints(scenario.framework)
        reset_kafka_topic(scenario.framework)
 
        # 3. Deploy job
        log.info("Step 3: Deploying streaming job...")
        if scenario.framework == 'flink':
            deploy_flink_job()
        else:
            deploy_spark_job()
 
        # 4. Start producer (background)
        log.info("Step 4: Starting producer with dynamic load config...")
        producer_proc = subprocess.Popen([
            sys.executable,
            str(SCRIPTS_DIR / 'producer.py'),
            '--broker', 'localhost:9092',
            '--topic', 'cic-topic',
            '--config', str(CONFIG_DIR / scenario.load_config),
            '--duration', str(scenario.duration),
        ])
 
        # 5. Start metrics collector (background)
        log.info("Step 5: Starting metrics collector...")
        collector_proc = subprocess.Popen([
            sys.executable,
            str(SCRIPTS_DIR / 'metrics_collector.py'),
            '--prometheus', 'http://localhost:9090',
            '--output', str(output_csv),
            '--duration', str(scenario.duration),
            '--interval', '1',
            '--load-config', str(CONFIG_DIR / scenario.load_config),
        ])
 
        # 6. Start fault injection (background, dengan delay 5 detik)
        log.info("Step 6: Starting fault injection...")
        time.sleep(5)
        fault_proc = subprocess.Popen([
            'bash',
            str(SCRIPTS_DIR / 'fault_injection.sh'),
            scenario.framework,
            str(CONFIG_DIR / scenario.fault_config),
        ], env={**os.environ,
                'FLINK_JOB_UI': 'http://localhost:8083',
                'FLINK_JOB_CONTAINER': 'flink-jobmanager',
                'FLINK_JOB_JAR': '/tmp/cic-flink-job.jar',
                'FLINK_KAFKA_BOOTSTRAP': 'kafka:29092',
                'FLINK_KAFKA_TOPIC': 'cic-topic',
                'FLINK_KAFKA_GROUP': 'flink-group',
                **scenario.extra_env})
 
        # 7. Tunggu producer selesai (ini yang menentukan durasi eksperimen)
        log.info(f"Step 7: Running experiment for {scenario.duration}s...")
        producer_proc.wait(timeout=scenario.duration + 300)
 
        # 8. Bersihkan proses background
        try:
            collector_proc.wait(timeout=60)
            log.info("Metrics collector selesai dan CSV tersimpan")
        except subprocess.TimeoutExpired:
            log.warning("Collector timeout — force terminate")
            collector_proc.terminate()
            collector_proc.wait(timeout=5)

        # Baru terminate fault injection
        fault_proc.terminate()
        try:
            fault_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            fault_proc.kill()
 
        duration_actual = time.time() - t0
        log.info(f"Scenario complete in {duration_actual:.1f}s — CSV: {output_csv}")
 
        return RunResult(
            scenario_name=scenario.name,
            run_index=run_index,
            start_time=start_ts,
            end_time=datetime.now().isoformat(),
            output_csv=str(output_csv),
            success=True,
            duration_actual=duration_actual,
        )
 
    except Exception as e:
        log.error(f"Scenario FAILED: {e}", exc_info=True)
        return RunResult(
            scenario_name=scenario.name,
            run_index=run_index,
            start_time=start_ts,
            end_time=datetime.now().isoformat(),
            output_csv=str(output_csv),
            success=False,
            error=str(e),
            duration_actual=time.time() - t0,
        )


def preflight_check(dry_run: bool = False) -> bool:
    """Verifikasi semua dependensi sebelum eksperimen dimulai."""
    if dry_run:
        log.info("Preflight: dry-run mode, skipping checks")
        return True

    checks = []
    errors = []

    # 1. Docker running
    try:
        result = subprocess.run(['docker', 'info'], capture_output=True, timeout=10)
        ok = result.returncode == 0
    except Exception:
        ok = False
    checks.append(('Docker daemon', ok))
    if not ok:
        errors.append("Docker tidak running. Jalankan: open -a Docker")

    # 2. Config files
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

    # 4. JARs
    jar = PROJECT_ROOT / 'flink-jobs' / 'target' / 'cic-flink-job.jar'
    ok = jar.exists()
    checks.append(('Flink JAR', ok))
    if not ok:
        errors.append(f"Flink JAR tidak ditemukan: {jar}. Jalankan build_flink_job.sh")

    jar = PROJECT_ROOT / 'spark-jobs' / 'target' / 'cic-spark-job-1.0.0.jar'
    ok = jar.exists()
    checks.append(('Spark JAR', ok))
    if not ok:
        errors.append(f"Spark JAR tidak ditemukan: {jar}")

    # 5. Dataset parquet
    dataset = PROJECT_ROOT / 'cic-collection.parquet'
    ok = dataset.exists()
    checks.append(('Dataset parquet', ok))
    if not ok:
        errors.append(f"Dataset tidak ditemukan: {dataset}")

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
 
 
# ─── Main ─────────────────────────────────────────────────────────────────────
 
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Experiment Orchestrator — Tesis Fault Recovery Flink vs Spark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh penggunaan:
  # Jalankan semua skenario, 1 run, cooldown 30 detik
  python3 experiment_orchestrator.py --num-runs 1 --cooldown 30
 
  # Jalankan satu skenario spesifik
  python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --scenario central-flink
 
  # Dry run — lihat apa yang akan dijalankan tanpa eksekusi
  python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --dry-run
 
  # Jalankan skenario multi-fault saja (eksperimen utama)
  python3 experiment_orchestrator.py --num-runs 1 --cooldown 30 --scenario flink --scenario spark
 
Skenario yang tersedia:
  flink               Flink dynamic load + multi-fault cascading (900s)
  spark               Spark dynamic load + multi-fault cascading (900s)
  single-node-flink   Flink single TaskManager failure, no restart (600s)
  single-node-spark   Spark single Worker failure, no restart (600s)
  central-flink       Flink JobManager failure + recovery (600s)
  central-spark       Spark Master/Driver failure + recovery (600s)
        """
    )
    parser.add_argument(
        '--num-runs', type=int, default=1,
        help='Jumlah run per skenario (default: 1)'
    )
    parser.add_argument(
        '--cooldown', type=int, default=30,
        help='Jeda antar run dalam detik (default: 30)'
    )
    parser.add_argument(
        '--scenario', action='append', dest='scenarios',
        choices=list(SCENARIO_MAP.keys()),
        help='Skenario yang dijalankan (bisa multiple). Default: semua skenario.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Tampilkan apa yang akan dijalankan tanpa eksekusi'
    )
    parser.add_argument(
        '--skip-cluster-down', action='store_true',
        help='Jangan matikan cluster antar skenario (lebih cepat, kurang isolasi)'
    )
    parser.add_argument(
        '--output-summary', type=str,
        default=str(RESULTS_DIR / 'orchestrator_summary.json'),
        help='Path file JSON summary hasil semua run'
    )
    return parser.parse_args()
 
 
def main() -> None:
    args = parse_args()
 
    # Tentukan skenario yang akan dijalankan
    selected_names = args.scenarios or list(SCENARIO_MAP.keys())
    selected = [SCENARIO_MAP[name] for name in selected_names]
 
    log.info("=" * 60)
    log.info("EXPERIMENT ORCHESTRATOR — TESIS RIZKY ZEIN ADAM")
    log.info("=" * 60)
    log.info(f"Scenarios  : {[s.name for s in selected]}")
    log.info(f"Num runs   : {args.num_runs}")
    log.info(f"Cooldown   : {args.cooldown}s")
    log.info(f"Dry run    : {args.dry_run}")
    log.info("=" * 60)
 
    # Preflight checks
    ok = preflight_check(dry_run=args.dry_run)
    if not ok:
        log.error("Preflight gagal — hentikan eksekusi")
        sys.exit(2)

    # Bangun jadwal run berurutan agar estimasi waktu bisa dihitung
    run_schedule: List[tuple] = []  # (scenario, run_idx, duration)
    for scenario in selected:
        for run_idx in range(1, args.num_runs + 1):
            run_schedule.append((scenario, run_idx, scenario.duration))

    all_results: List[RunResult] = []
    total = len(run_schedule)

    for idx, (scenario, run_idx, duration) in enumerate(run_schedule, start=1):
        log.info(f"\n[{idx}/{total}] {scenario.description} — Run {run_idx}")

        result = run_scenario(scenario, run_idx, dry_run=args.dry_run)
        all_results.append(result)

        if result.success:
            log.info(f"  SUKSES — {result.duration_actual:.0f}s — {result.output_csv}")
        else:
            log.error(f"  GAGAL  — {result.error}")

        # Estimasi sisa waktu
        remaining_runs = total - idx
        remaining_duration = sum(d for (_s, _r, d) in run_schedule[idx:])
        total_remaining = remaining_duration + max(0, remaining_runs) * args.cooldown
        hours, rem = divmod(int(total_remaining), 3600)
        mins = rem // 60
        if remaining_runs > 0:
            log.info(f"  Estimasi sisa waktu: {hours}j {mins}m for {remaining_runs} runs")

        # Cooldown antar run (kecuali run terakhir)
        if idx < total and not args.dry_run:
            log.info(f"Cooldown {args.cooldown}s sebelum run berikutnya...")
            time.sleep(args.cooldown)

        # Shutdown cluster antar skenario untuk isolasi penuh
        if not args.skip_cluster_down and not args.dry_run:
            compose_path = PROJECT_ROOT / scenario.compose_file
            docker_compose_down(compose_path)
 
    # Simpan summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_runs': total,
        'successful': sum(1 for r in all_results if r.success),
        'failed': sum(1 for r in all_results if not r.success),
        'results': [asdict(r) for r in all_results],
    }
    summary_path = Path(args.output_summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2))
 
    log.info("\n" + "=" * 60)
    log.info(f"SELESAI: {summary['successful']}/{total} sukses")
    log.info(f"Summary : {summary_path}")
    log.info("=" * 60)
 
    # Exit code 1 jika ada yang gagal
    if summary['failed'] > 0:
        sys.exit(1)
 
 
if __name__ == '__main__':
    main()
 