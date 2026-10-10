#!/usr/bin/env python3
"""Generate Flink/Spark markdown reports and experiment summary from metrics CSV files.

PENTING (perbaikan 10 Okt 2026)
-------------------------------
File ini DULU punya salinan definisi metrik sendiri (compute_recovery_interval,
compute_throughput_deficit). Salinan itu berbeda dari definisi naskah dan menghasilkan
angka yang salah:

  * "stabilization interval" selalu 0,00 s (fungsi tidak pernah menemukan jendela stabil),
  * baseline diambil dari rata-rata 10 sampel pertama (fase warm-up), bukan 60 s pra-fault,
  * T2 dicari mulai dari T1, bukan dari akhir jendela fault aktif, sehingga komponen yang
    masih mati bisa terlihat "sudah pulih",
  * jendela pelapor-mati (t = 120-180 s di skenario central) tidak ditolkan, sehingga
    deficit Spark keluar 15,14% alih-alih 19,35% seperti naskah,
  * tidak ada penjaga celah koleksi (dt > 30 s diekstrapolasi).

Sekarang metrik TIDAK didefinisikan di sini. Semuanya mendelegasi ke SATU implementasi
kanonik di analysis_recovery_metrics.py. Kalau file kanonik tidak ketemu, skrip ini
BERHENTI -- sengaja tanpa salinan cadangan, karena salinan cadangan itulah yang dulu
membuat angka laporan melenceng dari Tabel 1 naskah.

Pakai:
    python generate_report.py --flink-metrics flink_central_rep1.csv \
        --spark-metrics spark_central_rep1.csv --fault-time 120 \
        --flink-report out/flink.md --spark-report out/spark.md \
        --comparison-report out/comparison.md
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional

import pandas as pd


NUMERIC_COLS = [
    "elapsed_seconds",
    "load_rate",
    "throughput",
    "latency",
    "cpu",
    "memory",
    "kafka_lag",
    "recovery_time",
]


def load_metrics(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing metrics file: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Metrics file is empty: {path}")

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# --------------------------------------------------------------------- implementasi kanonik
def load_canonical():
    """Impor analysis_recovery_metrics.py sebagai satu-satunya definisi metrik."""
    here = Path(__file__).resolve().parent
    candidates = [
        os.environ.get("RECOVERY_METRICS_PY"),
        here / "analysis_recovery_metrics.py",
        here / "scripts" / "analysis_recovery_metrics.py",
        here.parent / "analysis_recovery_metrics.py",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            spec = importlib.util.spec_from_file_location("analysis_recovery_metrics", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    sys.exit(
        "ERROR: analysis_recovery_metrics.py tidak ditemukan.\n"
        "Taruh file itu di folder yang sama, atau set RECOVERY_METRICS_PY ke lokasinya.\n"
        "Sengaja tidak ada salinan cadangan: angka laporan harus datang dari satu sumber."
    )


def _rows_from_df(df: pd.DataFrame) -> List[Dict[str, object]]:
    """Ubah DataFrame menjadi bentuk baris yang dipakai modul kanonik."""
    rows: List[Dict[str, object]] = []
    for record in df.to_dict("records"):
        try:
            t = float(record["elapsed_seconds"])
            load = float(record["load_rate"])
            tp = float(record["throughput"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (math.isfinite(t) and math.isfinite(load) and math.isfinite(tp)):
            continue
        mem: Optional[float]
        try:
            mem = float(record["memory"])
            if not math.isfinite(mem):
                mem = None
        except (KeyError, TypeError, ValueError):
            mem = None
        rows.append({"t": t, "load": load, "tp": tp, "mem": mem})
    rows.sort(key=lambda item: item["t"])
    return rows


def read_fault_start(default: float = 300.0) -> float:
    """Waktu injeksi dari config/fault.yaml. Dipakai kalau --fault-time tidak diberikan."""
    fault_yaml = Path(__file__).resolve().parent.parent / "config" / "fault.yaml"
    if not fault_yaml.exists():
        return default

    try:
        import yaml

        content = yaml.safe_load(fault_yaml.read_text(encoding="utf-8")) or {}
        faults = content.get("cascading_faults", [])
        if faults and isinstance(faults[0], dict):
            return float(faults[0].get("time", default))
    except Exception as exc:  # noqa: BLE001 - jangan gagal senyap
        print(f"warning: gagal membaca {fault_yaml} ({exc}); memakai default {default} s",
              file=sys.stderr)

    return default


def compute_metrics(df: pd.DataFrame, fault_time: float, t2_min: Optional[float],
                    canonical) -> Dict[str, Dict[str, float]]:
    rows = canonical.dedup(_rows_from_df(df))
    if not rows:
        raise ValueError("Tidak ada baris valid (elapsed_seconds/load_rate/throughput).")

    # (i) jendela pelapor-mati dari footprint memori, dibatasi jadwal injeksi.
    #     Ini yang dipakai untuk MENOLKAN throughput di deficit -- sama seperti skrip kanonik.
    win = canonical.unmeasurable_window(rows, fault_time)

    # (ii) semua metrik -- dari implementasi kanonik, tidak ada salinan lokal
    breakdown = canonical.recovery_breakdown(rows, fault_time, t2_min)
    deficit_pct = canonical.deficit(rows, win)
    backlog_run = canonical.peak_backlog_rate(rows, win)
    backlog_fault = canonical.peak_backlog_rate(rows, win or None, fault_time, fault_time + 60.0)
    peak_mem = canonical.peak_memory(rows)

    # (iii) statistik deskriptif yang tidak ada di kanonik
    mem_values = [row["mem"] for row in rows if row["mem"] is not None]
    latency_values = ([float(v) for v in df["latency"].dropna()] if "latency" in df.columns else [])
    latency_nonzero = [v for v in latency_values if v > 0]
    cpu_values = ([float(v) for v in df["cpu"].dropna()] if "cpu" in df.columns else [])
    lag_values = ([float(v) for v in df["kafka_lag"].dropna()] if "kafka_lag" in df.columns else [])

    tp_values = [row["tp"] for row in rows]
    total_in = sum(row["load"] for row in rows)
    total_out = sum(tp_values)

    # (iv) neraca event hanyalah PERKIRAAN dari kolom throughput, bukan pengukuran kehilangan
    missing_events = max(total_in - total_out, 0.0)
    balance_pct = (missing_events / total_in * 100.0) if total_in > 0 else 0.0

    return {
        "Throughput": {"average": mean(tp_values), "peak": max(tp_values)},
        "Latency": {
            "average": mean(latency_nonzero) if latency_nonzero else 0.0,
            "peak": max(latency_values) if latency_values else 0.0,
        },
        "Backpressure": {
            "max_lag_raw": max(lag_values) if lag_values else 0.0,
            "peak_backlog_run": backlog_run,
            "peak_backlog_fault_window": backlog_fault,
        },
        "Resource": {
            "cpu_avg": mean(cpu_values) if cpu_values else 0.0,
            "mem_avg_mb": mean(mem_values) if mem_values else 0.0,
            "mem_peak_mb": peak_mem if peak_mem is not None else 0.0,
        },
        "Recovery": {
            "fault_time": fault_time,
            "t1": breakdown["t1"],
            "t2": breakdown["t2"],
            "interval": breakdown["interval"],
            "unmeasurable_window": list(win) if win else None,
        },
        "Deficit": {"throughput_deficit_pct": deficit_pct},
        "DataLoss": {
            "loss_percentage": balance_pct,
            "total_input_events": int(round(total_in)),
            "total_output_events": int(round(total_out)),
            "missing_events": int(round(missing_events)),
        },
    }


def write_engine_report(engine: str, metrics: Dict[str, Dict[str, float]], output: Path) -> None:
    recovery = metrics["Recovery"]
    interval = recovery["interval"]
    interval_text = "tidak tercapai" if interval is None else "%.2f" % interval
    t2_text = "tidak tercapai" if recovery["t2"] is None else "%.1f" % recovery["t2"]
    window = recovery["unmeasurable_window"]
    window_text = "tidak terdeteksi" if window is None else "[%.1f, %.1f] s" % (window[0], window[1])

    lines = [
        "# %s Experiment Report" % engine,
        "",
        "## Core Metrics",
        "",
        "- Throughput average: %.2f msg/s" % metrics["Throughput"]["average"],
        "- Throughput peak: %.2f msg/s" % metrics["Throughput"]["peak"],
        "- Latency average (non-zero samples): %.2f ms" % metrics["Latency"]["average"],
        "- Latency peak (whole run, includes warm-up): %.2f ms" % metrics["Latency"]["peak"],
        "- CPU average: %.2f%%" % metrics["Resource"]["cpu_avg"],
        "- Memory average: %.2f MB" % metrics["Resource"]["mem_avg_mb"],
        "- Memory peak: %.2f MB" % metrics["Resource"]["mem_peak_mb"],
        "- Peak backlog (whole run): %.2f msg/s" % metrics["Backpressure"]["peak_backlog_run"],
        "- Peak backlog (fault window): %.2f msg/s" % metrics["Backpressure"]["peak_backlog_fault_window"],
        "- Raw kafka_lag column maximum: %.2f (collector column, NOT validated as backlog)"
        % metrics["Backpressure"]["max_lag_raw"],
        "",
        "## Recovery",
        "",
        "- Fault injected at: %.1f s" % recovery["fault_time"],
        "- Reporter-down window: %s" % window_text,
        "- T1: %.1f s" % recovery["t1"],
        "- T2: %s" % t2_text,
        "- Stabilization interval (T2 - T1): %s s" % interval_text,
        "- Throughput deficit (whole run): %.2f%%" % metrics["Deficit"]["throughput_deficit_pct"],
        "",
        "## Event Balance (estimate from the throughput column, NOT a loss measurement)",
        "",
        "- Input events (integral of load_rate): %d" % metrics["DataLoss"]["total_input_events"],
        "- Output events (integral of throughput): %d" % metrics["DataLoss"]["total_output_events"],
        "- Difference: %d (%.2f%%)" % (metrics["DataLoss"]["missing_events"],
                                      metrics["DataLoss"]["loss_percentage"]),
        "- Note: the throughput deficit already contains a supply-side component and the",
        "  first-60-s warm-up, so this difference is NOT a data-loss claim.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_comparison(analysis: Dict[str, Dict[str, Dict[str, float]]]) -> List[Dict[str, object]]:
    metrics_to_compare = [
        ("Throughput (Avg)", "Throughput", "average"),
        ("Throughput (Peak)", "Throughput", "peak"),
        ("Stabilization Interval (s)", "Recovery", "interval"),
        ("Throughput Deficit (%)", "Deficit", "throughput_deficit_pct"),
        ("Peak Backlog (msg/s)", "Backpressure", "peak_backlog_run"),
        ("Peak Memory (MB)", "Resource", "mem_peak_mb"),
        ("CPU Usage (Avg, %)", "Resource", "cpu_avg"),
    ]

    rows: List[Dict[str, object]] = []
    for name, category, key in metrics_to_compare:
        flink_value = analysis["Flink"][category].get(key)
        spark_value = analysis["Spark"][category].get(key)
        rows.append({
            "Metric": name,
            "Flink": "" if flink_value is None else round(float(flink_value), 2),
            "Spark": "" if spark_value is None else round(float(spark_value), 2),
        })
    return rows


def write_comparison_report(comparison: List[Dict[str, object]], output: Path) -> None:
    lines = [
        "# Flink and Spark Comparison",
        "",
        "| Metric | Flink | Spark |",
        "|---|---:|---:|",
    ]
    for row in comparison:
        lines.append("| %s | %s | %s |" % (row["Metric"], row["Flink"], row["Spark"]))
    lines.append("")
    lines.append("Descriptive only. Resource-constrained single-node setup, n = 3 per group, so the")
    lines.append("numbers are reported as consistency evidence rather than a significance test.")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate comparison reports from experiment metrics")
    parser.add_argument("--flink-metrics", required=True)
    parser.add_argument("--spark-metrics", required=True)
    parser.add_argument("--flink-report", required=True)
    parser.add_argument("--spark-report", required=True)
    parser.add_argument("--comparison-report", required=True)
    parser.add_argument("--fault-time", type=float, default=None,
                        help="fault injection time in seconds (default: config/fault.yaml)")
    parser.add_argument("--scenario", choices=["single", "central", "multi"], default=None,
                        help="skenario dari tabel kanonik; menentukan T2 minimum "
                             "(central: T2 dihitung dari t=180 s, seperti aturan jadwal di naskah)")
    args = parser.parse_args()

    canonical = load_canonical()

    t2_min: Optional[float] = None
    if args.scenario:
        default_fault, t2_min = canonical.SCENARIOS[args.scenario]
        fault_time = default_fault if args.fault_time is None else float(args.fault_time)
    elif args.fault_time is not None:
        fault_time = float(args.fault_time)
    else:
        fault_time = read_fault_start()
        print("fault time from config/fault.yaml: %.1f s" % fault_time, file=sys.stderr)

    flink_df = load_metrics(Path(args.flink_metrics))
    spark_df = load_metrics(Path(args.spark_metrics))

    analysis = {
        "Flink": compute_metrics(flink_df, fault_time, t2_min, canonical),
        "Spark": compute_metrics(spark_df, fault_time, t2_min, canonical),
    }
    comparison = build_comparison(analysis)

    write_engine_report("Flink", analysis["Flink"], Path(args.flink_report))
    write_engine_report("Spark", analysis["Spark"], Path(args.spark_report))
    write_comparison_report(comparison, Path(args.comparison_report))

    summary_path = Path(args.comparison_report).parent / "experiment_summary.json"
    summary_payload = {
        "timestamp": datetime.now().isoformat(),
        "fault_time": fault_time,
        "analysis": analysis,
        "comparison": comparison,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    print("Saved %s" % args.flink_report)
    print("Saved %s" % args.spark_report)
    print("Saved %s" % args.comparison_report)
    print("Saved %s" % summary_path)


if __name__ == "__main__":
    main()
