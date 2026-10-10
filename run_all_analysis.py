#!/usr/bin/env python3
"""Unified Master Runner untuk Post-Processing, Analisis, dan Gambar Tesis/Paper.
================================================================================
Menyatukan alur eksekusi dari 4 komponen acuan utama:
  1. analysis_recovery_metrics.py  -> Verifikasi metrik Tabel 1 & 2 naskah (Kanonik)
  2. make_fig1_scenarios.py        -> Figure 1 (Diagram skenario arsitektur, 300 DPI)
  3. make_figs_print.py            -> Figure 2, 3, 4 (Grafik cetak naskah, 300 DPI)
  4. scripts/generate_charts_bab4_3x.py -> Gambar 4.1 - 4.14 Bab 4 (3x replikasi)
  5. generate_report.py & generate_publication_figures.py -> Laporan & figur publikasi

Cara pakai:
  python3 run_all_analysis.py          # Jalankan SEMUA pipeline
  python3 run_all_analysis.py --check  # Cek kecocokan metrik dengan Tabel naskah saja
  python3 run_all_analysis.py --print  # Buat Figure 1-4 format cetak naskah saja
  python3 run_all_analysis.py --bab4   # Buat Gambar Bab 4 (4.1 - 4.14) saja
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def print_banner(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def find_script(name: str) -> Path:
    candidates = [
        PROJECT_ROOT / "scripts" / name,
        PROJECT_ROOT / name,
    ]
    for c in candidates:
        if c.is_file():
            return c
    raise FileNotFoundError(f"Skrip {name} tidak ditemukan di scripts/ maupun root.")


def step_check_metrics(data_dir: str | None = None) -> bool:
    print_banner("1. VERIFIKASI METRIK KANONIK (Tabel 1 & 2 Naskah)")
    script = find_script("analysis_recovery_metrics.py")
    cmd = [sys.executable, str(script), "--check"]
    if data_dir:
        cmd.extend(["--data-dir", data_dir])

    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return res.returncode == 0


def step_make_figs_print(out_dir: str | None = None) -> bool:
    print_banner("2. GENERASI FIGURE 1 - 4 FORMAT CETAK NASKAH (300 DPI)")
    target_out = out_dir or str(PROJECT_ROOT / "results" / "figs_print")
    os.makedirs(target_out, exist_ok=True)
    env = os.environ.copy()
    env["FIGS_PRINT_DIR"] = target_out

    # Fig 1
    script_fig1 = find_script("make_fig1_scenarios.py")
    print(">> Menjalankan make_fig1_scenarios.py ...")
    r1 = subprocess.run([sys.executable, str(script_fig1)], cwd=str(PROJECT_ROOT), env=env)

    # Fig 2, 3, 4
    script_figs = find_script("make_figs_print.py")
    print(">> Menjalankan make_figs_print.py ...")
    r2 = subprocess.run([sys.executable, str(script_figs)], cwd=str(PROJECT_ROOT), env=env)

    print(f">> Gambar cetak selesai disimpan di: {target_out}")
    return r1.returncode == 0 and r2.returncode == 0


def step_generate_charts_bab4(results_dir: str | None = None, out_dir: str | None = None) -> bool:
    print_banner("3. GENERASI 14 GAMBAR BAB 4 (3 SKENARIO × 3 REPLIKASI)")
    script = find_script("generate_charts_bab4_3x.py")
    res_dir = results_dir or str(PROJECT_ROOT / "results")
    output_dir = out_dir or str(PROJECT_ROOT / "output_charts_bab4")

    cmd = [
        sys.executable,
        str(script),
        "--results-dir",
        res_dir,
        "--out-dir",
        output_dir,
    ]
    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return res.returncode == 0


def step_generate_reports_and_pub(results_dir: str | None = None) -> bool:
    print_banner("4. GENERASI LAPORAN KOMPARATIF & PUBLICATION FIGURES")
    res_dir = results_dir or str(PROJECT_ROOT / "results")
    flink_metrics = PROJECT_ROOT / "metrics" / "flink_metrics.csv"
    spark_metrics = PROJECT_ROOT / "metrics" / "spark_metrics.csv"

    # Report generator jika metrics tersedia
    if flink_metrics.is_file() and spark_metrics.is_file():
        print(">> Menjalankan generate_report.py...")
        rep_script = find_script("generate_report.py")
        subprocess.run(
            [
                sys.executable,
                str(rep_script),
                "--flink-metrics",
                str(flink_metrics),
                "--spark-metrics",
                str(spark_metrics),
                "--flink-report",
                str(Path(res_dir) / "flink_results.md"),
                "--spark-report",
                str(Path(res_dir) / "spark_results.md"),
                "--comparison-report",
                str(Path(res_dir) / "comparison.md"),
            ],
            cwd=str(PROJECT_ROOT),
        )

    # Publication figures
    pub_script = find_script("generate_publication_figures.py")
    if pub_script.is_file():
        print(">> Menjalankan generate_publication_figures.py...")
        subprocess.run(
            [
                sys.executable,
                str(pub_script),
                "--project-root",
                str(PROJECT_ROOT),
                "--results-dir",
                res_dir,
            ],
            cwd=str(PROJECT_ROOT),
        )

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Unified Post-Processing Runner untuk Analisis & Visualisasi Tesis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--data-dir", default=None, help="Folder berisi CSV eksperimen (default: results/)")
    parser.add_argument("--check", action="store_true", help="Hanya verifikasi metrik terhadap Tabel 1 & 2 naskah")
    parser.add_argument("--print", action="store_true", help="Hanya regenerasi Figure 1-4 format cetak naskah (300 DPI)")
    parser.add_argument("--bab4", action="store_true", help="Hanya regenerasi Gambar Bab 4 (4.1 - 4.14)")
    parser.add_argument("--report", action="store_true", help="Hanya regenerasi laporan komparatif dan publikasi")
    parser.add_argument("--all", action="store_true", help="Jalankan semua tahapan (default jika tanpa argumen)")
    args = parser.parse_args()

    # Default ke --all jika tidak ada flag spesifik
    run_all = args.all or not (args.check or getattr(args, "print") or args.bab4 or args.report)

    success = True
    if run_all or args.check:
        success = step_check_metrics(args.data_dir) and success

    if run_all or getattr(args, "print"):
        success = step_make_figs_print() and success

    if run_all or args.bab4:
        success = step_generate_charts_bab4(args.data_dir) and success

    if run_all or args.report:
        success = step_generate_reports_and_pub(args.data_dir) and success

    print_banner("SELESAI")
    if success:
        print(">> Seluruh proses selesai dengan sukses dan sesuai dengan acuan paper!")
    else:
        print(">> Terdapat tahapan yang mengalami kendala. Periksa log di atas.")


if __name__ == "__main__":
    main()
