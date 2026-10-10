#!/usr/bin/env python3
"""
Generator Gambar Bab 4 — Rizky Zein Adam
=========================================
Evaluasi Fault Recovery: Apache Flink vs Apache Spark Structured Streaming
Universitas Nasional, 2026

Data: 3 skenario (multi-fault, single-node, central) x 3 replikasi x 2 framework.
Semua angka (recovery, deficit, mean ± SD) DIHITUNG dari CSV — tidak ada hardcode.

Cara pakai:
  python generate_charts_bab4_3x.py [--results-dir DIR] [--data-dir DIR] [--out-dir DIR]

  --results-dir : folder RESULTS yang berisi CSV hasil run_*_3x.sh
                  (default: ./results) — pola nama: {fw}_{scn}_rep{N}.csv
                  mis. flink_multi_rep1.csv, spark_central_rep3.csv
  --data-dir    : fallback — folder berisi subfolder multi_3x/, single_3x/, central_3x/
                  (dipakai kalau CSV tidak ditemukan di results-dir)
  --out-dir     : folder output PNG (default: ./output_charts_bab4/)

Struktur file yang dicari (urutan prioritas):
  1. {results_dir}/{fw}_{scn}_rep{1,2,3}.csv
  2. {data_dir}/{scn}_3x/{fw}_{scn}_rep{1,2,3}.csv
  3. {data_dir}/{fw}_{scn}_rep{1,2,3}.csv
  scn: multi | single | central
  fw : flink | spark

Output: 14 gambar
  4.1  Throughput — Multi-Fault          (2 panel Flink/Spark, 3 rep overlay + mean±SD)
  4.2  Latency — Multi-Fault
  4.3  CPU & Memory — Multi-Fault        (2x2 panel)
  4.4  Backlog Proxy — Multi-Fault
  4.5  Throughput — Single-Node
  4.6  Latency — Single-Node
  4.7  CPU & Memory — Single-Node
  4.8  Backlog Proxy — Single-Node
  4.9  Throughput — Central
  4.10 Latency — Central
  4.11 CPU & Memory — Central
  4.12 Backlog Proxy — Central
  4.13 Recovery Time (mean ± SD) per skenario
  4.14 Throughput Deficit (mean ± SD) per skenario

Prinsip (sesuai kesepakatan 18-19 Agu):
  - backlog proxy = max(load - throughput, 0) — BUKAN kafka_lag (artifact restart Flink)
  - memory dalam MiB (bukan %), ylim dinamis
  - T1 fallback ke fault_time kalau <50% baseline tidak terdeteksi (graceful degradation)
  - missing CSV di-skip (gambar tetap jalan)
"""

import argparse
import csv
import math
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# KONFIGURASI
# ============================================================
SCENARIOS = {
    'multi': {
        'label': 'Multi-Fault Cascading',
        'fault_time': 330.0,
        'restart_time': 660.0,
        'fault_lines': [300.0, 330.0],
        'fault_labels': ['Switchover\nWorker2 (t=300)', 'Kill\nWorker1 (t=330)'],
        'restart_label': 'Restart\nWorker1 (t=660)',
        'load_label': 'Dynamic Load 100-1000 msg/s',
    },
    'single': {
        'label': 'Single-Node Failure',
        'fault_time': 120.0,
        'restart_time': None,
        'fault_lines': [120.0],
        'fault_labels': ['Kill\nWorker (t=120)'],
        'restart_label': None,
        'load_label': 'Load Ramp 100-1000 msg/s',
    },
    'central': {
        'label': 'Central Component Failure',
        'fault_time': 120.0,
        'restart_time': 180.0,
        'fault_lines': [120.0],
        'fault_labels': ['Kill\nJobManager/\nMaster (t=120)'],
        'restart_label': 'Restart\n(t=180)',
        'load_label': 'Load Ramp 100-1000 msg/s',
    },
}

FRAMEWORKS = {
    'flink': {'label': 'Apache Flink', 'color': '#D62728'},
    'spark': {'label': 'Apache Spark Structured Streaming', 'color': '#FF7F0E'},
}

REPS = [1, 2, 3]
LOAD_COLOR = '#999999'
FAULT_COLOR = '#CC0000'
RESTART_COLOR = '#0055AA'
MEAN_COLOR = '#1F77B4'
STD_ALPHA = 0.18

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 11,
    'axes.titlesize': 12,
    'axes.labelsize': 11,
    'legend.fontsize': 9,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})


# ============================================================
# LOAD DATA
# ============================================================
def load_csv(path):
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            try:
                rows.append({
                    't': float(r['elapsed_seconds']),
                    'load': float(r['load_rate']),
                    'tp': float(r['throughput']),
                    'lat': float(r['latency']),
                    'cpu': float(r['cpu']),
                    'mem': float(r['memory']),
                })
            except (ValueError, KeyError):
                continue
    rows.sort(key=lambda x: x['t'])
    return rows


def find_csv(results_dir, data_dir, scn, fw, rep):
    """Cari CSV: prioritas 1 = folder results/ (pola run_*_3x.sh),
    lalu fallback ke struktur scn_3x/ dan flat."""
    fname = f'{fw}_{scn}_rep{rep}.csv'
    for cand in (f'{results_dir}/{fname}',
                 f'{data_dir}/{scn}_3x/{fname}',
                 f'{data_dir}/{fname}',
                 f'{data_dir}/{scn}_3x/{fname.upper()}'):
        if os.path.exists(cand):
            return cand
    return None


def load_all(results_dir, data_dir):
    """Return data[scn][fw] = {'rep1': rows, 'rep2': ..., 'rep3': ...} (None jika missing)."""
    data = {}
    for scn in SCENARIOS:
        data[scn] = {}
        for fw in FRAMEWORKS:
            reps = {}
            for rep in REPS:
                path = find_csv(results_dir, data_dir, scn, fw, rep)
                reps[f'rep{rep}'] = load_csv(path) if path else None
            data[scn][fw] = reps
    return data


# ============================================================
# METRICS (Delegasi ke SATU implementasi kanonik di analysis_recovery_metrics.py)
# ============================================================
def _load_canonical_metrics():
    import importlib.util
    from pathlib import Path
    candidates = [
        Path(__file__).resolve().parent.parent / "analysis_recovery_metrics.py",
        Path(__file__).resolve().parent / "analysis_recovery_metrics.py",
        Path("analysis_recovery_metrics.py"),
    ]
    for c in candidates:
        if c.is_file():
            spec = importlib.util.spec_from_file_location("analysis_recovery_metrics", str(c))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise ImportError("analysis_recovery_metrics.py tidak ditemukan!")

arm = _load_canonical_metrics()


def recovery_metrics(rows, fault_time, t2_min=None):
    if not rows:
        return None
    bd = arm.recovery_breakdown(rows, fault_time, t2_min)
    return {'baseline': bd['baseline'], 't1': bd['t1'], 't2': bd['t2'],
            'recovery': bd['interval']}


def throughput_deficit(rows, fault_time=None):
    if not rows:
        return None
    win = arm.unmeasurable_window(rows, fault_time) if fault_time is not None else None
    return arm.deficit(rows, win)


def row_backlog(r):
    return arm.backlog_rate(r)


def mean_std_bins(rows_list, value_fn, bin_seconds=10.0):
    """Mean ± std antar rep per bin 10s. value_fn(row) -> float."""
    valid = [rs for rs in rows_list if rs]
    if not valid:
        return [], [], []
    bins = {}
    for rs in valid:
        seen = set()
        for r in rs:
            v = value_fn(r)
            if not math.isfinite(v):
                continue
            b = int(r['t'] // bin_seconds)
            if b in seen:
                continue
            seen.add(b)
            bins.setdefault(b, []).append(v)
    ts, means, stds = [], [], []
    for b in sorted(bins):
        vals = bins[b]
        ts.append(b * bin_seconds + bin_seconds / 2.0)
        means.append(sum(vals) / len(vals))
        stds.append(np.std(vals) if len(vals) > 1 else 0.0)
    return ts, means, stds


def metric_value_fn(metric):
    def fn(r):
        v = r[metric]
        return v if math.isfinite(v) else float('nan')
    return fn


# ============================================================
# PLOTTING HELPERS
# ============================================================
def add_fault_lines(ax, scn_cfg):
    for ft in scn_cfg['fault_lines']:
        ax.axvline(ft, color=FAULT_COLOR, linestyle=':', linewidth=1.6, alpha=0.8)
    if scn_cfg['restart_time']:
        ax.axvline(scn_cfg['restart_time'], color=RESTART_COLOR,
                   linestyle=':', linewidth=1.6, alpha=0.8)


def annotate_faults(ax, scn_cfg, ypos):
    fault_lines = scn_cfg['fault_lines']
    fault_labels = scn_cfg['fault_labels']
    for i, (ft, label) in enumerate(zip(fault_lines, fault_labels)):
        # kalau ada fault berikutnya yang dekat (< 60s), taruh label ke KIRI
        # biar nggak nabrak label fault berikutnya (mis. multi-fault t=300 & t=330)
        next_ft = fault_lines[i + 1] if i + 1 < len(fault_lines) else None
        if next_ft is not None and (next_ft - ft) < 60:
            ax.text(ft - 3, ypos, label, fontsize=8, color=FAULT_COLOR,
                    ha='right', va='center', linespacing=1.1)
        else:
            ax.text(ft + 3, ypos, label, fontsize=8, color=FAULT_COLOR,
                    ha='left', va='center', linespacing=1.1)
    if scn_cfg['restart_time'] and scn_cfg['restart_label']:
        ax.text(scn_cfg['restart_time'] + 3, ypos * 0.60, scn_cfg['restart_label'],
                fontsize=8, color=RESTART_COLOR, ha='left', va='center', linespacing=1.1)



def plot_rep_overlay(ax, rows_list, metric, color):
    """Plot 3 rep tipis + mean ± SD. value_fn default dari kolom metric."""
    ts, means, stds = mean_std_bins(rows_list, metric_value_fn(metric))
    for rs in rows_list:
        if not rs:
            continue
        vals = [metric_value_fn(metric)(r) for r in rs]
        ax.plot([r['t'] for r in rs], vals, color=color, lw=0.9, alpha=0.30)
    if ts:
        ax.plot(ts, means, color=MEAN_COLOR, lw=2.0, label='Mean (3 rep)')
        ax.fill_between(ts, [m - s for m, s in zip(means, stds)],
                        [m + s for m, s in zip(means, stds)],
                        color=MEAN_COLOR, alpha=STD_ALPHA, label='Mean ± SD')
    return ts, means, stds


def plot_load(ax, load_rows):
    ax.plot([r['t'] for r in load_rows], [r['load'] for r in load_rows],
            color=LOAD_COLOR, lw=1.1, ls='--', alpha=0.8)


METRIC_LABELS = {
    'tp': 'Throughput (msg/s)',
    'lat': 'Latency (ms)',
    'cpu': 'CPU (%)',
    'mem': 'Memory (MiB)',
    'backlog': 'Backlog Proxy (msg)',
}


# ============================================================
# FIGURE: metrik time-series per skenario (2 panel Flink/Spark)
# ============================================================
def fig_metric_scenario(data, scn, metric, fig_no):
    scn_cfg = SCENARIOS[scn]
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    fig.suptitle(
        f'Gambar 4.{fig_no}. {METRIC_LABELS[metric]} — {scn_cfg["label"]} (3 Replikasi)\n'
        f'{scn_cfg["load_label"]}',
        fontsize=13)

    for ax, fw in zip(axes, FRAMEWORKS):
        cfg = FRAMEWORKS[fw]
        rows_list = [data[scn][fw][f'rep{r}'] for r in REPS]
        if any(rs is None for rs in rows_list):
            ax.text(0.5, 0.5, 'Data tidak tersedia (CSV kurang)', ha='center',
                    transform=ax.transAxes, fontsize=13, color='#555555')
            ax.set_title(cfg['label'], fontsize=12, loc='left')
            continue

        load_rows = rows_list[0]
        if metric == 'backlog':
            for rs in rows_list:
                ax.plot([r['t'] for r in rs], [row_backlog(r) for r in rs],
                        color=cfg['color'], lw=0.9, alpha=0.30)
            ts, means, stds = mean_std_bins(rows_list, row_backlog)
            if ts:
                ax.plot(ts, means, color=MEAN_COLOR, lw=2.0, label='Mean (3 rep)')
                ax.fill_between(ts, [m - s for m, s in zip(means, stds)],
                                [m + s for m, s in zip(means, stds)],
                                color=MEAN_COLOR, alpha=STD_ALPHA, label='Mean ± SD')
        else:
            plot_rep_overlay(ax, rows_list, metric, cfg['color'])

        plot_load(ax, load_rows)
        add_fault_lines(ax, scn_cfg)

        # posisi label fault: pakai persentase tinggi sumbu (bukan max data)
        ylo, yhi = ax.get_ylim()
        annotate_faults(ax, scn_cfg, yhi * 0.90)

        ax.set_ylabel(METRIC_LABELS[metric])
        ax.set_title(cfg['label'], fontsize=12, loc='left')
        ax.set_ylim(bottom=0)
        ax.legend(loc='upper right', fontsize=8)

    axes[-1].set_xlabel('Waktu (detik)')
    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig4{fig_no}_{metric}_{scn}.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'  OK: {out}')
    return out


# ============================================================
# FIGURE: CPU & Memory per skenario (2x2 panel)
# ============================================================
def fig_resource_scenario(data, scn, fig_no):
    scn_cfg = SCENARIOS[scn]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        f'Gambar 4.{fig_no}. Penggunaan Sumber Daya — {scn_cfg["label"]} (3 Replikasi)\n'
        f'{scn_cfg["load_label"]} | CPU rata-rata (%), Memory total (MiB)',
        fontsize=13)

    panels = [
        ('flink', 'cpu', 0, 0, 'CPU — Apache Flink'),
        ('spark', 'cpu', 0, 1, 'CPU — Apache Spark'),
        ('flink', 'mem', 1, 0, 'Memory — Apache Flink'),
        ('spark', 'mem', 1, 1, 'Memory — Apache Spark'),
    ]
    for fw, metric, row, col, title in panels:
        ax = axes[row][col]
        cfg = FRAMEWORKS[fw]
        rows_list = [data[scn][fw][f'rep{r}'] for r in REPS]
        if any(rs is None for rs in rows_list):
            ax.text(0.5, 0.5, 'Data tidak tersedia (CSV kurang)', ha='center',
                    transform=ax.transAxes, fontsize=12, color='#555555')
            ax.set_title(title, fontsize=11, loc='left')
            continue
        load_rows = rows_list[0]
        plot_rep_overlay(ax, rows_list, metric, cfg['color'])
        if metric == 'cpu':
            # CPU dalam % — jangan plot load (msg/s) di axis yang sama,
            # dan cap sumbu 0-300 biar data CPU keliatan jelas (bukan 0-1000)
            ax.set_ylim(0, 300)
        else:
            plot_load(ax, load_rows)
            ax.set_ylim(bottom=0)
        add_fault_lines(ax, scn_cfg)
        ylo, yhi = ax.get_ylim()
        annotate_faults(ax, scn_cfg, yhi * 0.90)
        ax.set_ylabel(METRIC_LABELS[metric])
        ax.set_xlabel('Waktu (detik)')
        ax.set_title(title, fontsize=11, loc='left')
        ax.legend(fontsize=8, loc='upper right')


    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig4{fig_no}_resource_{scn}.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'  OK: {out}')
    return out


# ============================================================
# SUMMARY BAR CHARTS
# ============================================================
def fig_recovery_summary(data, fig_no):
    scns = list(SCENARIOS)
    fws = list(FRAMEWORKS)
    x = np.arange(len(scns))
    width = 0.35
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle(
        f'Gambar 4.{fig_no}. Rata-rata Recovery Stabilization Interval per Skenario (3 Replikasi)',
        fontsize=13)

    for i, fw in enumerate(fws):
        means, stds = [], []
        for scn in scns:
            fault, t2_min = arm.SCENARIOS[scn]
            vals = []
            for rep in REPS:
                rs = data[scn][fw][f'rep{rep}']
                if rs:
                    m = recovery_metrics(rs, fault, t2_min)
                    if m and m['recovery'] is not None:
                        vals.append(m['recovery'])
            means.append(np.mean(vals) if vals else float('nan'))
            stds.append(np.std(vals, ddof=1) if len(vals) > 1 else 0.0)
        ax.bar(x + (i - 0.5) * width, means, width, yerr=stds, capsize=5,
               color=FRAMEWORKS[fw]['color'], alpha=0.85,
               label=FRAMEWORKS[fw]['label'], edgecolor='black', linewidth=0.5)
        for xi, v, s in zip(x + (i - 0.5) * width, means, stds):
            if not math.isnan(v):
                y_pos = v + (s if not math.isnan(s) else 0) + 1.8
                ax.text(xi, y_pos, f'{v:.1f}s', ha='center', fontsize=9.5, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIOS[s]['label'] for s in scns], fontsize=11)
    ax.set_ylabel('Recovery Stabilization Interval (Second)')
    ax.set_ylim(bottom=0, top=82)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.13), ncol=2, fontsize=10)
    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig4{fig_no}_recovery_summary.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'  OK: {out}')
    return out


def fig_deficit_summary(data, fig_no):
    scns = list(SCENARIOS)
    fws = list(FRAMEWORKS)
    x = np.arange(len(scns))
    width = 0.35
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle(
        f'Gambar 4.{fig_no}. Rata-rata Throughput Deficit per Skenario (3 Replikasi)',
        fontsize=13)

    for i, fw in enumerate(fws):
        means, stds = [], []
        for scn in scns:
            fault, _ = arm.SCENARIOS[scn]
            vals = []
            for rep in REPS:
                rs = data[scn][fw][f'rep{rep}']
                if rs:
                    d = throughput_deficit(rs, fault)
                    if d is not None:
                        vals.append(d)
            means.append(np.mean(vals) if vals else float('nan'))
            stds.append(np.std(vals, ddof=1) if len(vals) > 1 else 0.0)
        ax.bar(x + (i - 0.5) * width, means, width, yerr=stds, capsize=5,
               color=FRAMEWORKS[fw]['color'], alpha=0.85,
               label=FRAMEWORKS[fw]['label'], edgecolor='black', linewidth=0.5)
        for xi, v, s in zip(x + (i - 0.5) * width, means, stds):
            if not math.isnan(v):
                y_pos = v + (s if not math.isnan(s) else 0) + 0.6
                ax.text(xi, y_pos, f'{v:.2f}%', ha='center', fontsize=9.5, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIOS[s]['label'] for s in scns], fontsize=11)
    ax.set_ylabel('Throughput Deficit (%)')
    ax.set_ylim(bottom=0, top=25)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.13), ncol=2, fontsize=10)
    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig4{fig_no}_deficit_summary.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'  OK: {out}')
    return out


# ============================================================
# CONSOLE SUMMARY TABLE
# ============================================================
def print_summary(data):
    print('\n' + '=' * 82)
    print('RINGKASAN RECOVERY & DEFICIT (dihitung dari CSV)')
    print('=' * 82)
    print(f"{'Skenario':<12} {'Framework':<8} {'Baseline':>9} {'T1':>7} {'T2':>7} "
          f"{'Recovery':>9} {'Deficit':>8}")
    print('-' * 82)
    for scn in SCENARIOS:
        fault, t2_min = arm.SCENARIOS[scn]
        for fw in FRAMEWORKS:
            for rep in REPS:
                rs = data[scn][fw][f'rep{rep}']
                if not rs:
                    print(f"{SCENARIOS[scn]['label']:<12} {fw:<8} MISSING rep{rep}")
                    continue
                m = recovery_metrics(rs, fault, t2_min)
                d = throughput_deficit(rs, fault)
                rec = f"{m['recovery']:.1f}s" if m and m['recovery'] is not None else 'N/A'
                print(f"{SCENARIOS[scn]['label']:<12} {fw:<8} {m['baseline']:9.2f} "
                      f"{m['t1']:7.1f} {m['t2']:7.1f} {rec:>9} {d:8.2f}%")
    print('=' * 82)


# ============================================================
# MAIN
# ============================================================
def main():
    global OUTPUT_DIR
    p = argparse.ArgumentParser(description='Chart generator Bab 4 — 3 skenario x 3 rep')
    p.add_argument('--results-dir', default='./results',
                   help='Folder RESULTS berisi {fw}_{scn}_rep{N}.csv (default: ./results)')
    p.add_argument('--data-dir', default=os.path.dirname(os.path.abspath(__file__)),
                   help='Fallback folder berisi multi_3x/, single_3x/, central_3x/ (default: folder script)')
    p.add_argument('--out-dir', default='./output_charts_bab4', help='Folder output')
    args = p.parse_args()

    OUTPUT_DIR = args.out_dir
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print('Loading CSV files...')
    data = load_all(args.results_dir, args.data_dir)
    n_missing = sum(1 for scn in data for fw in data[scn] for rep in REPS
                    if data[scn][fw][f'rep{rep}'] is None)
    print(f'  {18 - n_missing}/18 CSV tersedia ({n_missing} missing)')
    print()

    print_summary(data)

    print('\nGenerating charts...')
    fig_no = 1
    for scn in SCENARIOS:
        fig_metric_scenario(data, scn, 'tp', fig_no);     fig_no += 1
        fig_metric_scenario(data, scn, 'lat', fig_no);    fig_no += 1
        fig_resource_scenario(data, scn, fig_no);          fig_no += 1
        fig_metric_scenario(data, scn, 'backlog', fig_no); fig_no += 1
    fig_recovery_summary(data, fig_no); fig_no += 1
    fig_deficit_summary(data, fig_no)
    print(f'\nSelesai. Output: {OUTPUT_DIR}/')


if __name__ == '__main__':
    main()
