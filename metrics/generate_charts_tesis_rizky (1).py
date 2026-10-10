"""
Chart Generator Script — Thesis Rizky Zein Adam
Fault Recovery Evaluation: Apache Flink vs Apache Spark Structured Streaming
Universitas Nasional, 2026

FIXED VERSION (18 Agu 2026):
- Recovery & deficit DIHITUNG dari CSV (bukan hardcode) — sesuai definisi proposal
- T1 = fault_time jika tidak terdeteksi <50% (graceful degradation Spark)
- fig44 pakai backlog proxy (load_rate - throughput) — bukan kafka_lag artifact
- fig45 memory dalam MiB (bukan %), ylim dinamis
- Font & angka lebih besar biar jelas
- Missing file = di-skip (grafik tetap jalan)

HOW TO USE:
1. Taruh 6 CSV di folder yang sama (nama sesuai CSV_FILES, bisa diubah di bawah)
2. pip install matplotlib numpy
3. python generate_charts_tesis_rizky.py
4. Hasil: ./output_charts/
"""

import csv
import math
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ============================================================
# CSV PATH CONFIGURATION
# ============================================================
CSV_FILES = {
    'flink_dynamic':  'run01_flink_dynamic_load.csv',
    'spark_dynamic':  'run01_spark_dynamic_load.csv',
    'flink_single':   'run01_flink_single_node_failure.csv',
    'spark_single':   'run01_spark_single_node_failure.csv',
    'flink_central':  'run01_flink_central_component_failure.csv',
    'spark_central':  'run01_spark_central_component_failure.csv',
}

# Fault times per scenario (detik) — sesuaikan dengan fault.yaml
FAULT_TIMES = {
    'dynamic': {'fault': 330, 'restart': 660, 'fault_lines': [300, 330]},
    'single':  {'fault': 120, 'restart': None, 'fault_lines': [120]},
    'central': {'fault': 120, 'restart': 180, 'fault_lines': [120]},
}

OUTPUT_DIR = './output_charts'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Colors
FLINK_COLOR   = '#D62728'
SPARK_COLOR   = '#FF7F0E'
LOAD_COLOR    = '#999999'
FAULT_COLOR   = '#CC0000'
RESTART_COLOR = '#0055AA'

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 12,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'legend.fontsize': 11,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ============================================================
# LOAD DATA (stdlib csv — tidak butuh pandas)
# ============================================================
def load_csv(path):
    rows = []
    if not os.path.exists(path):
        return None
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
                    'lag': float(r['kafka_lag']),
                })
            except (ValueError, KeyError):
                continue
    rows.sort(key=lambda x: x['t'])
    return rows

print("Loading CSV files...")
dfs = {}
missing = []
for key, fname in CSV_FILES.items():
    df = load_csv(fname)
    if df is not None and df:
        dfs[key] = df
        print(f"  OK: {fname} — {len(df)} records")
    else:
        missing.append(fname)
        print(f"  MISSING: {fname}")

if missing:
    print(f"\nWARNING: {len(missing)} CSV missing — figures that need them will be skipped.")
    if len(missing) == len(CSV_FILES):
        print("No data at all. Exiting.")
        sys.exit(1)
print()

def col_vals(df, name):
    return [r[name] for r in df]

# Backlog proxy per row: max(load - tp, 0) — bukan kafka_lag (artifact Flink)
def backlog_col(df):
    return [max(0.0, r['load'] - r['tp']) if math.isfinite(r['tp']) else 0.0 for r in df]

# ============================================================
# HELPER: recovery metrics dari CSV (definisi proposal)
# ============================================================
def resample_10s(df, bin_seconds=10.0):
    bins = {}
    for r in df:
        if not math.isfinite(r['tp']):
            continue
        b = int(r['t'] // bin_seconds)
        bins.setdefault(b, []).append(r['tp'])
    out = []
    for b in sorted(bins):
        out.append({'t': b * bin_seconds + bin_seconds / 2.0, 'tp': sum(bins[b]) / len(bins[b])})
    return out

def recovery_metrics(df, fault_time, baseline_window=60.0):
    """Kembalikan dict recovery sesuai definisi proposal.
    T1 = fault_time jika <50% baseline tidak terdeteksi (graceful degradation)."""
    if df is None or not df:
        return None
    samples = resample_10s(df)
    base_vals = [r['tp'] for r in df if math.isfinite(r['tp']) and (fault_time - baseline_window) <= r['t'] < fault_time]
    baseline = (sum(base_vals) / len(base_vals)) if base_vals else None
    if baseline is None or baseline <= 0:
        return None

    post = [s for s in samples if s['t'] >= fault_time]
    t1 = next((s['t'] for s in post if s['tp'] < 0.5 * baseline), None)
    if t1 is None:
        t1 = float(fault_time)  # graceful degradation → T1 = waktu injeksi
    streak = 0
    t2 = None
    for s in post:
        if s['tp'] >= 0.9 * baseline:
            streak += 1
            if streak >= 5:
                t2 = s['t']
                break
        else:
            streak = 0
    return {'baseline': baseline, 't1': t1, 't2': t2,
            'recovery': (t2 - t1) if t2 is not None else None}

def throughput_deficit(df):
    if df is None or not df:
        return None
    offered = actual = 0.0
    for i, r in enumerate(df):
        dt = 1.0 if i == 0 else r['t'] - df[i - 1]['t']
        if dt > 30:
            dt = 1.0
        offered += r['load'] * dt
        actual += (r['tp'] if math.isfinite(r['tp']) else 0.0) * dt
    if offered <= 0:
        return None
    return max(0.0, (1.0 - actual / offered) * 100.0)

def mean_col(df, name, min_t=None, max_t=None):
    vals = [r[name] for r in df if math.isfinite(r[name])
            and (min_t is None or r['t'] >= min_t) and (max_t is None or r['t'] < max_t)]
    return (sum(vals) / len(vals)) if vals else float('nan')

# ============================================================
# HELPER: fault lines
# ============================================================
def add_fault_lines(ax, fault_times, restart_time=None):
    for ft in fault_times:
        ax.axvline(ft, color=FAULT_COLOR, linestyle=':', linewidth=1.5, alpha=0.75)
    if restart_time:
        ax.axvline(restart_time, color=RESTART_COLOR, linestyle=':', linewidth=1.5, alpha=0.75)

# ============================================================
# FIGURE 4.1 — Throughput Multi-Fault Cascading
# ============================================================
def fig41():
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    fig.suptitle(
        'Figure 4.1. Throughput Evolution — Dynamic Load + Multi-Fault Cascading (900 s)\n'
        'Fault: Switchover Worker2 (t=300s), Kill Worker1 (t=330s), Restart Worker1 (t=660s)',
        fontsize=13)

    for ax, key, label, color in [
        (axes[0], 'flink_dynamic', 'Apache Flink', FLINK_COLOR),
        (axes[1], 'spark_dynamic', 'Apache Spark Structured Streaming', SPARK_COLOR),
    ]:
        if key not in dfs:
            ax.text(0.5, 0.5, 'Data not available', ha='center', transform=ax.transAxes, fontsize=14)
            continue
        df = dfs[key]
        ax.plot(col_vals(df, 't'), col_vals(df, 'tp'), color=color, lw=1.3, label='Throughput (msg/s)')
        ax.plot(col_vals(df, 't'), col_vals(df, 'load'), color=LOAD_COLOR, lw=1.0, ls='--', label='Offered Load (msg/s)', alpha=0.7)
        add_fault_lines(ax, [300, 330], 660)
        ymax = max(col_vals(df, 'tp'))
        ax.text(262, ymax * 0.68, 'Switchover\nWorker2', fontsize=9, color=FAULT_COLOR, ha='right', va='center')
        ax.text(338, ymax * 0.50, 'Kill\nWorker1', fontsize=9, color=FAULT_COLOR, ha='left', va='center')
        ax.text(668, ymax * 0.30, 'Restart\nWorker1', fontsize=9, color=RESTART_COLOR, ha='left', va='center')
        ax.set_ylabel('msg/s')
        ax.set_title(label, fontsize=12, loc='left')
        ax.legend(loc='upper right', fontsize=10)
        ax.set_ylim(bottom=0)

    axes[1].set_xlabel('Elapsed Time (seconds)')
    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig41_throughput_multifault.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.2 — Throughput Single Node + Central Component
# ============================================================
def fig42():
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        'Figure 4.2. Throughput — Single Node Failure (left) & Central Component Failure (right)\n'
        'Fault t=120s; Restart t=180s (Central Component only)',
        fontsize=13)

    configs = [
        ('flink_single',  'Flink — Single Node Failure',      FLINK_COLOR, 0, 0, 'single'),
        ('spark_single',  'Spark — Single Node Failure',       SPARK_COLOR, 1, 0, 'single'),
        ('flink_central', 'Flink — Central Component Failure', FLINK_COLOR, 0, 1, 'central'),
        ('spark_central', 'Spark — Central Component Failure', SPARK_COLOR, 1, 1, 'central'),
    ]

    for key, title, color, row, col_idx, scen in configs:
        ax = axes[row][col_idx]
        if key not in dfs:
            ax.text(0.5, 0.5, 'Data not available', ha='center', transform=ax.transAxes, fontsize=14)
            continue
        df = dfs[key]
        ft = FAULT_TIMES[scen]
        ax.plot(col_vals(df, 't'), col_vals(df, 'tp'), color=color, lw=1.3, label='Throughput')
        ax.plot(col_vals(df, 't'), col_vals(df, 'load'), color=LOAD_COLOR, lw=1.0, ls='--', label='Offered Load', alpha=0.7)
        add_fault_lines(ax, ft['fault_lines'], ft['restart'])
        ymax = max(col_vals(df, 'tp'))
        ax.text(125, ymax * 0.70, 'Fault\nt=120s', fontsize=9, color=FAULT_COLOR)
        if ft['restart']:
            ax.text(185, ymax * 0.40, 'Restart\nt=180s', fontsize=9, color=RESTART_COLOR)
        ax.set_title(title, fontsize=11, loc='left')
        ax.set_ylabel('msg/s')
        ax.set_xlabel('Elapsed (s)')
        ax.legend(fontsize=9, loc='upper right')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig42_throughput_single_central.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.3 — Latency Three Scenarios
# ============================================================
def fig43():
    fig, axes = plt.subplots(2, 3, figsize=(17, 9))
    fig.suptitle(
        'Figure 4.3. Latency Evolution — Flink and Spark across Three Fault Scenarios\n'
        'Drop to 0ms in Spark = idle batch interval (not actual zero latency)',
        fontsize=13)

    configs = [
        ('flink_dynamic',  'Flink — Multi-Fault',      FLINK_COLOR, 0, 0),
        ('flink_single',   'Flink — Single Node',       FLINK_COLOR, 0, 1),
        ('flink_central',  'Flink — Central Component', FLINK_COLOR, 0, 2),
        ('spark_dynamic',  'Spark — Multi-Fault *',     SPARK_COLOR, 1, 0),
        ('spark_single',   'Spark — Single Node *',     SPARK_COLOR, 1, 1),
        ('spark_central',  'Spark — Central Component *', SPARK_COLOR, 1, 2),
    ]

    for key, title, color, row, col_idx in configs:
        ax = axes[row][col_idx]
        if key not in dfs:
            ax.text(0.5, 0.5, 'Data not available', ha='center', transform=ax.transAxes, fontsize=14)
            continue
        df = dfs[key]
        lat = [r['lat'] if math.isfinite(r['lat']) else 0.0 for r in df]
        ax.plot(col_vals(df, 't'), lat, color=color, lw=0.9, alpha=0.85)
        pos = [v for v in lat if v > 0]
        avg = (sum(pos) / len(pos)) if pos else 0.0
        ax.axhline(avg, color='gray', ls='--', lw=0.8, alpha=0.7)
        ax.text(col_vals(df, 't')[-1] * 0.05, avg * 1.08 + 1, f'avg={avg:.1f}ms', fontsize=9, color='gray')
        ax.set_title(title, fontsize=11)
        ax.set_ylabel('Latency (ms)')
        ax.set_xlabel('Elapsed (s)')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig43_latency.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.4 — Backlog Proxy (load - throughput), bukan kafka_lag
# ============================================================
def fig44():
    fig, axes = plt.subplots(2, 3, figsize=(17, 9))
    fig.suptitle(
        'Figure 4.4. Instantaneous Backlog Proxy — Flink and Spark across Three Fault Scenarios\n'
        'Backlog proxy = max(load_rate − throughput, 0) per second (bukan cumulative)',
        fontsize=13)

    configs = [
        ('flink_dynamic',  'Flink — Multi-Fault',      FLINK_COLOR, 0, 0),
        ('flink_single',   'Flink — Single Node',       FLINK_COLOR, 0, 1),
        ('flink_central',  'Flink — Central Component', FLINK_COLOR, 0, 2),
        ('spark_dynamic',  'Spark — Multi-Fault',       SPARK_COLOR, 1, 0),
        ('spark_single',   'Spark — Single Node',       SPARK_COLOR, 1, 1),
        ('spark_central',  'Spark — Central Component', SPARK_COLOR, 1, 2),
    ]

    for key, title, color, row, col_idx in configs:
        ax = axes[row][col_idx]
        if key not in dfs:
            ax.text(0.5, 0.5, 'Data not available', ha='center', transform=ax.transAxes, fontsize=14)
            continue
        df = dfs[key]
        back = backlog_col(df)
        ax.fill_between(col_vals(df, 't'), back, color=color, alpha=0.3)
        ax.plot(col_vals(df, 't'), back, color=color, lw=0.9)
        peak = max(back)
        peak_t = col_vals(df, 't')[back.index(peak)]
        ax.annotate(f'peak={peak:,.0f}\nt={peak_t:.0f}s',
                    xy=(peak_t, peak), xytext=(peak_t + 30, peak * 0.75),
                    fontsize=9, color='darkred',
                    arrowprops=dict(arrowstyle='->', color='darkred', lw=0.7))
        ax.set_title(title, fontsize=11)
        ax.set_ylabel('Backlog (records/s)')
        ax.set_xlabel('Elapsed (s)')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig44_backlog_proxy.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.5 — Resource Usage (CPU % dan Memory MiB)
# ============================================================
def fig45():
    fig, axes = plt.subplots(4, 3, figsize=(17, 15))
    fig.suptitle(
        'Figure 4.5. CPU Utilization (rows 1–2) and Memory (rows 3–4)\n'
        'Flink (rows 1 & 3) and Spark (rows 2 & 4) across Three Scenarios — Memory dalam MiB',
        fontsize=13)

    order = [
        ('flink_dynamic', 'Flink Multi-Fault',   FLINK_COLOR, 0),
        ('flink_single',  'Flink Single Node',    FLINK_COLOR, 1),
        ('flink_central', 'Flink Central Comp',   FLINK_COLOR, 2),
        ('spark_dynamic', 'Spark Multi-Fault',    SPARK_COLOR, 0),
        ('spark_single',  'Spark Single Node',    SPARK_COLOR, 1),
        ('spark_central', 'Spark Central Comp',   SPARK_COLOR, 2),
    ]

    for i, (key, label, color, col_idx) in enumerate(order):
        r_cpu = 0 if i < 3 else 2
        r_mem = 1 if i < 3 else 3
        if key not in dfs:
            for r in (r_cpu, r_mem):
                axes[r][col_idx].text(0.5, 0.5, 'N/A', ha='center', transform=axes[r][col_idx].transAxes, fontsize=14)
            continue
        df = dfs[key]

        # CPU
        ax = axes[r_cpu][col_idx]
        cpu = col_vals(df, 'cpu')
        ax.plot(col_vals(df, 't'), cpu, color=color, lw=0.9)
        ax.fill_between(col_vals(df, 't'), cpu, color=color, alpha=0.15)
        avg = mean_col(df, 'cpu')
        ax.axhline(avg, color='gray', ls='--', lw=0.7)
        ax.text(10, avg + 2, f'avg={avg:.1f}%', fontsize=9, color='gray')
        ax.set_title(f'{label} — CPU (%)', fontsize=10)
        ax.set_ylabel('CPU (%)', fontsize=10)
        ax.set_ylim(0, min(110, max(cpu) * 1.15 + 5))
        ax.set_xlabel('Elapsed (s)', fontsize=10)

        # Memory (MiB — bukan %)
        ax = axes[r_mem][col_idx]
        mem = col_vals(df, 'mem')
        ax.plot(col_vals(df, 't'), mem, color=color, lw=0.9)
        ax.fill_between(col_vals(df, 't'), mem, color=color, alpha=0.15)
        avg_m = mean_col(df, 'mem')
        ax.axhline(avg_m, color='gray', ls='--', lw=0.7)
        ax.text(10, avg_m + 20, f'avg={avg_m:.0f} MiB', fontsize=9, color='gray')
        ax.set_title(f'{label} — Memory (MiB)', fontsize=10)
        ax.set_ylabel('Memory (MiB)', fontsize=10)
        ax.set_ylim(0, max(mem) * 1.15 + 50)
        ax.set_xlabel('Elapsed (s)', fontsize=10)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig45_resource.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.6, 4.7, 4.8 — Master Timeline (3 scenarios)
# ============================================================
def master_timeline(scenario_key_flink, scenario_key_spark, scen, title, fault_labels, out_fname):
    fig, axes = plt.subplots(3, 2, figsize=(17, 12), sharex='col')
    fig.suptitle(title, fontsize=13)

    ft = FAULT_TIMES[scen]
    metrics = [
        ('tp',   'Throughput (msg/s)'),
        ('lat',  'Latency (ms)'),
    ]

    for col_idx, (key, label, color) in enumerate([
        (scenario_key_flink, 'Apache Flink', FLINK_COLOR),
        (scenario_key_spark, 'Apache Spark Structured Streaming', SPARK_COLOR),
    ]):
        if key not in dfs:
            for row in range(3):
                axes[row][col_idx].text(0.5, 0.5, 'N/A', ha='center', transform=axes[row][col_idx].transAxes, fontsize=14)
            continue
        df = dfs[key]

        # Baris 1: Throughput
        ax = axes[0][col_idx]
        ax.plot(col_vals(df, 't'), col_vals(df, 'tp'), color=color, lw=1.1)
        ax.plot(col_vals(df, 't'), col_vals(df, 'load'), color=LOAD_COLOR, lw=0.8, ls='--', alpha=0.6, label='Offered Load')
        ax.set_title(label, fontsize=12, fontweight='bold')
        add_fault_lines(ax, ft['fault_lines'], ft['restart'])
        ax.set_ylabel('msg/s', fontsize=11)
        ax.set_ylim(bottom=0)
        ax.legend(fontsize=9, loc='upper right')

        # Baris 2: Latency
        ax = axes[1][col_idx]
        lat = [r['lat'] if math.isfinite(r['lat']) else 0.0 for r in df]
        ax.plot(col_vals(df, 't'), lat, color=color, lw=1.0)
        add_fault_lines(ax, ft['fault_lines'], ft['restart'])
        ax.set_ylabel('Latency (ms)', fontsize=11)
        ax.set_ylim(bottom=0)

        # Baris 3: Backlog proxy
        ax = axes[2][col_idx]
        back = backlog_col(df)
        ax.fill_between(col_vals(df, 't'), back, color=color, alpha=0.3)
        ax.plot(col_vals(df, 't'), back, color=color, lw=1.0)
        add_fault_lines(ax, ft['fault_lines'], ft['restart'])
        ax.set_ylabel('Backlog (rec/s)', fontsize=11)
        ax.set_ylim(bottom=0)

        # Anotasi fault (baris atas)
        ylim_top = axes[0][col_idx].get_ylim()[1]
        y_levels = [ylim_top * 0.85, ylim_top * 0.65]
        for idx, (fault_t, flabel) in enumerate(zip(ft['fault_lines'], fault_labels)):
            axes[0][col_idx].text(fault_t + 5, y_levels[idx % len(y_levels)], flabel,
                                  fontsize=9, color=FAULT_COLOR, ha='left', va='center')
        if ft['restart']:
            axes[0][col_idx].text(ft['restart'] + 5, ylim_top * 0.15, 'Restart',
                                  fontsize=9, color=RESTART_COLOR, ha='left')

    for col_idx in range(2):
        axes[2][col_idx].set_xlabel('Elapsed Time (seconds)')

    patches = [mpatches.Patch(color=FAULT_COLOR, label='Fault event')]
    if ft['restart']:
        patches.append(mpatches.Patch(color=RESTART_COLOR, label='Restart'))
    fig.legend(handles=patches, loc='lower center', ncol=2, fontsize=10, bbox_to_anchor=(0.5, 0.005))

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    out = f'{OUTPUT_DIR}/{out_fname}'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

def fig46():
    master_timeline(
        'flink_dynamic', 'spark_dynamic', 'dynamic',
        title='Figure 4.6. Master Timeline — Dynamic Load + Multi-Fault Cascading\n'
              'Throughput, Latency, Backlog Proxy with fault event annotations',
        fault_labels=['Switchover\nWorker2', 'Kill\nWorker1'],
        out_fname='fig46_master_timeline_multifault.png'
    )

def fig47():
    master_timeline(
        'flink_single', 'spark_single', 'single',
        title='Figure 4.7. Master Timeline — Single Node Failure\n'
              'Throughput, Latency, Backlog Proxy (fault t=120s, no restart)',
        fault_labels=['Kill Worker1'],
        out_fname='fig47_master_timeline_single.png'
    )

def fig48():
    master_timeline(
        'flink_central', 'spark_central', 'central',
        title='Figure 4.8. Master Timeline — Central Component Failure\n'
              'Throughput, Latency, Backlog Proxy (fault t=120s, restart t=180s)',
        fault_labels=['Kill JobManager/Driver'],
        out_fname='fig48_master_timeline_central.png'
    )

# ============================================================
# FIGURE 4.9 — Stabilization Interval Bar Chart (dari CSV)
# ============================================================
def fig49():
    scenarios = ['Multi-Fault\nCascading', 'Single Node\nFailure', 'Central Component\nFailure']
    scen_keys = ['dynamic', 'single', 'central']

    flink_vals, spark_vals = [], []
    for sk in scen_keys:
        ft = FAULT_TIMES[sk]['fault']
        fm = recovery_metrics(dfs.get('flink_' + sk), ft)
        sm = recovery_metrics(dfs.get('spark_' + sk), ft)
        flink_vals.append(fm['recovery'] if fm and fm['recovery'] is not None else None)
        spark_vals.append(sm['recovery'] if sm and sm['recovery'] is not None else None)

    x = np.arange(len(scenarios))
    w = 0.32
    fig, ax = plt.subplots(figsize=(11, 7))

    fv = [v if v is not None else 0.0 for v in flink_vals]
    sv = [v if v is not None else 0.0 for v in spark_vals]
    b1 = ax.bar(x - w / 2, fv, w, label='Apache Flink', color=FLINK_COLOR, alpha=0.85)
    b2 = ax.bar(x + w / 2, sv, w, label='Apache Spark', color=SPARK_COLOR, alpha=0.85)

    for i, (bar, val) in enumerate(zip(b1, flink_vals)):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f'{val:.1f}s' if val is not None else 'N/A',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    for i, (bar, val) in enumerate(zip(b2, spark_vals)):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f'{val:.1f}s' if val is not None else 'N/A',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
        if val is not None and flink_vals[i] is not None and flink_vals[i] > 0:
            ratio = val / flink_vals[i]
            ax.text(i, val + 1.8, f'Flink {ratio:.1f}x faster',
                    ha='center', fontsize=10, color='darkgreen', fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=11)
    ax.set_ylabel('Stabilization Interval (seconds)', fontsize=12)
    ax.set_title('Figure 4.9. Recovery Stabilization Interval — Flink vs Spark\n'
                 'T1 = fault injection time; T2 = throughput ≥90% baseline, stable ≥5 consecutive 10s samples',
                 fontsize=12)
    ax.legend(fontsize=11)
    ymax = max([v for v in [*fv, *sv] if v is not None] or [1])
    ax.set_ylim(0, ymax * 1.35 + 3)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig49_stabilization.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.10 — Throughput Deficit (dari CSV)
# ============================================================
def fig410():
    scenarios = ['Multi-Fault\nCascading', 'Single Node\nFailure', 'Central Component\nFailure']
    scen_keys = ['dynamic', 'single', 'central']

    flink_vals = [throughput_deficit(dfs.get('flink_' + sk)) for sk in scen_keys]
    spark_vals = [throughput_deficit(dfs.get('spark_' + sk)) for sk in scen_keys]

    x = np.arange(len(scenarios))
    w = 0.32
    fig, ax = plt.subplots(figsize=(11, 7))

    fv = [v if v is not None else 0.0 for v in flink_vals]
    sv = [v if v is not None else 0.0 for v in spark_vals]
    b1 = ax.bar(x - w / 2, fv, w, label='Apache Flink', color=FLINK_COLOR, alpha=0.85)
    b2 = ax.bar(x + w / 2, sv, w, label='Apache Spark', color=SPARK_COLOR, alpha=0.85)

    for bar, val in zip(b1, flink_vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f'{val:.1f}%' if val is not None else 'N/A',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    for bar, val in zip(b2, spark_vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f'{val:.1f}%' if val is not None else 'N/A',
                ha='center', va='bottom', fontsize=12, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=11)
    ax.set_ylabel('Throughput Deficit (%)', fontsize=12)
    ax.set_title('Figure 4.10. Throughput Deficit — Numerical Integral Reconciliation\n'
                 '1 − (sum(throughput×dt) / sum(load_rate×dt)) × 100%',
                 fontsize=12)
    ax.legend(fontsize=11)
    ax.set_ylim(0, 105)
    ax.axhline(100, color='red', ls='--', lw=0.8, alpha=0.4)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig410_dataloss.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.11 — Summary Comparison (dari CSV, window stabil pre-fault)
# ============================================================
def fig411():
    labels = ['Multi-Fault', 'Single Node', 'Central Comp']
    scen_keys = ['dynamic', 'single', 'central']
    pre_fault_windows = {
        'dynamic': (60, 300),   # fase 100-200 stabil sebelum kill
        'single':  (30, 120),
        'central': (30, 120),
    }

    flink_tp, spark_tp = [], []
    flink_lat, spark_lat = [], []
    flink_cpu, spark_cpu = [], []

    for sk in scen_keys:
        lo, hi = pre_fault_windows[sk]
        fk = 'flink_' + sk
        skk = 'spark_' + sk
        flink_tp.append(mean_col(dfs.get(fk), 'tp', lo, hi) if fk in dfs else float('nan'))
        spark_tp.append(mean_col(dfs.get(skk), 'tp', lo, hi) if skk in dfs else float('nan'))
        flink_lat.append(mean_col(dfs.get(fk), 'lat', lo, hi) if fk in dfs else float('nan'))
        spark_lat.append(mean_col(dfs.get(skk), 'lat', lo, hi) if skk in dfs else float('nan'))
        flink_cpu.append(mean_col(dfs.get(fk), 'cpu', lo, hi) if fk in dfs else float('nan'))
        spark_cpu.append(mean_col(dfs.get(skk), 'cpu', lo, hi) if skk in dfs else float('nan'))

    def fmt(v):
        return 'N/A' if math.isnan(v) else f'{v:.1f}'

    fig, axes = plt.subplots(1, 3, figsize=(17, 7))
    fig.suptitle('Figure 4.11. Throughput, Latency, and CPU Comparison (steady state sebelum fault)\n'
                 'Rata-rata pada window stabil pre-fault — Multi-Fault: t=60–300s; Single/Central: t=30–120s',
                 fontsize=13)

    datasets = [
        (axes[0], flink_tp, spark_tp, 'Avg Throughput (msg/s)', 'Throughput', ''),
        (axes[1], flink_lat, spark_lat, 'Avg Latency (ms)', 'Latency', '* Spark = proxy batch interval'),
        (axes[2], flink_cpu, spark_cpu, 'Avg CPU Usage (%)', 'CPU Usage', ''),
    ]

    x = np.arange(len(labels))
    w = 0.32
    for ax, fv, sv, ylabel, title, note in datasets:
        b1 = ax.bar(x - w / 2, [0 if math.isnan(v) else v for v in fv], w,
                    label='Apache Flink', color=FLINK_COLOR, alpha=0.85)
        b2 = ax.bar(x + w / 2, [0 if math.isnan(v) else v for v in sv], w,
                    label='Apache Spark', color=SPARK_COLOR, alpha=0.85)
        for bar, val in zip(b1, fv):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    fmt(val), ha='center', va='bottom', fontsize=11, fontweight='bold')
        for bar, val in zip(b2, sv):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    fmt(val), ha='center', va='bottom', fontsize=11, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=10)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_title(f'{title}\n{note}', fontsize=11)
        ax.legend(fontsize=10)
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig411_comparison.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print('=' * 60)
    print('Thesis Rizky Zein Adam — Chapter 4 Chart Generator (FIXED)')
    print(f'Output: {OUTPUT_DIR}')
    print('=' * 60)
    print()

    tasks = [
        ('Figure 4.1  — Throughput Multi-Fault',      fig41),
        ('Figure 4.2  — Throughput Single + Central', fig42),
        ('Figure 4.3  — Latency Three Scenarios',     fig43),
        ('Figure 4.4  — Backlog Proxy',               fig44),
        ('Figure 4.5  — Resource CPU & Memory',       fig45),
        ('Figure 4.6  — Master Timeline Multi-Fault', fig46),
        ('Figure 4.7  — Master Timeline Single Node', fig47),
        ('Figure 4.8  — Master Timeline Central Comp', fig48),
        ('Figure 4.9  — Stabilization Interval Bar',  fig49),
        ('Figure 4.10 — Throughput Deficit Bar',      fig410),
        ('Figure 4.11 — Summary Comparison',          fig411),
    ]

    for name, func in tasks:
        print(f'{name}...')
        try:
            func()
        except Exception as e:
            print(f"  ERROR: {e}")

    print()
    print('=' * 60)
    print('DONE — Files generated:')
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith('.png'):
            size = os.path.getsize(f'{OUTPUT_DIR}/{f}') // 1024
            print(f'  {f} ({size} KB)')
    print('=' * 60)