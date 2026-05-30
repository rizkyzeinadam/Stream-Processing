"""
Chart Generator Script — Thesis Rizky Zein Adam
Fault Recovery Evaluation: Apache Flink vs Apache Spark Structured Streaming
Universitas Nasional, 2026

HOW TO USE:
1. Save this script in the same folder as the 6 CSV files
2. Open terminal/cmd in that folder
3. Install dependencies: pip install pandas matplotlib numpy
4. Run: python generate_charts_tesis_rizky.py
5. All charts are saved to ./output_charts/

CSV FILES REQUIRED:
- run01_flink_dynamic_load.csv
- run01_spark_dynamic_load.csv
- run01_flink_single_node_failure.csv
- run01_spark_single_node_failure.csv
- run01_flink_central_component_failure.csv
- run01_spark_central_component_failure.csv
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os, sys

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

OUTPUT_DIR = './output_charts'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Colors
FLINK_COLOR   = '#D62728'
SPARK_COLOR   = '#FF7F0E'
LOAD_COLOR    = '#AAAAAA'
FAULT_COLOR   = '#CC0000'
RESTART_COLOR = '#0055AA'

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 10,
    'axes.titlesize': 11,
    'axes.labelsize': 10,
    'legend.fontsize': 9,
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ============================================================
# LOAD DATA
# ============================================================
print("Loading CSV files...")
dfs = {}
missing = []
for key, fname in CSV_FILES.items():
    if os.path.exists(fname):
        dfs[key] = pd.read_csv(fname)
        print(f"  OK: {fname} — {dfs[key].shape[0]} records")
    else:
        missing.append(fname)
        print(f"  MISSING: {fname}")

if missing:
    print(f"\nWARNING: {len(missing)} CSV file(s) not found.")
    print("Make sure the script is run from the same folder as the CSV files.")
    if len(missing) == len(CSV_FILES):
        sys.exit(1)
print()

# ============================================================
# HELPER: add fault lines
# ============================================================
def add_fault_lines(ax, fault_times, restart_time=None, ymax=None):
    for ft in fault_times:
        ax.axvline(ft, color=FAULT_COLOR, linestyle=':', linewidth=1.5, alpha=0.75)
    if restart_time:
        ax.axvline(restart_time, color=RESTART_COLOR, linestyle=':', linewidth=1.5, alpha=0.75)

# ============================================================
# FIGURE 4.1 — Throughput Multi-Fault Cascading
# ============================================================
def fig41():
    fig, axes = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
    fig.suptitle(
        'Figure 4.1. Throughput Evolution — Dynamic Load + Multi-Fault Cascading (900 s)\n'
        'Fault: Switchover Worker2 (t=300s), Kill Worker1 (t=330s), Restart Worker1 (t=660s)',
        fontsize=11)

    for ax, key, label, color in [
        (axes[0], 'flink_dynamic', 'Apache Flink', FLINK_COLOR),
        (axes[1], 'spark_dynamic', 'Apache Spark Structured Streaming', SPARK_COLOR),
    ]:
        if key not in dfs: continue
        df = dfs[key]
        ax.plot(df['elapsed_seconds'], df['throughput'], color=color, lw=1.2, label='Throughput (msg/s)')
        ax.plot(df['elapsed_seconds'], df['load_rate'],  color=LOAD_COLOR, lw=1.0, ls='--', label='Offered Load (msg/s)', alpha=0.7)
        add_fault_lines(ax, [300, 330], 660)
        # Annotations — stagger vertically to avoid overlap (only 30s apart)
        ymax = df['throughput'].max()
        ax.text(265, ymax*0.65, 'Switchover\nWorker2', fontsize=7, color=FAULT_COLOR,
                ha='right', va='center')
        ax.text(335, ymax*0.50, 'Kill\nWorker1', fontsize=7, color=FAULT_COLOR,
                ha='left', va='center')
        ax.text(665, ymax*0.30, 'Restart\nWorker1', fontsize=7, color=RESTART_COLOR,
                ha='left', va='center')
        ax.set_ylabel('msg/s')
        ax.set_title(label, fontsize=10, loc='left')
        ax.legend(loc='upper right', fontsize=8)
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
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.suptitle(
        'Figure 4.2. Throughput — Single Node Failure (left) & Central Component Failure (right)\n'
        'Fault t=120s; Restart t=180s (Central Component only)',
        fontsize=11)

    configs = [
        ('flink_single',  'Flink — Single Node Failure',       FLINK_COLOR, 0, 0, [120], None),
        ('spark_single',  'Spark — Single Node Failure',        SPARK_COLOR, 1, 0, [120], None),
        ('flink_central', 'Flink — Central Component Failure',  FLINK_COLOR, 0, 1, [120], 180),
        ('spark_central', 'Spark — Central Component Failure',  SPARK_COLOR, 1, 1, [120], 180),
    ]

    for key, title, color, row, col, faults, restart in configs:
        ax = axes[row][col]
        if key not in dfs:
            ax.text(0.5, 0.5, 'Data not available', ha='center', transform=ax.transAxes)
            continue
        df = dfs[key]
        ax.plot(df['elapsed_seconds'], df['throughput'], color=color, lw=1.2, label='Throughput')
        ax.plot(df['elapsed_seconds'], df['load_rate'],  color=LOAD_COLOR, lw=1.0, ls='--', label='Offered Load', alpha=0.7)
        add_fault_lines(ax, faults, restart)
        ymax = df['throughput'].max()
        ax.text(125, ymax*0.7, 'Fault\nt=120s', fontsize=7, color=FAULT_COLOR)
        if restart:
            ax.text(185, ymax*0.4, 'Restart\nt=180s', fontsize=7, color=RESTART_COLOR)
        ax.set_title(title, fontsize=9, loc='left')
        ax.set_ylabel('msg/s')
        ax.set_xlabel('Elapsed (s)')
        ax.legend(fontsize=8, loc='upper right')
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
    fig, axes = plt.subplots(2, 3, figsize=(16, 8))
    fig.suptitle(
        'Figure 4.3. Latency Evolution — Flink and Spark across Three Fault Scenarios\n'
        'Drop to 0ms in Spark = idle batch interval (not actual zero latency)',
        fontsize=11)

    configs = [
        ('flink_dynamic',  'Flink — Multi-Fault',        FLINK_COLOR, 0, 0),
        ('flink_single',   'Flink — Single Node',         FLINK_COLOR, 0, 1),
        ('flink_central',  'Flink — Central Component',   FLINK_COLOR, 0, 2),
        ('spark_dynamic',  'Spark — Multi-Fault *',       SPARK_COLOR, 1, 0),
        ('spark_single',   'Spark — Single Node *',       SPARK_COLOR, 1, 1),
        ('spark_central',  'Spark — Central Component *', SPARK_COLOR, 1, 2),
    ]

    for key, title, color, row, col in configs:
        ax = axes[row][col]
        if key not in dfs: continue
        df = dfs[key]
        ax.plot(df['elapsed_seconds'], df['latency'], color=color, lw=0.9, alpha=0.85)
        avg = df.loc[df['latency']>0, 'latency'].mean()
        ax.axhline(avg, color='gray', ls='--', lw=0.8, alpha=0.7)
        ax.text(df['elapsed_seconds'].max()*0.05, avg*1.08,
                f'avg={avg:.1f}ms', fontsize=7, color='gray')
        ax.set_title(title, fontsize=9)
        ax.set_ylabel('Latency (ms)')
        ax.set_xlabel('Elapsed (s)')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig43_latency.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.4 — Kafka Lag Proxy
# ============================================================
def fig44():
    fig, axes = plt.subplots(2, 3, figsize=(16, 8))
    fig.suptitle(
        'Figure 4.4. Kafka Lag Proxy Evolution — Flink and Spark across Three Fault Scenarios\n'
        'Synthetic proxy: cumulative load_rate − throughput per second',
        fontsize=11)

    configs = [
        ('flink_dynamic',  'Flink — Multi-Fault',        FLINK_COLOR, 0, 0),
        ('flink_single',   'Flink — Single Node',         FLINK_COLOR, 0, 1),
        ('flink_central',  'Flink — Central Component',   FLINK_COLOR, 0, 2),
        ('spark_dynamic',  'Spark — Multi-Fault',         SPARK_COLOR, 1, 0),
        ('spark_single',   'Spark — Single Node',         SPARK_COLOR, 1, 1),
        ('spark_central',  'Spark — Central Component',   SPARK_COLOR, 1, 2),
    ]

    for key, title, color, row, col in configs:
        ax = axes[row][col]
        if key not in dfs: continue
        df = dfs[key]
        ax.fill_between(df['elapsed_seconds'], df['kafka_lag'], color=color, alpha=0.3)
        ax.plot(df['elapsed_seconds'], df['kafka_lag'], color=color, lw=0.9)
        peak     = df['kafka_lag'].max()
        peak_idx = df['kafka_lag'].idxmax()
        peak_t   = df.loc[peak_idx, 'elapsed_seconds']
        ax.annotate(f'peak={peak:,.0f}\nt={peak_t:.0f}s',
                    xy=(peak_t, peak), xytext=(peak_t+30, peak*0.75),
                    fontsize=7, color='darkred',
                    arrowprops=dict(arrowstyle='->', color='darkred', lw=0.7))
        ax.set_title(title, fontsize=9)
        ax.set_ylabel('Kafka Lag (records)')
        ax.set_xlabel('Elapsed (s)')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig44_kafka_lag.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.5 — Resource Usage
# ============================================================
def fig45():
    fig, axes = plt.subplots(4, 3, figsize=(16, 13))
    fig.suptitle(
        'Figure 4.5. CPU Utilization (rows 1–2) and Memory (rows 3–4)\n'
        'Flink (rows 1 & 3) and Spark (rows 2 & 4) across Three Scenarios',
        fontsize=11)

    # Flink CPU: row 0, Flink Mem: row 1, Spark CPU: row 2, Spark Mem: row 3
    order = [
        ('flink_dynamic', 'Flink Multi-Fault',   FLINK_COLOR, 0),
        ('flink_single',  'Flink Single Node',    FLINK_COLOR, 1),
        ('flink_central', 'Flink Central Comp',   FLINK_COLOR, 2),
        ('spark_dynamic', 'Spark Multi-Fault',    SPARK_COLOR, 0),
        ('spark_single',  'Spark Single Node',    SPARK_COLOR, 1),
        ('spark_central', 'Spark Central Comp',   SPARK_COLOR, 2),
    ]

    for i, (key, label, color, col) in enumerate(order):
        r_cpu = 0 if i < 3 else 2
        r_mem = 1 if i < 3 else 3
        if key not in dfs: continue
        df = dfs[key]

        for r, metric, ylabel in [(r_cpu, 'cpu', 'CPU (%)'), (r_mem, 'memory', 'Memory (%)')]:
            ax = axes[r][col]
            ax.plot(df['elapsed_seconds'], df[metric], color=color, lw=0.9)
            ax.fill_between(df['elapsed_seconds'], df[metric], color=color, alpha=0.15)
            avg = df[metric].mean()
            ax.axhline(avg, color='gray', ls='--', lw=0.7)
            ax.text(10, avg+2, f'avg={avg:.1f}%', fontsize=7, color='gray')
            ax.set_title(f'{label} — {ylabel}', fontsize=8)
            ax.set_ylabel(ylabel, fontsize=8)
            ax.set_ylim(0, 100)
            ax.set_xlabel('Elapsed (s)', fontsize=8)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig45_resource.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.6, 4.7, 4.8 — Master Timeline (3 scenarios)
# ============================================================
def master_timeline(scenario_key_flink, scenario_key_spark, fault_times, restart_time,
                    title, fignum, fault_labels, out_fname):
    fig, axes = plt.subplots(3, 2, figsize=(16, 11), sharex='col')
    fig.suptitle(title, fontsize=11)

    metrics = [
        ('throughput', 'Throughput (msg/s)'),
        ('latency',    'Latency (ms)'),
        ('kafka_lag',  'Kafka Lag (records)'),
    ]

    for col, (key, label, color) in enumerate([
        (scenario_key_flink, 'Apache Flink',                  FLINK_COLOR),
        (scenario_key_spark, 'Apache Spark Structured Streaming', SPARK_COLOR),
    ]):
        if key not in dfs: continue
        df = dfs[key]
        for row, (metric, ylabel) in enumerate(metrics):
            ax = axes[row][col]
            ax.plot(df['elapsed_seconds'], df[metric], color=color, lw=1.0)
            if metric == 'throughput':
                ax.plot(df['elapsed_seconds'], df['load_rate'],
                        color=LOAD_COLOR, lw=0.8, ls='--', alpha=0.6, label='Offered Load')
                ax.set_title(label, fontsize=10, fontweight='bold')
            add_fault_lines(ax, fault_times, restart_time)
            ax.set_ylabel(ylabel, fontsize=9)
            ax.set_ylim(bottom=0)

        # Annotate fault events on top row — stagger y position when events are close
        ylim_top = axes[0][col].get_ylim()[1]
        y_levels = [ylim_top * 0.85, ylim_top * 0.65]   # alternating heights
        for idx, (ft, flabel) in enumerate(zip(fault_times, fault_labels)):
            y_pos = y_levels[idx % len(y_levels)]
            axes[0][col].text(ft + 5, y_pos, flabel, fontsize=7, color=FAULT_COLOR,
                              ha='left', va='center')
        if restart_time:
            axes[0][col].text(restart_time + 5, ylim_top * 0.15, 'Restart',
                              fontsize=7, color=RESTART_COLOR, ha='left')

    for col in range(2):
        axes[2][col].set_xlabel('Elapsed Time (seconds)')

    patches = [mpatches.Patch(color=FAULT_COLOR, label='Fault event')]
    if restart_time:
        patches.append(mpatches.Patch(color=RESTART_COLOR, label='Restart'))
    fig.legend(handles=patches, loc='lower center', ncol=2, fontsize=9, bbox_to_anchor=(0.5, 0.01))

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    out = f'{OUTPUT_DIR}/{out_fname}'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

def fig46():
    master_timeline(
        'flink_dynamic', 'spark_dynamic',
        fault_times=[300, 330], restart_time=660,
        title='Figure 4.6. Master Timeline — Dynamic Load + Multi-Fault Cascading\n'
              'Throughput, Latency, Kafka Lag with fault event annotations',
        fignum=6,
        fault_labels=['Switchover\nWorker2', 'Kill\nWorker1'],
        out_fname='fig46_master_timeline_multifault.png'
    )

def fig47():
    master_timeline(
        'flink_single', 'spark_single',
        fault_times=[120], restart_time=None,
        title='Figure 4.7. Master Timeline — Single Node Failure\n'
              'Throughput, Latency, Kafka Lag (fault t=120s, no restart)',
        fignum=7,
        fault_labels=['Kill Worker1'],
        out_fname='fig47_master_timeline_single.png'
    )

def fig48():
    master_timeline(
        'flink_central', 'spark_central',
        fault_times=[120], restart_time=180,
        title='Figure 4.8. Master Timeline — Central Component Failure\n'
              'Throughput, Latency, Kafka Lag (fault t=120s, restart t=180s)',
        fignum=8,
        fault_labels=['Kill Master'],
        out_fname='fig48_master_timeline_central.png'
    )

# ============================================================
# FIGURE 4.9 — Stabilization Interval Bar Chart
# ============================================================
def fig49():
    scenarios  = ['Multi-Fault\nCascading', 'Single Node\nFailure', 'Central Component\nFailure']
    flink_vals = [4.76, 4.40, 5.19]
    spark_vals = [8.73, 11.27, 22.91]
    ratios     = [s/f for f, s in zip(flink_vals, spark_vals)]
    x = np.arange(len(scenarios))
    w = 0.32

    fig, ax = plt.subplots(figsize=(10, 6))
    b1 = ax.bar(x - w/2, flink_vals, w, label='Apache Flink',  color=FLINK_COLOR, alpha=0.85)
    b2 = ax.bar(x + w/2, spark_vals, w, label='Apache Spark',  color=SPARK_COLOR, alpha=0.85)

    for bar in b1:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
                f'{bar.get_height():.2f}s', ha='center', va='bottom', fontsize=9, fontweight='bold')
    for bar in b2:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
                f'{bar.get_height():.2f}s', ha='center', va='bottom', fontsize=9, fontweight='bold')
    for i, (ratio, sv) in enumerate(zip(ratios, spark_vals)):
        ax.text(i, sv+1.8, f'Flink {ratio:.1f}x faster',
                ha='center', fontsize=8, color='darkgreen', fontweight='bold')

    ax.set_xticks(x); ax.set_xticklabels(scenarios, fontsize=10)
    ax.set_ylabel('Stabilization Interval (seconds)')
    ax.set_title('Figure 4.9. Threshold-based Stabilization Interval — Flink vs Spark\n'
                 'Threshold: throughput ≥90% baseline, stable ≥5 consecutive samples', fontsize=11)
    ax.legend(fontsize=10)
    ax.set_ylim(0, 28)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig49_stabilization.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.10 — Data Loss
# ============================================================
def fig410():
    scenarios  = ['Multi-Fault\nCascading', 'Single Node\nFailure', 'Central Component\nFailure']
    flink_vals = [44.4, 51.7, 79.0]
    spark_vals = [88.5, 93.8, 83.5]
    x = np.arange(len(scenarios))
    w = 0.32

    fig, ax = plt.subplots(figsize=(10, 6))
    b1 = ax.bar(x - w/2, flink_vals, w, label='Apache Flink',  color=FLINK_COLOR, alpha=0.85)
    b2 = ax.bar(x + w/2, spark_vals, w, label='Apache Spark',  color=SPARK_COLOR, alpha=0.85)

    for bar in [*b1, *b2]:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.5,
                f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.set_xticks(x); ax.set_xticklabels(scenarios, fontsize=10)
    ax.set_ylabel('Estimated Data Loss (%)')
    ax.set_title('Figure 4.10. Estimated Data Loss — Numerical Integral Reconciliation\n'
                 'max(0, sum(load_rate*dt) − sum(throughput*dt)) / sum(load_rate*dt) × 100%', fontsize=11)
    ax.legend(fontsize=10)
    ax.set_ylim(0, 105)
    ax.axhline(100, color='red', ls='--', lw=0.8, alpha=0.4)

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/fig410_dataloss.png'
    plt.savefig(out, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  OK: {out}")

# ============================================================
# FIGURE 4.11 — Summary Comparison
# ============================================================
def fig411():
    labels = ['Multi-Fault', 'Single Node', 'Central Comp']
    x = np.arange(len(labels))
    w = 0.32

    flink_tp  = [196.2, 208.6, 91.1]
    spark_tp  = [24.6,  26.0,  71.5]
    flink_lat = [103.7, 121.7, 133.6]
    spark_lat = [127.2, 242.6, 29.3]
    flink_cpu = [10.6,  12.9,  13.0]
    spark_cpu = [23.9,  27.0,  15.5]

    fig, axes = plt.subplots(1, 3, figsize=(15, 6))
    fig.suptitle('Figure 4.11. Throughput, Latency, and CPU Comparison\n'
                 'All Framework × Fault Scenario Combinations', fontsize=12)

    for ax, fv, sv, ylabel, title, note in [
        (axes[0], flink_tp,  spark_tp,  'Avg Throughput (msg/s)', 'Throughput', ''),
        (axes[1], flink_lat, spark_lat, 'Avg Latency (ms)',        'Latency',    '* Spark = proxy batch interval'),
        (axes[2], flink_cpu, spark_cpu, 'Avg CPU Usage (%)',       'CPU Usage',  ''),
    ]:
        b1 = ax.bar(x - w/2, fv, w, label='Apache Flink',  color=FLINK_COLOR, alpha=0.85)
        b2 = ax.bar(x + w/2, sv, w, label='Apache Spark',  color=SPARK_COLOR, alpha=0.85)
        for bar in [*b1, *b2]:
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.5,
                    f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=7.5)
        ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylabel(ylabel)
        ax.set_title(f'{title}\n{note}', fontsize=10)
        ax.legend(fontsize=8)
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
    print('=' * 55)
    print('Thesis Rizky Zein Adam — Chapter 4 Chart Generator')
    print(f'Output: {OUTPUT_DIR}')
    print('=' * 55)
    print()

    tasks = [
        ('Figure 4.1  — Throughput Multi-Fault',          fig41),
        ('Figure 4.2  — Throughput Single + Central',      fig42),
        ('Figure 4.3  — Latency Three Scenarios',          fig43),
        ('Figure 4.4  — Kafka Lag Proxy',                  fig44),
        ('Figure 4.5  — Resource Usage CPU & Memory',      fig45),
        ('Figure 4.6  — Master Timeline Multi-Fault',      fig46),
        ('Figure 4.7  — Master Timeline Single Node',      fig47),
        ('Figure 4.8  — Master Timeline Central Comp',     fig48),
        ('Figure 4.9  — Stabilization Interval Bar',       fig49),
        ('Figure 4.10 — Data Loss Bar',                    fig410),
        ('Figure 4.11 — Summary Comparison',               fig411),
    ]

    for name, func in tasks:
        print(f'{name}...')
        try:
            func()
        except Exception as e:
            print(f"  ERROR: {e}")

    print()
    print('=' * 55)
    print('DONE — Files generated:')
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith('.png'):
            size = os.path.getsize(f'{OUTPUT_DIR}/{f}') // 1024
            print(f'  {f} ({size} KB)')
    print('=' * 55)
