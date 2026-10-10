#!/usr/bin/env python3
"""Regenerasi Figure 1-4 pada ukuran cetak: lebar 6,10 in (= lebar teks A4 di docx,
margin kiri 3,0 cm + kanan 2,5 cm), dpi 300, font 8-9 pt.

Sesuai catatan reviewer 10 Okt 2026: gambar lama dibuat besar (10-16 in) lalu diperkecil
ke ~4,1-5,6 in di halaman, sehingga teks di dalam gambar jatuh ke 3-4 pt.
Gambar baru: ukuran sumber = ukuran tampil, jadi dpi efektif = 300 dan ukuran font di
gambar = ukuran font di cetakan.

Perubahan isi (hanya satu, disetujui sebagai koreksi inkonsistensi):
  Figure 1 panel "Central Component Failure AFTER": task manager tetap HIJAU/aktif
  (yang dimatikan hanya koordinator), dan teks "no one processing" diganti menjadi
  perilaku yang sebenarnya dilaporkan di Bagian 3.3.
"""
import importlib.util
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle, Patch

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
candidates = [
    PROJECT_ROOT / "scripts" / "generate_charts_bab4_3x.py",
    PROJECT_ROOT / "generate_charts_bab4_3x.py",
]
spec = None
for c in candidates:
    if c.is_file():
        spec = importlib.util.spec_from_file_location('chartmod', str(c))
        break
if spec is None:
    sys.exit("ERROR: generate_charts_bab4_3x.py tidak ditemukan.")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

W = 6.10          # lebar teks halaman (in)
DPI = 300
OUTDIR = os.environ.get("FIGS_PRINT_DIR", str(PROJECT_ROOT / "results" / "figs_print"))
os.makedirs(OUTDIR, exist_ok=True)
RESULTS_DIR = str(PROJECT_ROOT / "results")
DATA_DIR = str(PROJECT_ROOT / "results")

matplotlib.rcParams.update({
    'font.size': 8,
    'axes.labelsize': 8.5,
    'axes.titlesize': 9,
    'xtick.labelsize': 8,
    'ytick.labelsize': 8,
    'legend.fontsize': 7.5,
    'font.family': 'DejaVu Sans',
})


# Figure 1 punya generator sendiri: make_fig1_scenarios.py
# (layout 3x2 butuh tinggi 5,3 in supaya font 7-9 pt muat di dalam kotak)


# ======================== FIGURE 2 & 3 helper ========================
def two_panel(key, ymax, fname, faultlabels, restart_s):
    data = m.load_all(RESULTS_DIR, DATA_DIR)
    scn_cfg = m.SCENARIOS['multi']
    fig, axes = plt.subplots(2, 1, figsize=(W, 3.55), sharex=True)
    for ax, fw in zip(axes, m.FRAMEWORKS):
        cfg = m.FRAMEWORKS[fw]
        rows_list = [data['multi'][fw]['rep%d' % r] for r in m.REPS]
        for i, rs in enumerate(rows_list):
            ax.plot([r['t'] for r in rs], [r[key] for r in rs], color=cfg['color'], lw=0.7, alpha=0.32,
                    label='Replications (3)' if i == 0 else None)
        ts, means, stds = m.mean_std_bins(rows_list, m.metric_value_fn(key))
        ax.plot(ts, means, color=m.MEAN_COLOR, lw=1.4, label='Mean (3 replications)')
        ax.fill_between(ts, [x - s for x, s in zip(means, stds)], [x + s for x, s in zip(means, stds)],
                        color=m.MEAN_COLOR, alpha=m.STD_ALPHA, label='Mean and standard deviation')
        if key == 'tp':
            ax.plot([r['t'] for r in rows_list[0]], [r['load'] for r in rows_list[0]],
                    color=m.LOAD_COLOR, lw=0.9, ls=(0, (4, 2, 1, 2)), alpha=0.85, label='Offered load')
        m.add_fault_lines(ax, scn_cfg)
        ax.set_ylim(0, ymax)
        for ft, lab, frac, ha in faultlabels:
            ax.text(ft, ymax * frac, lab, fontsize=7, color=m.FAULT_COLOR, ha=ha, va='center', linespacing=1.1)
        ax.text(restart_s, ymax * 0.79, 'Restart\nWorker1 (t=660)', fontsize=7, color=m.RESTART_COLOR,
                ha='left', va='center', linespacing=1.1)
        ax.set_ylabel(m.METRIC_LABELS[key])
        ax.set_title(cfg['label'], fontsize=8.5, loc='left', pad=3)
        ax.tick_params(labelsize=7.5)
    axes[-1].set_xlabel('Time (s)')
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc='lower center', bbox_to_anchor=(0.5, 0.005), ncol=4, fontsize=7,
               frameon=False, handlelength=1.8, columnspacing=1.2, handletextpad=0.5)
    fig.subplots_adjust(top=0.945, bottom=0.235, left=0.105, right=0.985, hspace=0.20)
    out = '%s/%s' % (OUTDIR, fname)
    fig.savefig(out, dpi=DPI, facecolor='white')
    plt.close(fig)
    print('%s: %s' % (fname, plt.imread(out).shape))


two_panel('tp', 2000.0, 'fig2_throughput_print.png',
          [(296, 'Switchover\nWorker2 (t=300)', 0.93, 'right'), (334, 'Kill\nWorker1 (t=330)', 0.72, 'left')], 663)
two_panel('lat', 1000.0, 'fig3_latency_print.png',
          [(296, 'Switchover\nWorker2 (t=300)', 0.94, 'right'), (334, 'Kill\nWorker1 (t=330)', 0.72, 'left')], 663)


# ============================ FIGURE 4 ============================
import csv
import glob
import statistics as st


def load(path):
    rows = []
    for r in csv.DictReader(open(path)):
        try:
            rows.append((float(r['elapsed_seconds']), float(r['load_rate']), float(r['throughput'])))
        except ValueError:
            continue
    rows.sort()
    return rows


def sample(rows):
    boxes = {}
    for t, load, tp in rows:
        k = int(round(t))
        boxes.setdefault(k, {'load': [], 'tp': []})
        boxes[k]['load'].append(load)
        boxes[k]['tp'].append(tp)
    ts = sorted(boxes)
    # stdev = SD sampel (n-1), konvensi yang sama dengan tabel (dan dengan np.std(ddof=1)
    # di panel Figure 2/3). pstdev = SD populasi membuat pita lebih sempit ~18% pada n = 3.
    return (ts, [st.mean(boxes[t]['load']) for t in ts], [st.mean(boxes[t]['tp']) for t in ts],
            [st.stdev(boxes[t]['tp']) if len(boxes[t]['tp']) > 1 else 0.0 for t in ts])


FAULT = (120.0, 180.0)
flink_files = sorted(glob.glob(os.path.join(DATA_DIR, 'central_3x', 'flink_*.csv'))) or sorted(glob.glob(os.path.join(RESULTS_DIR, 'flink_central_rep*.csv')))
spark_files = sorted(glob.glob(os.path.join(DATA_DIR, 'central_3x', 'spark_*.csv'))) or sorted(glob.glob(os.path.join(RESULTS_DIR, 'spark_central_rep*.csv')))
flink = sample([r for f in flink_files for r in load(f)])
spark = sample([r for f in spark_files for r in load(f)])

fig = plt.figure(figsize=(W, 3.30))
ax = fig.add_axes([0.105, 0.235, 0.875, 0.70])
ax.axvspan(FAULT[0], FAULT[1], color='#F2F2F2', zorder=0)
ax.text(sum(FAULT) / 2, 1035, 'fault active\n(killed component down)', ha='center', va='top',
        fontsize=7, color='#777777', linespacing=1.2, zorder=1)
ax.plot(flink[0], flink[1], color='#555555', lw=1.1, ls=(0, (4, 2, 1, 2)), label='Offered load', zorder=2)
ts_s, ld_s, tp_s, sd_s = spark
ts_sp, tp_sp = [], []
for t, tp in zip(ts_s, tp_s):
    if FAULT[0] <= t <= FAULT[1]:
        ts_sp.append(t); tp_sp.append(float('nan'))
    else:
        ts_sp.append(t); tp_sp.append(tp)
ax.plot([], [], color='#FF9232', lw=1.7, linestyle=(0, (6, 2)), label='Apache Spark (mean of 3 replications)')
ax.plot(ts_sp, tp_sp, color='#FF9232', lw=1.7, linestyle=(0, (6, 2)), alpha=0.95, zorder=3)
ts_f, ld_f, tp_f, sd_f = flink
ax.plot(ts_f, tp_f, color='#DC4748', lw=1.4, label='Apache Flink (mean of 3 replications)', zorder=4)
ax.fill_between(ts_f, [v - s for v, s in zip(tp_f, sd_f)], [v + s for v, s in zip(tp_f, sd_f)],
                color='#DC4748', alpha=0.18, zorder=3)
ax.annotate('Flink workers keep consuming:\ndips to about two-thirds of the\noffered load, then climbs back',
            xy=(146, 245), xytext=(150, 690), fontsize=7.5, color='#DC4748', ha='center',
            arrowprops=dict(arrowstyle='->', color='#DC4748', lw=1.0), linespacing=1.25, zorder=6)
ax.annotate('Spark driver down: query stops,\nno metric available for 60 s\n(series interrupted)',
            xy=(150, 25), xytext=(200, 130), fontsize=7.5, color='#B36B00', ha='left',
            arrowprops=dict(arrowstyle='->', color='#B36B00', lw=1.0), linespacing=1.25, zorder=6)
ax.set_xlabel('Time (s)', fontsize=8.5)
ax.set_ylabel('Throughput (msg/s)', fontsize=8.5)
ax.set_xlim(0, 600); ax.set_ylim(0, 1050)
ax.set_yticks(range(0, 1001, 200))
ax.tick_params(labelsize=7.5)
ax.grid(True, color='#EBEBEB', lw=0.7)
ax.set_axisbelow(True)
ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.34), ncol=3, fontsize=7, frameon=False,
          handlelength=1.8, columnspacing=1.2, handletextpad=0.5)
out = '%s/fig4_central_print.png' % OUTDIR
fig.savefig(out, dpi=DPI, facecolor='white')
plt.close(fig)
print('fig4:', plt.imread(out).shape)
print('semua gambar disimpan di', OUTDIR)
