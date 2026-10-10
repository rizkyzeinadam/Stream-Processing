#!/usr/bin/env python3
"""Figure 1 (skenario) — ukuran cetak: lebar 6,10 in (lebar teks), tinggi 5,30 in, dpi 300.

Perbaikan v5 (dari temuan bangbro atas render v4):
  1. Garis pemisah vertikal dulu di x=0,517 padahal panel kiri membentang 0,135-0,560,
     jadi garis itu memotong label "Output"/"Kafka" milik kolom kiri. Kolom juga saling
     tumpang (0,545 < 0,560). Kolom kini tidak tumpang dan garis pemisah diletakkan
     tepat di gutter antar kolom.
  2. Warna bar Kafka: bagian "kosong" ikut merah karena syarat `RED if healthy < 0.5`
     selalu benar. Akibatnya bar hampir penuh merah bahkan di kondisi Normal.
     Sekarang bar = latar krem (kapasitas antrean) + isian hijau (Normal) / merah (Fault),
     jadi merah benar-benar berarti backlog dan krem (legenda "Kafka queue") terlihat.
  3. Teks status dan judul merah terlalu rapat (jarak center 7 unit = ~6 pt < tinggi huruf).
     Kini diberi jarak dan baris dinaikkan agar tidak bertumpuk.
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle, Patch

import os
from pathlib import Path

W, H, DPI = 6.10, 5.30, 300
PROJECT_ROOT = Path(__file__).resolve().parent
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
OUTDIR = os.environ.get("FIGS_PRINT_DIR", str(PROJECT_ROOT / "results" / "figs_print"))
os.makedirs(OUTDIR, exist_ok=True)
OUT = os.path.join(OUTDIR, "fig1_scenarios_print.png")

matplotlib.rcParams.update({'font.family': 'DejaVu Sans'})

GREEN_BG, GREEN_ED = '#C7E9C0', '#2CA02C'
GRAY_BG, GRAY_ED = '#E0E0E0', '#7F7F7F'
KAFKA_BG, KAFKA_ED = '#FFF3B0', '#B8860B'
RED = '#C00000'

BAR_Y, BAR_H = 24.0, 9.0          # posisi/tebal bar Kafka (data units)
OCC_NORMAL, OCC_FAULT = 0.22, 0.78  # isian bar: kecil saat normal, besar saat backlog


def fbox(ax, x, y, w, h, fc, ec, text, fs, tc='#111111', bold=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0.4,rounding_size=1.4',
                                facecolor=fc, edgecolor=ec, lw=0.9, zorder=3))
    ax.text(x + w / 2, y + h / 2, text, fontsize=fs, ha='center', va='center', color=tc,
            fontweight='bold' if bold else 'normal', zorder=4, linespacing=1.2)


def red_x(ax, cx, cy, s=2.0):
    ax.plot([cx - s, cx + s], [cy - s, cy + s], color=RED, lw=1.7, zorder=6)
    ax.plot([cx - s, cx + s], [cy + s, cy - s], color=RED, lw=1.7, zorder=6)


def arrow(ax, x0, y0, x1, y1, color='#333333', lw=1.0, ls='-'):
    ax.add_patch(FancyArrowPatch((x0, y0), (x1, y1), arrowstyle='-|>', mutation_scale=7,
                                 color=color, lw=lw, zorder=2, linestyle=ls))


def kafka_bar(ax, occ, fault):
    """Latar krem = kapasitas antrean; isian = tingkat antrean (hijau normal, merah backlog)."""
    ax.add_patch(Rectangle((3, BAR_Y), 94, BAR_H, facecolor=KAFKA_BG, edgecolor='none', zorder=2))
    ax.add_patch(Rectangle((3, BAR_Y), 94 * occ, BAR_H,
                           facecolor=RED if fault else GREEN_ED, edgecolor='none', zorder=2))
    ax.add_patch(Rectangle((3, BAR_Y), 94, BAR_H, facecolor='none',
                           edgecolor='#999999', lw=0.7, zorder=3))


def panel(ax, coord_dead, w1_dead, occ, fault, status, headline, headline_color):
    ax.set_xlim(0, 100); ax.set_ylim(0, 100)
    ax.set_xticks([]); ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)

    if coord_dead:
        fbox(ax, 24, 70, 52, 18, GRAY_BG, GRAY_ED, 'JobManager / Driver', 8, tc='#555555')
        red_x(ax, 77.5, 86.5, s=2.0)
    else:
        fbox(ax, 24, 70, 52, 18, GREEN_BG, GREEN_ED, 'JobManager / Driver', 8, bold=True)

    for wx, dead, lbl in [(3, w1_dead, 'TaskManager 1\nWorker 1'), (46, False, 'TaskManager 2\nWorker 2')]:
        if dead:
            fbox(ax, wx, 42, 39, 20, GRAY_BG, GRAY_ED, lbl, 7.5, tc='#555555')
            red_x(ax, wx + 35.5, 60.5, s=2.0)
        else:
            fbox(ax, wx, 42, 39, 20, GREEN_BG, GREEN_ED, lbl, 7.5)

    # output (label ditarik ke kiri supaya tidak menyentuh garis pemisah tengah)
    arrow(ax, 85, 52, 88, 52, color=GREEN_ED, lw=0.9)
    ax.text(99, 52, 'Output', fontsize=6.5, color='#555555', ha='right', va='center')

    # bar Kafka + label di tengah atas bar
    kafka_bar(ax, occ, fault)
    ax.text(50, BAR_Y + BAR_H + 1.0, 'Kafka', fontsize=6.5, color='#333333', ha='center', va='bottom')

    for wx, dead in [(3, w1_dead), (46, False)]:
        if not dead:
            arrow(ax, wx + 19.5, BAR_Y + BAR_H, wx + 19.5, 42, color=GREEN_ED, lw=0.9)
    if not coord_dead:
        arrow(ax, 42, 70, 24, 62, color='#555555', lw=0.8, ls=':')
        arrow(ax, 58, 70, 65.5, 62, color='#555555', lw=0.8, ls=':')

    ax.text(50, 14.5, status, fontsize=6.3, color='#666666', ha='center', va='center')
    ax.text(50, 4.5, headline, fontsize=6.8, color=headline_color, ha='center', va='center',
            fontweight='bold', linespacing=1.15)


fig = plt.figure(figsize=(W, H))
ROWS = [(0.700, 0.948), (0.398, 0.646), (0.096, 0.344)]
COLS = [(0.115, 0.415), (0.555, 0.415)]     # tidak tumpang; gutter 0,530-0,555
DIVIDER_X = 0.5425

STATUS = [('healthy (t < 120 s)', 'backlog grows; partitions reassigned'),
          ('healthy (t < 120 s)', 'Flink keeps consuming; Spark stops'),
          ('healthy (t < 300 s)', 'load 300-1000 msg/s; restart t=660 s')]
HEAD = [('Normal - t < 120 s', 'Fault t=120 s - kill Worker 1 (stays down)'),
        ('Normal - t < 120 s', 'Fault t=120 s, restart t=180 s (60 s down)'),
        ('Normal - t < 300 s', 'Switchover t=300 s, then kill Worker 1 at t=330 s')]

for r, (y0, y1) in enumerate(ROWS):
    for c, (x0, w) in enumerate(COLS):
        fault = (c == 1)
        panel(fig.add_axes([x0, y0, w, y1 - y0]),
              coord_dead=(r == 1 and c == 1),
              w1_dead=(fault and r in (0, 2)),
              occ=OCC_FAULT if fault else OCC_NORMAL,
              fault=fault,
              status=STATUS[r][c], headline=HEAD[r][c],
              headline_color=RED if fault else GREEN_ED)

for i, lbl in enumerate(['Single-Node\nFailure', 'Central\nComponent\nFailure', 'Multi-Fault\nCascading']):
    fig.text(0.012, (ROWS[i][0] + ROWS[i][1]) / 2, lbl, fontsize=7.5, fontweight='bold',
             ha='left', va='center', linespacing=1.3)
for x, t in [(0.33, 'BEFORE - Normal'), (0.76, 'AFTER - Fault')]:
    fig.text(x, 0.970, t, fontsize=9, fontweight='bold', ha='center', va='center')
for y in [0.673, 0.371]:
    fig.add_artist(plt.Line2D([0.01, 0.995], [y, y], color='#BBBBBB', lw=0.6))
fig.add_artist(plt.Line2D([DIVIDER_X, DIVIDER_X], [0.070, 0.955], color='#BBBBBB', lw=0.6))
fig.legend(handles=[Patch(facecolor=GREEN_BG, edgecolor=GREEN_ED, label='Active component'),
                    Patch(facecolor=GRAY_BG, edgecolor=GRAY_ED, label='Down component'),
                    Patch(facecolor=KAFKA_BG, edgecolor=KAFKA_ED, label='Kafka queue'),
                    Patch(facecolor=RED, edgecolor=RED, label='Backlog grows')],
           loc='lower left', bbox_to_anchor=(0.008, 0.005), ncol=4, fontsize=6.5, frameon=False,
           handlelength=1.3, columnspacing=1.0, handletextpad=0.45)
fig.savefig(OUT, dpi=DPI, facecolor='white')
print('fig1:', plt.imread(OUT).shape, '-> %.2f x %.2f in @ %d dpi' % (W, H, DPI))
