#!/usr/bin/env python3
"""Reproduksi metrik recovery stream-processing dari CSV collector mentah.

Dipakai untuk kasus kelas "artefak fallback throughput" (lihat
references/collector-fallback-artifact.md). Skrip ini mendefinisikan ulang ketiga metrik
secara eksplisit supaya angka naskah/paper bisa diperiksa orang lain tanpa notebook asli.

DEFINISI (harus sama dengan yang ditulis di metode naskah)
---------------------------------------------------------
1. Recovery stabilization interval
   T1 = sampel pertama pada atau setelah jadwal injeksi yang throughput-nya turun di bawah
        50% dari baseline pra-fault (baseline = rata-rata throughput 60 detik sebelum injeksi).
        Kalau seri tidak pernah turun serendah itu, T1 = waktu injeksi (anchor).
   T2 = sampel pertama dari 5 sampel berturut dengan throughput >= 90% dari beban yang
        berlaku SAAT injeksi. Interval = T2 - T1. Karena fault yang diinjeksi tetap aktif
        sampai komponen hidup lagi, T2 hanya dihitung DARI AKHIR jendela fault aktif
        (t = 180 s di skenario central) -- kalau tidak, framework bisa terlihat "pulih"
        sebelum komponennya kembali.

2. Throughput deficit
   integral(beban - throughput) / integral(beban) sepanjang run. Sampel yang metriknya
   tidak tersedia karena proses pelapor mati HARUS dihitung 0: itu kapasitas yang tidak
   dikirim. Jendelanya = jendela fault aktif menurut JADWAL INJEKSI (120-180 s di central),
   bukan "sampai footprint container pulih" -- footprint sering tidak kembali ke nilai
   pra-fault sehingga jendela jadi terlalu panjang dan angka menggelembung.

3. Peak backlog rate
   max(beban - throughput, 0) dalam msg/s (LAJU, bukan jumlah pesan terakumulasi),
   dilaporkan untuk seluruh run DAN untuk jendela fault (perbandingan apple-to-apple).

4. Peak container memory (kolom tambahan, untuk Tabel 2 naskah)
   max(memory) per run, lalu mean + SD sampel antar replikasi.

ATURAN PEMBACAAN CSV (diperbaiki 10 Okt 2026)
---------------------------------------------
Versi sebelumnya punya tiga titik yang bisa gagal SENYAP. Semua sudah dibereskan di sini,
TANPA mengubah satu angka pun yang dilaporkan:
  a) Baris dengan nilai tak-hingga (NaN/inf) di elapsed_seconds, load_rate, atau throughput
     sekarang BENAR-BENAR dibuang (dulu docstring bilang begitu tapi float('nan') tidak
     melempar error, jadi NaN lolos dan merambat ke rata-rata baseline tanpa suara).
     Kolom memory yang NaN diperlakukan sebagai sampel hilang, bukan 0.
  b) dedup() dipindah KE DALAM load_run(), jadi interval, deficit, backlog, dan deteksi
     jendela pelapor-mati semuanya memakai seri yang sama (dulu hanya interval yang
     di-dedup).
  c) Kolom wajib divalidasi sekali di awal; kalau ada kolom hilang/nama berubah, skrip
     berhenti dengan pesan jelas (dulu semua baris dibuang diam-diam lalu metrik jadi None).

SATU IMPLEMENTASI (10 Okt 2026)
-------------------------------
recovery_breakdown() / recovery_interval(), deficit(), peak_backlog_rate(), dan
backlog_rate() di file ini adalah SATU-SATUNYA definisi metrik. Modul chart
(generate_charts_bab4_3x_en.py) mengimpornya lalu mendelegasikan -- tidak boleh ada
salinan logika kedua. Kalau ada dua salinan, angka di gambar bisa melenceng dari Tabel 1
tanpa suara; itu pernah terjadi (chart ringkasan recovery melaporkan 65.0 +- 18.0 untuk
Flink multi-fault, padahal Tabel 1 = 50.0 +- 4.8).

Pakai:
    python analysis_recovery_metrics.py --data-dir data --out recovery_metrics.csv
    python analysis_recovery_metrics.py --data-dir data --check     # cocokkan ke Tabel 1 + 2

Layout yang diharapkan: <data-dir>/<scenario>_3x/<framework>_<scenario>_rep<N>.csv
dengan kolom: elapsed_seconds, load_rate, throughput, memory (kolom lain diabaikan).
"""
import argparse
import csv
from decimal import Decimal, ROUND_HALF_UP
import glob
import math
import os
import statistics as st
import sys

SCENARIOS = {
    # skenario: (waktu injeksi, akhir jendela fault aktif untuk T2)
    'multi':   (330.0, None),
    'single':  (120.0, None),
    'central': (120.0, 180.0),
}
FRAMEWORKS = ('flink', 'spark')

# Kolom yang wajib ada di CSV. Kalau salah satu hilang, lebih baik berhenti daripada
# diam-diam membuang seluruh baris dan melaporkan metrik None.
REQUIRED_COLS = ('elapsed_seconds', 'load_rate', 'throughput', 'memory')


def f1(v, nd=1):
    """Pembulatan setengah-ke-atas pada digit terakhir, sama seperti angka di naskah.

    round()/%.1f Python bisa memberi digit terakhir yang berbeda pada nilai tepat di
    batas (mis. 22.750 -> 22.7 vs 22.8, 52.846 -> 52.8 pada keduanya). Semua angka di
    naskah dibulatkan dengan aturan ini dari nilai tak terbulatkan.
    """
    q = Decimal(1).scaleb(-nd)
    return format(Decimal(repr(float(v))).quantize(q, rounding=ROUND_HALF_UP), 'f')


def _read_header(path):
    with open(path, newline='', encoding='utf-8') as fh:
        return next(csv.reader(fh), [])


def load_run(path):
    """Baca satu CSV. Baris tak-hingga dibuang, duplikat detik dibuang, urut naik.

    Mengembalikan list of dict {'t','load','tp','mem'}. 'mem' boleh None kalau kolom
    memory tidak terbaca untuk baris itu.
    """
    header = _read_header(path)
    missing = [c for c in REQUIRED_COLS if c not in header]
    if missing:
        sys.exit(f"ERROR: {path}\n  kolom wajib tidak ada: {missing}\n  header ditemukan: {header}")

    rows = []
    n_bad = 0
    for r in csv.DictReader(open(path, newline='', encoding='utf-8')):
        try:
            t = float(r['elapsed_seconds'])
            load = float(r['load_rate'])
            tp = float(r['throughput'])
        except (KeyError, TypeError, ValueError):
            n_bad += 1
            continue
        # (a) buang yang tak-hingga secara eksplisit -- float('nan') TIDAK melempar error
        if not (math.isfinite(t) and math.isfinite(load) and math.isfinite(tp)):
            n_bad += 1
            continue
        try:
            mem = float(r['memory'])
            if not math.isfinite(mem):
                mem = None
        except (KeyError, TypeError, ValueError):
            mem = None
        rows.append({'t': t, 'load': load, 'tp': tp, 'mem': mem})

    if not rows:
        sys.exit(f"ERROR: {path} tidak menghasilkan satu baris pun yang bisa dipakai "
                 f"({n_bad} baris dibuang). Header: {header}")

    rows.sort(key=lambda r: r['t'])
    return dedup(rows)          # (b) dedup di sini, supaya semua metrik pakai seri yang sama


def dedup(rows):
    """Buang sampel yang berbagi detik yang sama (elapsed bisa dobel)."""
    out, seen = [], set()
    for r in rows:
        k = int(round(r['t']))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def unmeasurable_window(rows, fault):
    """(start, end) kalau proses pelapor mati, else None.

    Buktinya footprint memori container: dia jatuh saat proses pengekspor metrik dibunuh.
    Jendelanya DIBATASI jadwal injeksi, bukan oleh pemulihan footprint.
    """
    pre = [r['mem'] for r in rows if fault - 60 <= r['t'] < fault and r['mem'] is not None]
    if not pre:
        return None
    ref = st.mean(pre)
    low = [r for r in rows if fault <= r['t'] <= fault + 60
           and r['mem'] is not None and r['mem'] < 0.5 * ref]
    return (fault, fault + 60.0) if low else None


def recovery_breakdown(rows, fault, t2_min=None):
    """SELURUH rincian metrik interval recovery. Ini SATU-SATUNYA implementasinya.

    Modul lain (mis. generate_charts_bab4_3x_en.py) WAJIB memanggil fungsi ini, bukan
    menyalin logikanya -- dua salinan = drift yang bisa kembali tanpa suara.

    Mengembalikan {'baseline','t1','t2','interval'}; 'interval' = None kalau T2 tidak
    pernah tercapai.
    """
    pre = [r['tp'] for r in rows if fault - 60 <= r['t'] < fault]
    baseline = st.mean(pre) if pre else 0.0
    load_at_fault = next((r['load'] for r in rows if r['t'] >= fault), 0.0)
    t1 = next((r['t'] for r in rows if r['t'] >= fault and r['tp'] < 0.5 * baseline), None)
    t1 = fault if t1 is None else t1
    start = fault if t2_min is None else max(fault, t2_min)
    streak, t2 = 0, None
    for r in rows:
        if r['t'] < start:
            continue
        if r['tp'] >= 0.9 * load_at_fault:
            streak += 1
            if streak >= 5:
                t2 = r['t']
                break
        else:
            streak = 0
    return {'baseline': baseline, 't1': t1, 't2': t2,
            'interval': None if t2 is None else t2 - t1}


def recovery_interval(rows, fault, t2_min=None):
    """Pembungkus tipis di atas recovery_breakdown()."""
    return recovery_breakdown(rows, fault, t2_min)['interval']


def backlog_rate(row):
    """Backlog proxy satu sampel: max(load - tp, 0) msg/s. TP tak-hingga -> 0.0."""
    if not math.isfinite(row['tp']):
        return 0.0
    return max(0.0, row['load'] - row['tp'])


def deficit(rows, zero_win):
    offered = achieved = 0.0
    for i, r in enumerate(rows):
        dt = 1.0 if i == 0 else r['t'] - rows[i - 1]['t']
        if dt > 30:                       # lubang koleksi: jangan diekstrapolasi
            dt = 1.0
        tp = 0.0 if (zero_win and zero_win[0] <= r['t'] <= zero_win[1]) else r['tp']
        offered += r['load'] * dt
        achieved += tp * dt
    return max(0.0, (1 - achieved / offered) * 100) if offered else float('nan')


def peak_backlog_rate(rows, zero_win=None, lo=None, hi=None):
    vals = []
    for r in rows:
        if lo is not None and not (lo <= r['t'] <= hi):
            continue
        tp = 0.0 if (zero_win and zero_win[0] <= r['t'] <= zero_win[1]) else r['tp']
        vals.append(max(r['load'] - tp, 0.0))
    return max(vals) if vals else float('nan')


def peak_memory(rows):
    """max(memory) pada satu run. None kalau kolom memory tidak terbaca sama sekali."""
    vals = [r['mem'] for r in rows if r['mem'] is not None]
    return max(vals) if vals else None


# Nilai sebagaimana tercetak di naskah (Tabel 1 + Tabel 2). Dipakai oleh --check.
MANUSCRIPT = {
    ('multi', 'flink'):   dict(interval_mean='50.0', interval_sd='4.8',
                               interval_runs='52.5/52.8/44.5',
                               deficit_mean='9.18', deficit_sd='1.45',
                               backlog_run='841', peakmem_mean='1391', peakmem_sd='24'),
    ('multi', 'spark'):   dict(interval_mean='35.3', interval_sd='11.4',
                               interval_runs='47.5/33.5/25.0',
                               deficit_mean='8.11', deficit_sd='0.74',
                               backlog_run='439', peakmem_mean='3914', peakmem_sd='139'),
    ('single', 'flink'):  dict(interval_mean='37.7', interval_sd='2.4',
                               interval_runs='40.5/36.3/36.3',
                               deficit_mean='10.59', deficit_sd='1.28',
                               backlog_run='275', peakmem_mean='1233', peakmem_sd='8'),
    ('single', 'spark'):  dict(interval_mean='27.5', interval_sd='9.9',
                               interval_runs='22.7/38.9/20.9',
                               deficit_mean='8.11', deficit_sd='1.66',
                               backlog_run='297', peakmem_mean='2826', peakmem_sd='29'),
    ('central', 'flink'): dict(interval_mean='68.6', interval_sd='1.2',
                               interval_runs='69.5/67.2/69.0',
                               deficit_mean='16.19', deficit_sd='2.61',
                               backlog_run='190', peakmem_mean='1233', peakmem_sd='1'),
    ('central', 'spark'): dict(interval_mean='68.4', interval_sd='1.8',
                               interval_runs='67.9/66.9/70.4',
                               deficit_mean='19.35', deficit_sd='1.43',
                               backlog_run='300', peakmem_mean='2567', peakmem_sd='164'),
}


def check_manuscript(summary):
    """Cocokkan hasil hitung dengan angka yang tercetak di Tabel 1 + Tabel 2."""
    print('\n' + '=' * 96)
    print('CEK TERHADAP NASKAH (Tabel 1 + Tabel 2)')
    print('=' * 96)
    bad = 0
    for s in summary:
        key = (s['scenario'], s['framework'])
        exp = MANUSCRIPT.get(key)
        if exp is None:
            print(f'  {key}: tidak ada acuan naskah -> dilewati')
            continue
        for field, want in exp.items():
            got = s.get(field)
            got = None if got is None else str(got)
            ok = (got == want)
            if not ok:
                bad += 1
            print(f"  {'OK  ' if ok else 'BEDA'} {s['scenario']:8s} {s['framework']:6s} "
                  f"{field:14s} naskah={want:>13s}  hitung={got}")
    print('-' * 96)
    print(f'  hasil: {"SEMUA COCOK" if bad == 0 else f"{bad} sel BEDA"}')


def resolve_data_dir(cli_dir):
    """Urutan prioritas:
    1. CLI argument --data-dir (jika diberikan)
    2. Env var RECOVERY_DATA_DIR
    3. Auto-detect di results/, data/, metrics/ (relatif terhadap folder skrip / CWD)
    """
    if cli_dir:
        if os.path.isdir(cli_dir):
            return cli_dir
        sys.exit(f"ERROR: folder data {cli_dir!r} tidak ada.")

    env_dir = os.environ.get("RECOVERY_DATA_DIR")
    if env_dir:
        if os.path.isdir(env_dir):
            return env_dir
        sys.exit(f"ERROR: RECOVERY_DATA_DIR={env_dir!r} tidak ditemukan.")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "results"),
        os.path.join(script_dir, "..", "results"),
        os.path.join(script_dir, "data"),
        os.path.join(script_dir, "..", "data"),
        os.path.join(script_dir, "metrics"),
        os.path.join(script_dir, "..", "metrics"),
        "results",
        "data",
        "metrics",
    ]
    for c in candidates:
        if os.path.isdir(c):
            # Cek apakah ada file rep CSV atau subfolder *_3x
            has_files = glob.glob(os.path.join(c, "*_rep*.csv")) or glob.glob(os.path.join(c, "*_3x", "*.csv"))
            if has_files:
                return c

    sys.exit("ERROR: Tidak dapat menemukan folder data eksperimen secara otomatis.\n"
             "Gunakan --data-dir <path> atau set environment variable RECOVERY_DATA_DIR.")


def find_scenario_paths(data_dir, scn, fw):
    """Cari file CSV untuk skenario dan framework:
    Mendukung layout:
      1. <data_dir>/<scn>_3x/<fw>_*.csv
      2. <data_dir>/<fw>_<scn>_rep*.csv
      3. <data_dir>/<scn>/<fw>_*.csv
    """
    patterns = [
        os.path.join(data_dir, f'{scn}_3x', f'{fw}_*.csv'),
        os.path.join(data_dir, f'{fw}_{scn}_rep*.csv'),
        os.path.join(data_dir, f'{fw}_{scn}_*.csv'),
        os.path.join(data_dir, scn, f'{fw}_*.csv'),
    ]
    for pat in patterns:
        paths = sorted(glob.glob(pat))
        if paths:
            return paths
    return []


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data-dir', default=None,
                    help="folder berisi data CSV (default: auto-detect)")
    ap.add_argument('--out', help='tulis tabel ringkasan ke CSV ini')
    ap.add_argument('--check', action='store_true',
                    help='cocokkan hasil dengan angka Tabel 1 + Tabel 2 naskah')
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data_dir)
    print(f"Menggunakan data directory: {data_dir}")

    summary, missing = [], 0
    for scn, (fault, t2_min) in SCENARIOS.items():
        for fw in FRAMEWORKS:
            paths = find_scenario_paths(data_dir, scn, fw)
            if not paths:
                missing += 1
                continue
            ivs, dfs, bks, bws, mems = [], [], [], [], []
            for path in paths:
                rows = load_run(path)
                win = unmeasurable_window(rows, fault)
                iv = recovery_interval(rows, fault, t2_min)
                if iv is not None:
                    ivs.append(iv)
                dfs.append(deficit(rows, win))
                bks.append(peak_backlog_rate(rows, win))
                bws.append(peak_backlog_rate(rows, win or None, fault, fault + 60.0))
                pm = peak_memory(rows)
                if pm is not None:
                    mems.append(pm)
            # 4) laporkan jumlah run yang benar-benar masuk rata-rata interval
            if len(ivs) != len(paths):
                print(f'warning: {scn}/{fw}: hanya {len(ivs)} dari {len(paths)} run punya '
                      f'interval; rata-rata dihitung dari {len(ivs)} run', file=sys.stderr)
            summary.append({
                'scenario': scn, 'framework': fw, 'runs': len(ivs),
                'files': len(paths),
                'interval_mean': f1(st.mean(ivs)) if ivs else None,
                'interval_sd': f1(st.stdev(ivs)) if len(ivs) > 1 else None,
                'interval_runs': '/'.join(f1(v) for v in ivs),
                'deficit_mean': f1(st.mean(dfs), 2),
                'deficit_sd': f1(st.stdev(dfs), 2) if len(dfs) > 1 else None,
                'backlog_run': f1(st.mean(bks), 0),
                'backlog_fault_window': f1(st.mean(bws), 1),
                'peakmem_mean': f1(st.mean(mems), 0) if mems else None,
                'peakmem_sd': f1(st.stdev(mems), 0) if len(mems) > 1 else None,
                'peakmem_runs': '/'.join(f1(v, 0) for v in mems),
            })

    if missing:
        print(f'warning: {missing} kombinasi scenario/framework tidak ada di {data_dir!r}',
              file=sys.stderr)
    if not summary:
        sys.exit(f"ERROR: tidak ada CSV yang cocok di {data_dir!r}.")

    width = 148
    print('\n' + '=' * width)
    print(f'{"scenario":9s} {"framework":10s} {"interval (s)":>18s} {"per run":>22s} '
          f'{"deficit (%)":>16s} {"backlog run":>12s} {"backlog fault":>14s} '
          f'{"peak mem (MB)":>18s} {"per run":>20s}')
    print('=' * width)
    for s in summary:
        iv_str = f"{s['interval_mean']} ± {s['interval_sd']}"
        df_str = f"{s['deficit_mean']} ± {s['deficit_sd']}"
        pm_str = f"{s['peakmem_mean']} ± {s['peakmem_sd']}"
        print(f'{s["scenario"]:9s} {s["framework"]:10s} '
              f'{iv_str:>18s} '
              f'{s["interval_runs"]:>22s} '
              f'{df_str:>16s} '
              f'{s["backlog_run"]:>12s} {s["backlog_fault_window"]:>14s} '
              f'{pm_str:>18s} {s["peakmem_runs"]:>20s}')
    print('=' * width)

    if args.out:
        with open(args.out, 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary[0].keys()))
            w.writeheader()
            w.writerows(summary)
        print(f'written: {args.out}')

    if args.check:
        check_manuscript(summary)


if __name__ == '__main__':
    main()
