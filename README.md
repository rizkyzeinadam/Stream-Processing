# Evaluasi Fault Recovery: Apache Flink vs Apache Spark Structured Streaming

Kerangka kerja eksperimental komparatif dan artefak analisis data untuk evaluasi *fault recovery* pada mesin pemrosesan data terdistribusi (**Apache Flink** dan **Apache Spark Structured Streaming**) di bawah beban dinamis dan skenario kegagalan bertingkat (*cascading faults*).

Repository ini memuat seluruh pipeline dari eksekusi container Docker, injeksi kegagalan, perekaman telemetri per detik, hingga skrip pasca-pemrosesan data (*post-processing*) untuk menghasilkan angka Tabel 1 & 2 naskah jurnal (TELKOMNIKA) serta 14 grafik Gambar Bab 4 tesis.

---

## 📋 Ringkasan Eksperimen

Eksperimen mengevaluasi ketahanan dan pemulihan kedua mesin pemrosesan terhadap 3 skenario kegagalan:

1. **Multi-Fault Cascading (900 detik):**
   - Beban dinamis bertingkat (*step load*) 100 → 1000 msg/s.
   - Kegagalan beruntun: *switchover* Worker 2 ($t=300\text{ s}$), terminasi paksa (*kill*) Worker 1 ($t=330\text{ s}$), dan pemulihan kembali Worker 1 ($t=660\text{ s}$).
2. **Single-Node Failure (600 detik):**
   - Beban bertahap (*ramp load*) 100 → 1000 msg/s.
   - Terminasi paksa 1 worker aktif pada $t=120\text{ s}$ (tanpa restart).
3. **Central Component Failure (600 detik):**
   - Beban bertahap (*ramp load*) 100 → 1000 msg/s.
   - Terminasi komponen koordinator (JobManager Flink / Driver Spark) pada $t=120\text{ s}$ dan dihidupkan kembali pada $t=180\text{ s}$ (downtime 60 detik).

Eksperimen dijalankan dengan **3 kali replikasi** untuk setiap skenario dan framework ($3 \times 3 \times 2 = 18\text{ run}$ independen) dengan prosedur sanitasi penuh (*stack reset*, re-create topic Kafka, penghapusan state/checkpoint) antar-replikasi untuk menghindari kontaminasi data.

---

## 🎯 7 Metrik Evaluasi (Bab 1 Tesis & Naskah)

Seluruh metrik didefinisikan secara kanonik pada [`scripts/analysis_recovery_metrics.py`](scripts/analysis_recovery_metrics.py) sebagai satu-satunya sumber kebenaran (*single source of truth*):

| No | Metrik Evaluasi | Satuan | Definisi & Penjelasan |
|:---:|:---|:---:|:---|
| **1** | **Throughput** | msg/s | Laju data output yang berhasil diproses per detik. Dilaporkan sebagai nilai rata-rata dan puncak. |
| **2** | **Latency** | ms | Waktu latensi pemrosesan data end-to-end (rata-rata sampel non-nol & puncak). Dilaporkan sebagai karakteristik tren internal. |
| **3** | **Backlog Proxy Rate** | msg/s | Laju penumpukan pesan antrean yang dihitung dari $\max(\text{beban} - \text{throughput}, 0)$ per detik (menggantikan raw Kafka lag untuk menghindari artefak restart Flink). |
| **4** | **CPU Utilization** | % | Rata-rata persentase penggunaan CPU node/container selama pengujian berlangsung. |
| **5** | **Memory Utilization** | MB | Penggunaan memori penampung tertinggi (*Peak Container Memory*) per run, dihitung rata-rata dan SD sampel (Tabel 2 naskah). |
| **6** | **Recovery Stabilization Interval** | detik | Durasi $T_2 - T_1$ di mana:<br>- $T_1$: sampel pertama saat throughput jatuh $< 50\%$ dari baseline pra-fault (rata-rata 60 detik pra-injeksi).<br>- $T_2$: sampel pertama dari 5 sampel beruntun yang mencapai throughput $\ge 90\%$ beban injeksi, dihitung dari akhir jendela fault aktif (Tabel 1 naskah). |
| **7** | **Throughput Deficit** | % | Persentase kapasitas beban yang hilang akibat kegagalan:<br>$1 - \frac{\int \text{throughput}\,dt}{\int \text{beban}\,dt} \times 100\%$<br>Sampel selama jendela pelapor mati (central failure) dihitung throughput 0. Celah data $> 30\text{ s}$ dijaga dengan $dt = 1\text{ s}$. |

---

## 📊 Target Verifikasi Metrik (Tabel 1 & Tabel 2 Naskah)

Semua angka dihitung langsung dari 18 file CSV mentah tanpa hardcoding:

| Skenario | Framework | Recovery Interval (s) | Per Run (rep1/2/3) | Throughput Deficit (%) | Peak Backlog (msg/s) | Peak Memory (MB) |
|:---|:---|:---:|:---:|:---:|:---:|:---:|
| **Multi-Fault** | **Flink** | **50.0 ± 4.8** | 52.5 / 52.8 / 44.5 | **9.18 ± 1.45** | 841 | 1391 ± 24 |
| **Multi-Fault** | **Spark** | **35.3 ± 11.4** | 47.5 / 33.5 / 25.0 | **8.11 ± 0.74** | 439 | 3914 ± 139 |
| **Single-Node** | **Flink** | **37.7 ± 2.4** | 40.5 / 36.3 / 36.3 | **10.59 ± 1.28** | 275 | 1233 ± 8 |
| **Single-Node** | **Spark** | **27.5 ± 9.9** | 22.7 / 38.9 / 20.9 | **8.11 ± 1.66** | 297 | 2826 ± 29 |
| **Central** | **Flink** | **68.6 ± 1.2** | 69.5 / 67.2 / 69.0 | **16.19 ± 2.61** | 190 | 1233 ± 1 |
| **Central** | **Spark** | **68.4 ± 1.8** | 67.9 / 66.9 / 70.4 | **19.35 ± 1.43** | 300 | 2567 ± 164 |

*Catatan: Pembulatan mengikuti kaidah ROUND_HALF_UP pada digit desimal terakhir; simpangan baku menggunakan SD sampel ($ddof=1$).*

---

## 📁 Struktur Direktori

```
Thesis/
├── run_all_analysis.py          # Master CLI: jalankan seluruh post-processing sekali jalan
├── README.md                    # Dokumentasi lengkap proyek
├── requirements.txt             # Dependensi Python
├── cic-collection.parquet       # Dataset sumber traffic jaringan (CIC-DDoS)
│
├── scripts/                     # Seluruh skrip eksekusi, analisis, dan visualisasi
│   ├── analysis_recovery_metrics.py  # [Kanonik] Verifikasi metrik Tabel 1 & 2 naskah
│   ├── generate_charts_bab4_3x.py   # Generator 14 grafik Gambar Bab 4 tesis
│   ├── make_fig1_scenarios.py       # Generator Figure 1 naskah (Arsitektur, 300 DPI)
│   ├── make_figs_print.py           # Generator Figure 2, 3, 4 naskah (Cetak, 300 DPI)
│   ├── generate_report.py           # Generator laporan komparatif Markdown
│   ├── generate_publication_figures.py # Generator pack gambar publikasi
│   ├── run_all_replications.sh      # Master runner replikasi 3x (18 run)
│   ├── run_experiment.sh            # Orkestrator eksperimen Flink & Spark
│   ├── fault_injection.sh           # Pengontrol injeksi kegagalan
│   ├── metrics_collector.py         # Pengumpul telemetri per detik
│   └── producer.py                  # Generator beban sintetis ke Kafka
│
├── results/                     # Hasil CSV 18 run & figur naskah
│   ├── flink_multi_rep[1-3].csv
│   ├── spark_multi_rep[1-3].csv
│   ├── flink_single_rep[1-3].csv
│   ├── spark_single_rep[1-3].csv
│   ├── flink_central_rep[1-3].csv
│   ├── spark_central_rep[1-3].csv
│   └── figs_print/              # Gambar cetak naskah 300 DPI (Figure 1, 2, 3, 4)
│
├── output_charts_bab4/          # 14 file gambar PNG untuk Bab 4 tesis
│   ├── fig41_tp_multi.png s/d fig412_backlog_central.png  (Timeseries)
│   ├── fig413_recovery_summary.png                       (Bar Chart Recovery Time)
│   └── fig414_deficit_summary.png                        (Bar Chart Deficit)
│
├── docker-compose/              # Konfigurasi container cluster Flink, Spark, Kafka
├── config/                      # Konfigurasi load ramp, fault timeline, monitoring
├── flink-jobs/                  # Source code Java & JAR Flink streaming job
├── spark-jobs/                  # Source code Java & JAR Spark streaming job
├── metrics/                     # Telemetri run aktif terkini
└── archive/                     # Arsip catatan prompt dan dokumen kerja lama
```

---

## 🚀 Panduan Penggunaan

### 1. Post-Processing & Visualisasi (Data CSV Sudah Ada)

Jika 18 file CSV sudah tersedia di folder `results/`, cukup jalankan satu perintah:

```bash
# Jalankan seluruh verifikasi naskah, gambar Bab 4, figure cetak 300 DPI, dan laporan
python3 run_all_analysis.py
```

#### Opsi Parsial:
* **Verifikasi kecocokan metrik ke naskah saja:**
  ```bash
  python3 run_all_analysis.py --check
  ```
  *(Output akan menampilkan baris-demi-baris OK dan diakhiri `hasil: SEMUA COCOK`).*
* **Regenerasi Figure 1–4 format cetak naskah saja (300 DPI):**
  ```bash
  python3 run_all_analysis.py --print
  ```
* **Regenerasi 14 Gambar Bab 4 tesis saja:**
  ```bash
  python3 run_all_analysis.py --bab4
  ```
* **Regenerasi laporan Markdown perbandingan:**
  ```bash
  python3 run_all_analysis.py --report
  ```

---

### 2. Menjalankan Ulang Eksperimen Docker (Jika Diperlukan)

#### Prasyarat:
1. Docker Desktop aktif.
2. Python environment aktif:
   ```bash
   pip install -r requirements.txt
   ```

#### Menjalankan Replikasi:
```bash
# Jalankan SEMUA skenario dan framework (18 run):
bash scripts/run_all_replications.sh all all 3

# Atau jalankan per skenario / framework spesifik:
bash scripts/run_all_replications.sh flink multi 3     # 3x Flink multi-fault
bash scripts/run_all_replications.sh spark multi 3     # 3x Spark multi-fault
bash scripts/run_all_replications.sh flink single 3    # 3x Flink single node
bash scripts/run_all_replications.sh spark central 3   # 3x Spark central failure
```

Setelah replikasi selesai, jalankan kembali:
```bash
python3 run_all_analysis.py
```
Seluruh gambar dan laporan akan otomatis diperbarui mengikuti data terbaru.

---

## 🖼️ Hasil Output Gambar

1. **Paket Naskah / Paper (`results/figs_print/`):**
   - `fig1_scenarios_print.png`: Arsitektur 3 skenario kegagalan (Before vs After).
   - `fig2_throughput_print.png`: Throughput Multi-Fault 2-panel (Flink vs Spark).
   - `fig3_latency_print.png`: Latensi Multi-Fault 2-panel.
   - `fig4_central_print.png`: Karakteristik throughput Central Component Failure.
   *(Ukuran sumber dirancang pas dengan lebar teks halaman docx A4 pada margin kiri 3,0 cm + kanan 2,5 cm = 6,10 in @ 300 DPI).*

2. **Paket Bab 4 Tesis (`output_charts_bab4/`):**
   - `Gambar 4.1 – 4.4`: Multi-Fault Cascading (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Gambar 4.5 – 4.8`: Single-Node Failure (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Gambar 4.9 – 4.12`: Central Component Failure (Throughput, Latency, CPU & Memory, Backlog Proxy).
   - `Gambar 4.13`: Rata-rata Recovery Stabilization Interval per Skenario (Bar Chart Mean ± SD).
   - `Gambar 4.14`: Rata-rata Throughput Deficit per Skenario (Bar Chart Mean ± SD).
