# BAB IV: HASIL DAN PEMBAHASAN

## 4.1 Skenario Eksperimen dan Landasan Observasi
Bab ini tidak diposisikan sebagai sekadar perbandingan benchmark, tetapi sebagai evaluasi pemulihan terdistribusi pada dua arsitektur pemrosesan aliran yang dieksekusi pada kondisi beban dan kegagalan yang sama. Skenario yang tersedia dalam artefak eksperimen menunjukkan tiga titik gangguan utama pada sekitar t=300 s, t=330 s, dan t=660 s, dengan fase beban yang bergerak dari baseline, increasing load, high load, burst load, hingga recovery phase.

Pengamatan utama diambil dari deret waktu `flink_metrics.csv` dan `spark_metrics.csv`, lalu diperkaya oleh figur publikasi yang sudah dihasilkan pada `results/publication_charts`, `results/backlog_analysis`, `results/recovery_analysis`, dan `results/correctness_analysis`. Karena itu, pembahasan di bawah dibangun hanya dari bukti yang sudah ada: throughput, latency, CPU, memory, Kafka lag, recovery duration, output reconciliation, dan replay amplification.

## 4.2 Metrik Pemulihan Formal
Untuk menggeser fokus dari "siapa lebih cepat" menjadi "bagaimana sistem pulih", penelitian ini dapat dibaca melalui enam metrik pemulihan yang memang sudah tersirat dalam data:

| Metrik Pemulihan | Definisi Operasional | Flink | Spark | Interpretasi |
|---|---|---:|---:|---|
| Recovery duration | Durasi sampai sistem kembali pada kondisi aktif setelah gangguan | 3.33 s | 87.78 s | Flink pulih lebih cepat secara end-to-end |
| Backlog amplitude | Puncak akumulasi antrean di boundary observability | 5234 | 100 | Flink menampung backlog yang jauh lebih besar pada boundary Kafka |
| Latency amplification | Puncak latency selama recovery | 852.46 ms | 21423.58 ms | Spark menanggung biaya recovery pada latency internal |
| Throughput retention | Throughput rata-rata selama eksperimen | 179.65 msg/s | 89.57 msg/s | Flink mempertahankan laju pemrosesan lebih tinggi |
| Replay amplification | Rasio output terhadap input yang tidak menjadi data hilang | 79.23% | 7.27% | Flink menunjukkan replay/reprocessing yang jauh lebih besar |
| Loss ratio | Rekonsiliasi input-output untuk event yang tak terpulihkan | 0.00% | 0.00% | Tidak ada data loss permanen yang terukur |

Angka-angka ini mendukung framing bahwa recovery bukan hanya masalah throughput rata-rata, melainkan kombinasi antara durasi pemulihan, besarnya backlog, dan biaya temporal yang dibayar sistem untuk mengejar ketertinggalan.

## 4.3 Temporal Recovery-State Abstraction
Figur gabungan pada `recovery_timeline_master.png` dan `kafka_lag_evolution.png` memperlihatkan bahwa keadaan sistem dapat diabstraksikan menjadi beberapa state temporal:

1. **State normal / pre-fault**: sistem melayani beban yang meningkat dengan latensi relatif terkendali.
2. **State backlog build-up**: gangguan memicu akumulasi lag dan penurunan throughput efektif.
3. **State catch-up**: sistem mulai mengejar backlog, tetapi beban observabilitas berpindah dari antrean ke latency dan CPU.
4. **State stabilization**: throughput dan latency mendekati pola yang lebih konsisten, meskipun tidak selalu kembali ke baseline yang sama.

Pada Flink, transisi state terlihat jelas pada kenaikan backlog boundary di sekitar fault sequence. Cuplikan data di sekitar t≈300 s menunjukkan throughput rata-rata sekitar 203 msg/s, latency sekitar 102 ms, dan lag rata-rata sekitar 319 record. Di sekitar t≈330 s, lag rata-rata melonjak ke sekitar 1159 record, sementara throughput tetap ada pada kisaran operasional. Ini menunjukkan bahwa Flink menyerap gangguan terutama sebagai backlog yang membesar, bukan sebagai lonjakan latency yang ekstrem.

Pada Spark, pola state berbeda. Di sekitar t≈300 s, throughput rata-rata berada di kisaran 57 msg/s dengan latency rata-rata sekitar 633 ms. Di sekitar t≈330 s, throughput naik ke sekitar 178 msg/s, tetapi latency rata-rata juga naik ke sekitar 1966 ms sementara lag boundary tetap rendah. Artinya, beban recovery pada Spark lebih banyak berpindah ke delay eksekusi internal daripada ke penumpukan backlog di boundary Kafka.

Abstraksi state ini penting karena menunjukkan bahwa dua sistem tidak hanya berbeda dalam nilai metrik, tetapi juga dalam lokasi fisik tempat recovery cost muncul.

## 4.4 Backlog Stabilization dan Propagasi Pemulihan
Figur `kafka_lag_evolution.png` memperlihatkan bahwa Flink membangun backlog lebih besar, tetapi backlog tersebut juga menjadi ruang tempat pemulihan dapat diamati secara eksplisit. Setelah fault kedua dan ketiga, lag Flink membentuk puncak di atas 10^3 record, lalu turun bertahap ketika throughput mengejar backlog. Pada bagian akhir eksperimen, Flink masih menyisakan lag yang tidak nol, sehingga recovery belum sepenuhnya selesai pada boundary Kafka, tetapi throughput dan latency sudah jauh lebih stabil dibanding saat puncak gangguan.

Spark menunjukkan kebalikan dari pola tersebut. Lag boundary tetap rendah sepanjang banyak fase, tetapi latency tetap tinggi dan pada tail eksperimen masih berada di atas 1.8 s bahkan ketika lag mendekati nol. Ini berarti backlog di Kafka bukan satu-satunya indikator recovery; pada Spark, biaya recovery lebih banyak muncul sebagai antrean internal, scheduling delay, dan efek micro-batch yang tertinggal.

Implikasinya, lag yang rendah tidak boleh langsung ditafsirkan sebagai sistem yang sudah pulih. Untuk studi recovery, lag harus dibaca bersama latency end-to-end dan throughput retention.

## 4.5 Cross-Metric Correlation
Deret waktu yang tersedia juga memperlihatkan hubungan antar metrik yang berguna untuk interpretasi sistem:

| Korelasi Pearson | Flink | Spark | Makna |
|---|---:|---:|---|
| Throughput vs Kafka lag | -0.26 | -0.69 | Pada Spark, backlog boundary lebih erat terkait penurunan throughput |
| Kafka lag vs latency | -0.19 | -0.57 | Pada Spark, latency tetap tinggi walau lag boundary kecil |
| Throughput vs latency | -0.50 | 0.68 | Flink memperlihatkan trade-off lebih langsung antara laju dan delay |
| CPU vs throughput | -0.42 | 0.50 | Spark menghabiskan CPU lebih besar ketika mengejar pemulihan |
| CPU vs Kafka lag | -0.20 | -0.66 | Pada Spark, CPU naik ketika observabilitas backlog boundary turun |

Pembacaan yang paling penting di sini bukan sekadar nilai korelasi, tetapi arah beban recovery. Flink memindahkan beban ke backlog boundary, sedangkan Spark memindahkan beban ke latency dan CPU. Inilah alasan mengapa satu sistem dapat tampak "lebih bersih" pada lag tetapi tetap jauh lebih lambat pulih pada latensi.

## 4.6 Observability-Driven Recovery Interpretation
Figur master recovery timeline menunjukkan bahwa observability yang kaya diperlukan untuk membaca recovery secara benar. Jika hanya throughput yang dilihat, Flink dan Spark sama-sama tampak aktif memproses beban. Jika hanya Kafka lag yang dilihat, Spark bisa tampak lebih sehat karena backlog boundary rendah. Namun ketika throughput, latency, lag, dan recovery-state dilihat bersama, terlihat bahwa Spark masih menanggung recovery cost yang berat sampai akhir eksperimen.

Sebaliknya, Flink memperlihatkan pola yang konsisten dengan streaming kontinu: throughput relatif stabil, latency lebih terkontrol, tetapi backlog yang menumpuk lebih besar di boundary Kafka. Maka, untuk Bab 4, metrik observabilitas sebaiknya diposisikan sebagai indikator lokasi biaya recovery, bukan sebagai kemenangan atau kekalahan tunggal.

## 4.7 Replay-Aware Recovery Analysis
Figur `replay_amplification_analysis.png` dan `data_loss_reconciliation.png` memperlihatkan bahwa tidak ada kehilangan data permanen pada kedua sistem, tetapi output yang dihasilkan melebihi jumlah input. Flink menghasilkan 160,168 event output dari 89,579 event input, sedangkan Spark menghasilkan 95,964 output dari 89,461 input. Karena loss ratio untuk keduanya adalah 0.00%, surplus output ini lebih aman diinterpretasikan sebagai replay-induced amplification atau transient reprocessing, bukan sebagai silent data loss.

Interpretasi ini penting untuk integritas ilmiah. Angka duplikasi tidak boleh langsung diklaim sebagai pelanggaran correctness permanen tanpa lineage-level provenance yang lebih rinci. Dalam batas bukti yang tersedia, pembacaan yang valid adalah:

1. sistem melakukan replay atau reprocessing selama recovery,
2. surplus output mencerminkan biaya pemulihan, dan
3. tidak ada bukti data hilang secara permanen.

Dengan framing seperti ini, hasil penelitian tetap kuat tetapi tidak melebihi bukti yang ada.

## 4.8 Implikasi Arsitektural
Hasil yang tersedia mendukung tiga implikasi arsitektural yang dapat dipakai untuk menggeser tesis dari studi perbandingan menjadi studi evaluasi recovery terdistribusi:

1. **Recovery harus dievaluasi sebagai lintasan temporal, bukan satu angka agregat.** Recovery duration, latency spike, dan backlog decay perlu dibaca bersama.
2. **Boundary backlog bukan satu-satunya lokasi biaya recovery.** Spark menunjukkan bahwa lag rendah tidak menjamin recovery selesai jika latency dan CPU masih tinggi.
3. **Replay semantics harus dipisahkan dari data loss.** Surplus output perlu dilaporkan sebagai replay amplification selama bukti lineage belum cukup untuk menyimpulkan correctness failure.

## 4.9 Kesimpulan Bab
Secara keseluruhan, data yang tersedia tidak mendukung narasi sederhana bahwa satu engine "menang" atas yang lain dalam arti umum. Yang didukung oleh bukti adalah bahwa Flink pulih lebih cepat dan mempertahankan latency yang lebih rendah, sedangkan Spark menahan backlog boundary yang lebih kecil tetapi membayar biaya recovery pada latency internal yang jauh lebih tinggi. Dengan demikian, kontribusi ilmiah yang paling kuat dari penelitian ini adalah evaluasi recovery terdistribusi yang berorientasi pada state, observability, dan replay semantics, bukan sekadar benchmark performa.
