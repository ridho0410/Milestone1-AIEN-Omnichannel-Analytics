# Panduan Mencoba Proyek Omnichannel Retail

Panduan ini ditujukan untuk pengguna baru yang ingin menjalankan proyek di komputer sendiri. Perintah menggunakan **Windows dan PowerShell di VS Code**.

Ikuti jalur utama terlebih dahulu: database lokal, pipeline Airflow, lalu aplikasi analitik. Jalur ini memakai mode `mock`, sehingga **tidak memerlukan API key**. Opsi Neon dan LLM tersedia di bagian akhir.

## Gambaran singkat

Proyek mengubah file mentah menjadi data yang siap dianalisis:

```text
File mentah → Bronze → Silver → Quality gate 1 → Gold → Quality gate 2
```

| Bagian | Fungsinya |
|---|---|
| Bronze | Menyimpan record dari file sumber. |
| Silver | Membersihkan, memvalidasi, dan menghapus duplikasi data. |
| Gold | Menyiapkan tabel untuk analisis penjualan, pelanggan, produk, dan campaign. |
| Quality gate | Memeriksa apakah data memenuhi aturan sebelum proses dilanjutkan atau dinyatakan berhasil. |

**PostgreSQL** menyimpan tabelnya. **Airflow** menjalankan urutan pipeline. Setelah Gold siap, **API** menerima pertanyaan dan **Streamlit** menampilkan jawaban.

## 1. Siapkan alat dan buka proyek

Siapkan:

- VS Code.
- [UV](https://docs.astral.sh/uv/getting-started/installation/) untuk mengelola Python dan dependensi.
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) yang sudah berjalan dengan Linux containers.
- Koneksi internet untuk mengunduh dependensi, image Docker, dan dataset.

Unduh ZIP atau clone repository proyek yang dibagikan pemilik proyek. Jika menggunakan ZIP, ekstrak terlebih dahulu.

Di VS Code, pilih **File → Open Folder**, lalu buka folder utama proyek. Folder yang benar berisi `pyproject.toml`, `uv.lock`, `docker-compose.yml`, `dags/`, `pipelines/`, `engine/`, dan `ui/`. Gunakan paket proyek lengkap; folder submission tugas saja belum cukup.

Buka **Terminal → New Terminal** dan pilih **PowerShell**:

```powershell
uv --version
docker compose version
uv python install 3.11
uv sync --frozen
```

**Tanda berhasil:** instalasi selesai tanpa error dan folder `.venv` tersedia. `uv sync --frozen` memakai daftar versi dalam `uv.lock`.

**Jika gagal karena direktori: cd ".\Milestone1-AIEN-Omnichannel-Analytics-main\Milestone1-submission"**
> Semua perintah dalam panduan dijalankan dari folder utama proyek. Setiap membuka terminal baru, pastikan lokasinya tetap di folder tersebut. Tidak perlu mengaktifkan `.venv` secara manual karena perintah Python memakai `uv run`.

## 2. Buat dan atur file konfigurasi

Jalankan untuk membuat `.env` dari contoh bawaan jika belum ada:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Buka `.env` di VS Code. Ubah atau tambahkan pengaturan berikut; hindari nama variabel yang ditulis dua kali:

```dotenv
ANALYTICS_DB_TARGET=local
LOCAL_DATABASE_URL=postgresql://retail:retail@localhost:5432/retail_analytics
NL2SQL_PROVIDER=mock
NL2SQL_MAX_ROWS=100
ENGINE_URL=http://localhost:8000
```

**Artinya:** aplikasi membaca database lokal dan memakai kumpulan pertanyaan contoh. User, password, dan database di atas sesuai konfigurasi latihan bawaan.

Gunakan kredensial milik sendiri untuk layanan eksternal. Jangan meminta atau membagikan `.env` milik orang lain; file tersebut dapat memuat API key dan password.

## 3. Masukkan dataset

Dataset tidak otomatis ikut saat clone karena `data/raw/` diabaikan oleh Git. Unduh dataset dari sumber yang disebutkan pada bagian **Data Lake Google Drive** di [README tugas](README.md), lalu ekstrak dengan susunan berikut:

```text
data/raw/
├── manifest.json
├── operational/   # 11 file JSON
├── events/        # 5 file JSON
├── inventory/     # inventory_snapshots.csv
└── reference/     # campaign_spend.csv dan city_reference.json
```

**Tanda siap:** file seperti `data/raw/operational/orders.json` dapat dibuka. Jangan sampai ada folder tambahan di tengah, misalnya `data/raw/nama-dataset/operational/`.

Ada 19 file sumber yang diharapkan. Daftar lengkapnya ada di README tugas dan `pipelines/ingestion/files.py`. Sertakan `manifest.json` asli jika disediakan: kode dapat berjalan tanpa manifest, tetapi tidak bisa membandingkan checksum dengan manifest sumber. Tidak adanya manifest bukan bukti bahwa file sudah terverifikasi.

## 4. Nyalakan database dan buat tabel

Jalankan PostgreSQL:

```powershell
docker compose up -d postgres
docker compose exec postgres pg_isready -U retail -d retail_analytics
```

Tunggu sampai muncul pesan **accepting connections**. Jika belum siap, tunggu sebentar lalu ulangi perintah `pg_isready`.

Setelah itu, buat struktur database:

```powershell
uv run python -m pipelines.ingestion.initialize_database --target local
```

**Tanda berhasil:** output menyebut `bronze.sql`, `silver.sql`, `gold.sql`, dan `ops.sql` sudah diterapkan.

Langkah inisialisasi wajib dilakukan sebelum menjalankan DAG pertama. Task awal DAG langsung menulis catatan eksekusi ke tabel `ops`. Tabel Gold masih dapat kosong sampai pipeline selesai.

## 5. Jalankan pipeline melalui Airflow

Nyalakan Airflow:

```powershell
docker compose -f airflow-docker/docker-compose.yml up -d --build
```

Buka **http://localhost:8080**. Gunakan kredensial admin yang muncul pada log startup Airflow, sesuai [panduan standalone Airflow 2.10.5](https://airflow.apache.org/docs/apache-airflow/2.10.5/start.html). Jika belum siap, tunggu lalu jalankan kembali perintah log. Jangan bagikan log yang memuat password.

Di dashboard Airflow:

1. Cari DAG **`retail_omnichannel_pipeline`**.
2. Aktifkan DAG jika masih paused.
3. Klik **Trigger DAG**.
4. Buka tampilan Graph/Grid dan tunggu semua task berstatus **success**.

Urutan task:

```text
validate_raw_files → initialize_schemas → load_bronze → build_silver
    → quality_gate_pre_gold → build_gold → validate_gold
```

**Tanda berhasil:** seluruh task sukses. Jika gate gagal, baca bagian 8 sebelum menjalankan ulang.

Perhatikan sebelum mengulang pipeline: Bronze mengganti record untuk file yang dimuat, sedangkan Silver dan Gold menghapus lalu membangun ulang isi tabel. Gunakan database latihan; versi historis Bronze tidak disimpan secara append-only.

DAG juga memiliki jadwal harian pukul 00.00 menurut timezone Airflow. Pause DAG setelah mencoba jika tidak ingin proses berjalan terjadwal. Task gagal memiliki satu retry dengan jeda dua menit.

## 6. Jalankan API — terminal pertama

Setelah pipeline berhasil, buka terminal PowerShell untuk API:

```powershell
$env:ANALYTICS_DB_TARGET = 'local'
$env:NL2SQL_PROVIDER = 'mock'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Biarkan terminal ini tetap berjalan. Buka **http://localhost:8000/health** di browser.

**Tanda berhasil:** respons memuat `"status":"ok"` dan `"provider":"MockProvider"`. Dokumentasi API ada di **http://localhost:8000/docs**.

Endpoint health hanya memeriksa API. Koneksi database dan hasil analitik baru terbukti ketika pertanyaan berhasil dijalankan.

## 7. Jalankan tampilan aplikasi — terminal kedua

Buka terminal PowerShell baru di folder proyek, lalu jalankan:

```powershell
$env:ENGINE_URL = 'http://localhost:8000'
uv run python -m streamlit run ui/app.py --server.address 127.0.0.1 --server.port 8501
```

Buka **http://localhost:8501** dan masukkan pertanyaan:

```text
What is total net revenue by sales channel?
```

**Tanda berhasil:** aplikasi menampilkan jawaban dan tabel hasil; grafik muncul jika hasilnya mendukung visualisasi.

Contoh pertanyaan lain:

- `How many orders were placed each day?`
- `Which campaigns had the best ROAS?`
- `What was the daily net revenue?`

Mode `mock` hanya menerima pertanyaan yang sudah disediakan di `engine/llm/providers.py`. Kapitalisasi dan spasi dinormalkan, tetapi kalimat bebas atau terjemahan tidak otomatis didukung. Mode ini tetap membaca tabel Gold; hasilnya bukan data palsu yang dibuat tanpa database.

## 8. Pahami hasil quality gate

Quality gate berfungsi sebagai **pemeriksaan kelayakan data**. Aturannya ada di `pipelines/quality/run_quality_checks.py`.

### Kapan pemeriksaan dilakukan?

| Pemeriksaan | Yang diperiksa | Jika ada error |
|---|---|---|
| Sebelum Gold: `pre_gold` | Bronze berisi data; ID order/item tidak duplikat; quantity positif; referensi order, product, customer, dan channel tersedia. | Task `quality_gate_pre_gold` gagal. Gold belum dibangun oleh run ini. |
| Setelah Gold: `post_gold` | Mengulang pemeriksaan Bronze/Silver, memeriksa keunikan baris Gold, rumus revenue, nilai nominal tertentu, stockout, dan kecocokan total antar tabel. | Task `validate_gold` gagal. Run tidak dinyatakan sukses. |

Mencocokkan total antar tabel disebut **rekonsiliasi**. Contohnya, total pendapatan dari tabel per order dibandingkan dengan total pendapatan dari tabel ringkasan harian.

### Bagaimana membaca status?

| Hasil | Artinya | Tindakan |
|---|---|---|
| `pass` | Pemeriksaan memenuhi aturan. | Lanjutkan membaca hasil pemeriksaan lainnya. |
| `fail` dengan level `error` | Ada pelanggaran yang menghentikan gate. | Periksa log, perbaiki data atau transformasi, lalu jalankan ulang. |
| `fail` dengan level `warning` | Ada masalah yang dicatat, tetapi tidak menghentikan gate. | Tinjau dampaknya pada hasil analisis. |
| `skipped` | Pemeriksaan dilewati. | Periksa alasannya; status ini bukan bukti lolos. |

`gate_passed=True` berarti tidak ada hasil `fail` berlevel error. Warning dan skipped masih mungkin ada. Jika eksekusi SQL bermasalah, task juga dapat gagal sebelum ringkasan gate muncul.

### Tiga contoh sederhana

**Semua contoh berikut adalah ilustrasi, bukan hasil run proyek.** Log ditampilkan sebagian; contoh lolos mengasumsikan tidak ada kegagalan error lain.

**A. Lolos: tidak ada quantity yang tidak valid**

```text
silver.order_items_no_negative_quantity: pass (observed=0.0, expected=0.0)
[quality:pre_gold] gate_passed=True
```

`observed` adalah hasil pemeriksaan dan `expected` adalah nilai yang diharapkan. Di sini, kode menghitung item dengan `quantity <= 0`. Hasil `0` berarti tidak ditemukan item yang melanggar, sehingga pemeriksaan lolos.

**B. Warning: satu file sumber penting belum tersedia**

```text
bronze.missing_critical_sources: fail (observed=1.0, expected=0.0)
[quality:pre_gold] WARNINGS: 1
[quality:pre_gold] gate_passed=True
```

Kode mengharapkan tidak ada sumber penting yang hilang, tetapi menemukan satu. Aturan ini berlevel **warning**, sehingga pipeline masih boleh melanjutkan. Hasil keuangan perlu ditinjau karena sumbernya belum lengkap.

**C. Gagal: total pendapatan tidak cocok**

Misalnya, total `net_revenue` di `gold.order_360` adalah `1000000.00`, tetapi total di `gold.executive_kpis_daily` hanya `990000.00`.

```text
net_revenue_total: fail (variance=10000.0, tolerance=0.01)
[quality:post_gold] gate_passed=False
```

`variance` adalah selisih kedua nilai. `tolerance` adalah selisih maksimum yang diizinkan. Selisih `10000.00` melebihi `0.01` dalam satuan nilai data, sehingga rekonsiliasi berlevel **error** ini menggagalkan validasi.

Periksa perhitungan dan join saat membangun ringkasan harian. Validasi akhir yang gagal tidak membatalkan Gold yang sudah dibuat oleh task sebelumnya; perbaiki dan periksa kembali sebelum hasil dipakai.

### Di mana melihat hasil sebenarnya?

Di Airflow, klik task **`quality_gate_pre_gold`** atau **`validate_gold`**, lalu buka **Logs**.

Hasil juga dicatat di database:

| Tabel | Isinya |
|---|---|
| `ops.pipeline_runs` | Status keseluruhan run dan pesan error. |
| `ops.quality_checks` | Nama pemeriksaan, level error/warning, status, nilai aktual, dan target. |
| `ops.reconciliation_results` | Dua nilai pembanding, selisih, toleransi, dan status. |

Log per-check tidak menampilkan level error/warning. Lihat `severity` pada `ops.quality_checks`. Tabel rekonsiliasi tidak menyimpan severity; acuannya adalah definisi `RECONCILIATIONS` di kode. Rekonsiliasi refund/payment saat ini berlevel warning dan dilewati jika tabel event terkait kosong.

<details>
<summary>Opsional: perintah untuk melihat hasil run terbaru</summary>

Jalankan pada terminal lain dari folder utama proyek. Perintah berikut hanya membaca database:

```powershell
$sql = @"
SELECT pipeline_run_id, status, error_message
FROM ops.pipeline_runs ORDER BY started_at_utc DESC LIMIT 5;

SELECT check_name, severity, status, observed_value, expected_value
FROM ops.quality_checks
WHERE pipeline_run_id = (
    SELECT pipeline_run_id FROM ops.pipeline_runs
    ORDER BY started_at_utc DESC LIMIT 1
)
ORDER BY check_id;

SELECT metric_name, left_value, right_value, variance, tolerance, status
FROM ops.reconciliation_results
WHERE pipeline_run_id = (
    SELECT pipeline_run_id FROM ops.pipeline_runs
    ORDER BY started_at_utc DESC LIMIT 1
)
ORDER BY reconciliation_id;
"@
$sql | docker compose exec -T postgres psql -U retail -d retail_analytics
```

Pemeriksaan Bronze/Silver dapat muncul berulang karena dijalankan sebelum dan sesudah Gold, serta saat retry. Cocokkan urutan dengan log task. Jika run belum mencapai pemeriksaan setelah Gold, hasil rekonsiliasinya belum tersedia.

</details>

## 9. Jika terjadi masalah

| Masalah | Langkah yang dicoba |
|---|---|
| `uv` tidak dikenali | Instal UV melalui tautan bagian 1, lalu buka ulang terminal. |
| Docker daemon tidak dapat dihubungi | Jalankan Docker Desktop dan tunggu sampai siap. |
| Port 5432, 8000, atau 8501 sudah dipakai | Periksa proses/container lama. Jika API/UI Docker lama berjalan, hentikan dengan `docker compose stop nl2sql-api analytics-ui` sebelum memakai UV. |
| Koneksi PostgreSQL gagal | Periksa status container dan `LOCAL_DATABASE_URL`. Lihat log dengan `docker compose logs --tail 100 postgres`. |
| `ops.pipeline_runs` belum ada | Ulangi inisialisasi pada bagian 4 sebelum trigger DAG. |
| DAG tidak muncul | Periksa log Airflow dan jalankan `docker exec retail-airflow airflow dags list-import-errors`. |
| Raw files tidak ditemukan | Cocokkan struktur folder pada bagian 3; data tidak otomatis ikut clone repository. |
| Gold tidak ada atau hasil kosong | Pastikan pipeline berhasil dan API menggunakan target `local`. Lihat hasil quality gate. |
| Pertanyaan ditolak mode mock | Gunakan contoh pertanyaan persis dari bagian 7. |
| UI tidak terhubung / HTTP 503 | Pastikan API hidup, Engine URL di sidebar UI adalah `http://localhost:8000`, lalu baca error pada terminal API. |
| Konfigurasi berubah tetapi perilaku tetap sama | Periksa variabel `$env:` di terminal; nilainya mengalahkan `.env`. Restart API/UI setelah perubahan. |

Jika port PostgreSQL diubah, sesuaikan port pada Compose utama, URL lokal di `.env`, dan URL Airflow. Proses Python di Windows memakai `localhost`; Airflow di container memakai `host.docker.internal`, sesuai konfigurasi bawaan.

## 10. Selesai mencoba dan menjalankan kembali

Hentikan API dan UI dengan **Ctrl+C** pada masing-masing terminal. Hentikan container:

```powershell
docker compose -f airflow-docker/docker-compose.yml stop
docker compose stop postgres
```

Untuk memakai lagi tanpa menghapus data:

```powershell
docker compose up -d postgres
docker compose -f airflow-docker/docker-compose.yml up -d
```

Lalu jalankan API dan UI seperti bagian 6–7. Trigger pipeline lagi jika perlu memuat atau membangun ulang data. Hindari `down -v` kecuali memang ingin menghapus isi volume database atau metadata Airflow.

## Opsi tambahan

Bagian berikut dapat dicoba setelah alur utama dipahami.

<details>
<summary>Memakai LLM untuk pertanyaan bebas</summary>

Isi pengaturan berikut di `.env` menggunakan API key milik sendiri:

```dotenv
NL2SQL_PROVIDER=openai
OPENAI_API_KEY=ISI_API_KEY_ANDA
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4.1-mini
```

Nama model mengikuti default proyek; aksesnya belum tentu tersedia pada setiap akun. Gunakan model yang tersedia pada akun dan kompatibel dengan adapter proyek, yang mengirim request ke `/chat/completions`.

Hentikan API dengan **Ctrl+C**, lalu restart:

```powershell
$env:ANALYTICS_DB_TARGET = 'local'
$env:NL2SQL_PROVIDER = 'openai'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Pengaturan terminal diperbarui karena nilai `mock` dari langkah awal dapat mengalahkan `.env`. UI tetap dijalankan seperti bagian 7.

Pemakaian LLM dapat dikenai biaya. Pertanyaan, konteks percakapan, dan schema catalog dikirim ke provider; periksa isinya sebelum memakai informasi sensitif.

</details>

<details>
<summary>Mencoba demo menggunakan Gold sample di Neon</summary>

Opsi ini membutuhkan akses ke database Neon yang sudah memiliki Gold sample. Minta sumber akses yang sesuai kepada pemilik demo atau gunakan database Neon sendiri yang sudah disiapkan.

Untuk demo ini, dataset lokal dan Airflow tidak diperlukan. Isi `.env`:

```dotenv
ANALYTICS_DB_TARGET=neon
NEON_DATABASE_URL=postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
NL2SQL_PROVIDER=mock
```

Ganti seluruh placeholder URL dengan koneksi milik sendiri. Field `POSTGRES_*` masih didukung untuk Neon jika `NEON_DATABASE_URL` tidak terisi.

Hentikan API lama, lalu jalankan:

```powershell
$env:ANALYTICS_DB_TARGET = 'neon'
$env:NL2SQL_PROVIDER = 'mock'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Jalankan UI seperti bagian 7. Engine hanya membaca Gold. Pipeline tetap memakai profil `local`; jangan mengisi `LOCAL_DATABASE_URL` dengan alamat Neon karena pipeline akan menulis ke alamat tersebut.

</details>

<details>
<summary>Perintah diagnosis dan pengujian</summary>

```powershell
# Menjalankan pipeline manual untuk diagnosis
uv run python -m pipelines.run_pipeline --input data/raw --target local

# Menjalankan test yang disediakan
uv run python -m pytest -q

# Memeriksa kedua file Compose tanpa menyalakan service
docker compose -f docker-compose.yml config --quiet
docker compose -f airflow-docker/docker-compose.yml config --quiet

# Menjalankan benchmark pertanyaan
uv run python -m engine.benchmarks.run_benchmark
```

Jangan menjalankan CLI pipeline dan DAG secara bersamaan karena keduanya menulis ulang tabel yang sama. Untuk kebutuhan milestone, orkestrasi dan bukti eksekusi tetap melalui DAG.

Benchmark membaca database dan dapat memakai layanan LLM sesuai provider yang aktif. Test generator tidak dikoleksi jika paket `pipelines.generator` tidak tersedia. Lulus test tidak membuktikan DAG atau koneksi provider berjalan langsung.

Jika ingin memilih interpreter di VS Code: **Ctrl+Shift+P → Python: Select Interpreter → `.venv\Scripts\python.exe`**.

Peta file utama: `dags/dag.py` mengatur alur; `pipelines/` berisi transformasi dan quality checks; `database/schemas/` berisi struktur tabel; `shared/db.py` mengatur koneksi; `engine/` berisi API/NL-to-SQL; `ui/` berisi tampilan.

</details>

## Batas panduan

Panduan disusun dari kode dan konfigurasi proyek pada 7 November 2026, dengan pemeriksaan sintaks perintah serta kecocokan contoh quality gate. Panduan belum diuji sebagai instalasi baru di komputer pengguna lain. Contoh log adalah ilustrasi; gunakan hasil Airflow/database pada komputer sendiri sebagai bukti eksekusi. Konfigurasi bawaan ditujukan untuk latihan lokal.
