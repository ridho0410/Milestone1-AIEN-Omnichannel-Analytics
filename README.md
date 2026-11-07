# Panduan Setup — Omnichannel Retail NL-to-SQL Analytics

Panduan menjalankan proyek di Windows melalui terminal PowerShell VS Code: PostgreSQL lokal → pipeline Airflow → Gold → API FastAPI → UI Streamlit. Panduan ini melengkapi `README.md` yang berisi instruksi milestone.

Disusun dari kode dan konfigurasi proyek pada 7 November 2026. Semua perintah dijalankan dari folder utama proyek, kecuali disebutkan lain.

## 1. Persiapan

Siapkan:

- VS Code, dengan folder proyek dibuka melalui **File → Open Folder**.
- UV untuk mengelola Python dan dependensi.
- Python 3.11, sesuai `.python-version`; `pyproject.toml` mensyaratkan Python >=3.11.
- Docker Desktop yang sudah berjalan untuk PostgreSQL dan Airflow.
- Dataset di `data/raw/`.
- Koneksi internet saat pertama mengunduh dependensi dan image Docker. API key hanya diperlukan jika memakai provider LLM.

Buka **Terminal → New Terminal**, pilih PowerShell, kemudian:

```powershell
Set-Location -LiteralPath 'C:\Users\bahar\OneDrive\Documents\AI HACKIV8\Milestone1-omnichannel_retail-main'
uv --version
docker --version
docker compose version
```

Jika UV belum tersedia, instal melalui petunjuk resmi [UV](https://docs.astral.sh/uv/getting-started/installation/), lalu buka ulang terminal. Jika Python 3.11 belum tersedia melalui UV:

```powershell
uv python install 3.11
```

Port yang dipakai konfigurasi proyek:

| Komponen | Alamat dari Windows |
|---|---|
| PostgreSQL | `localhost:5432` |
| Airflow | `http://localhost:8080` |
| API | `http://localhost:8000` |
| Dokumentasi API | `http://localhost:8000/docs` |
| Streamlit | `http://localhost:8501` |

## 2. Instal dependensi Python

```powershell
uv sync --frozen
uv run python --version
```

`uv sync --frozen` memakai `uv.lock` yang sudah tersedia. Di VS Code, tekan **Ctrl+Shift+P → Python: Select Interpreter**, lalu pilih `.venv\Scripts\python.exe` jika ekstensi Python terpasang.

Tidak perlu mengaktifkan virtual environment secara manual jika semua perintah memakai `uv run`. Airflow dijalankan melalui image Docker tersendiri; Airflow tidak tercantum dalam dependensi UV proyek.

## 3. Atur `.env` untuk mode lokal

File `.env` sudah ada pada folder yang diperiksa. Pertahankan nilai rahasia yang masih diperlukan; jangan menimpanya dengan `.env.example`. Pada salinan proyek yang belum mempunyai `.env`, buat dengan:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Buka `.env` di VS Code dan pastikan pengaturan berikut ada, masing-masing hanya satu kali:

```dotenv
ANALYTICS_DB_TARGET=local
LOCAL_DATABASE_URL=postgresql://retail:retail@localhost:5432/retail_analytics
NL2SQL_PROVIDER=mock
NL2SQL_MAX_ROWS=100
ENGINE_URL=http://localhost:8000
```

Nilai `retail` di atas adalah kredensial database latihan dari `docker-compose.yml`, bukan kredensial Neon. Jangan menyalin API key atau URL database pribadi ke dokumentasi, screenshot, atau GitHub. `.env` sudah tercantum di `.gitignore`.

Perbedaan konfigurasi yang perlu diketahui:

- `ANALYTICS_DB_TARGET` menentukan database yang dibaca engine: `local` atau `neon`.
- Pipeline dibatasi ke target `local`, terlepas dari target engine.
- `LOCAL_DATABASE_URL` untuk proses Python di Windows memakai `localhost`.
- Airflow di dalam container memakai `host.docker.internal`; nilainya sudah diatur dalam `airflow-docker/docker-compose.yml`.
- Environment variable pada terminal atau container lebih diprioritaskan daripada `.env`. Setelah mengganti konfigurasi, periksa override pada terminal dan restart API/UI.

## 4. Jalankan PostgreSQL lokal

```powershell
docker compose up -d postgres
docker compose ps postgres
docker compose exec postgres pg_isready -U retail -d retail_analytics
```

Tunggu sampai PostgreSQL sehat dan `pg_isready` melaporkan menerima koneksi. Jika gagal, lihat:

```powershell
docker compose logs --tail 100 postgres
```

Panduan utama menjalankan API/UI dengan UV. Karena itu, cukup nyalakan service `postgres` di tahap ini. Jika API/UI container dari percobaan sebelumnya masih berjalan dan menggunakan port yang sama:

```powershell
docker compose stop nl2sql-api analytics-ui
```

## 5. Siapkan dataset

Struktur yang diharapkan:

```text
data/raw/
├── manifest.json
├── operational/   # 11 file JSON
├── events/        # 5 file JSON
├── inventory/     # inventory_snapshots.csv
└── reference/     # campaign_spend.csv dan city_reference.json
```

Daftar lengkap file dan sumber dataset ada pada bagian **Data Lake Google Drive** di `README.md`; daftar yang diperiksa kode ada di `pipelines/ingestion/files.py`.

Saat panduan dibuat, 19 file sumber sudah ditemukan dan tidak ada sumber yang hilang, tetapi `data/raw/manifest.json` belum tersedia. Kode saat ini memperlakukan manifest sebagai opsional; tanpa manifest, checksum tidak dapat dibandingkan dengan manifest sumber. Untuk memenuhi dokumentasi dataset pada tugas, sertakan manifest asli jika tersedia. Jangan membuat checksum pembanding sendiri lalu menganggapnya sebagai bukti kecocokan dengan sumber.

Setelah mengekstrak dataset, pastikan tidak ada folder bertingkat seperti `data/raw/nama-dataset/operational/`.

## 6. Inisialisasi schema sebelum menjalankan DAG

```powershell
uv run python -m pipelines.ingestion.initialize_database --target local
```

Perintah ini menerapkan `bronze.sql`, `silver.sql`, `gold.sql`, dan `ops.sql`. Inisialisasi pertama wajib dilakukan sebelum trigger DAG: task `validate_raw_files` langsung memanggil `start_run()` yang menulis ke `ops.pipeline_runs`, sedangkan task `initialize_schemas` baru berada setelahnya.

Periksa schema:

```powershell
docker compose exec postgres psql -U retail -d retail_analytics -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze','silver','gold','ops') ORDER BY schema_name;"
```

Keempat schema harus muncul. Gold masih dapat kosong sebelum pipeline berhasil.

## 7. Jalankan Airflow dan pipeline

Dari folder utama proyek:

```powershell
docker compose -f airflow-docker/docker-compose.yml up -d --build
docker compose -f airflow-docker/docker-compose.yml ps
docker compose -f airflow-docker/docker-compose.yml logs --tail 100 airflow
```

Airflow menggunakan image dasar `apache/airflow:2.10.5` dengan mode `standalone`. Buka `http://localhost:8080`. Gunakan kredensial admin yang ditampilkan pada log startup Airflow di atas, sesuai [panduan resmi Airflow 2.10.5](https://airflow.apache.org/docs/apache-airflow/2.10.5/start.html). Jangan bagikan output yang memuat password. Jika akun pernah diubah, gunakan kredensial yang berlaku pada volume Airflow tersebut.

Di dashboard:

1. Cari DAG `retail_omnichannel_pipeline`.
2. Aktifkan DAG jika masih paused.
3. Pilih **Trigger DAG** untuk menjalankan pipeline.
4. Buka tampilan Graph/Grid dan periksa urutan task:

```text
validate_raw_files
  → initialize_schemas
  → load_bronze
  → build_silver
  → quality_gate_pre_gold
  → build_gold
  → validate_gold
```

Run berhasil jika seluruh task berstatus **success**. Jika gagal, buka log task pertama yang gagal; task berikutnya dapat berhenti karena dependency atau quality gate. DAG memiliki satu retry dengan jeda dua menit.

Kode DAG saat ini memakai cron `0 0 * * *` pada Airflow 2.10.5, yaitu pukul 00.00 menurut timezone Airflow yang digunakan. Mengaktifkan DAG juga mengaktifkan jadwal tersebut; pause kembali setelah demo jika tidak ingin run terjadwal. `catchup=False` dan `max_active_runs=1`.

**Perilaku saat dijalankan ulang:** Bronze mengganti record untuk setiap file sumber yang dimuat; SQL Silver dan Gold menghapus lalu membangun kembali isi tabelnya. Gunakan database latihan yang memang menjadi target pipeline, karena hasil sebelumnya dapat diganti. Implementasi ini tidak menyimpan semua versi historis Bronze secara append-only.

### Pemeriksaan hasil

```powershell
docker compose exec postgres psql -U retail -d retail_analytics -c "SELECT pipeline_run_id, status, started_at_utc, finished_at_utc, error_message FROM ops.pipeline_runs ORDER BY started_at_utc DESC LIMIT 5;"
docker compose exec postgres psql -U retail -d retail_analytics -c "SELECT 'order_360' AS table_name, COUNT(*) FROM gold.order_360 UNION ALL SELECT 'customer_daily', COUNT(*) FROM gold.customer_daily UNION ALL SELECT 'product_daily', COUNT(*) FROM gold.product_daily UNION ALL SELECT 'channel_campaign_daily', COUNT(*) FROM gold.channel_campaign_daily UNION ALL SELECT 'executive_kpis_daily', COUNT(*) FROM gold.executive_kpis_daily;"
```

Periksa run terbaru berstatus `success` dan kelima tabel Gold berisi hasil sesuai dataset. Jangan memakai jumlah baris tertentu sebagai patokan tanpa mencocokkannya dengan dataset yang dimuat.

### Menjalankan manual untuk diagnosis

Jika perlu memeriksa pipeline tanpa dashboard Airflow:

```powershell
uv run python -m pipelines.run_pipeline --input data/raw --target local
```

CLI mencatat run dan menjalankan tahapan pipeline, tetapi untuk milestone, orkestrasi dan bukti eksekusi tetap melalui DAG. Jangan menjalankan CLI dan DAG bersamaan karena keduanya menulis ulang tabel yang sama.

## 8. Jalankan API dan UI

Buka terminal PowerShell pertama dari folder utama proyek:

```powershell
$env:ANALYTICS_DB_TARGET = 'local'
$env:NL2SQL_PROVIDER = 'mock'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Biarkan terminal ini berjalan. Buka terminal kedua, masuk ke folder proyek yang sama, lalu:

```powershell
$env:ENGINE_URL = 'http://localhost:8000'
uv run python -m streamlit run ui/app.py --server.address 127.0.0.1 --server.port 8501
```

Buka `http://localhost:8501`. UI memanggil API; UI tidak menjalankan pipeline atau membuat tabel Gold.

Pada terminal ketiga, periksa API:

```powershell
Invoke-RestMethod -Uri 'http://localhost:8000/health'
```

Respons yang diharapkan untuk mode ini adalah `status=ok` dan `provider=MockProvider`. Endpoint `/health` hanya memeriksa aplikasi API; respons sehat belum membuktikan database terhubung atau Gold terisi.

Uji query yang benar-benar membaca Gold:

```powershell
$body = @{
    question = 'What is total net revenue by sales channel?'
    max_rows = 100
    visualization = 'auto'
} | ConvertTo-Json
Invoke-RestMethod -Uri 'http://localhost:8000/v1/analyze' -Method Post -ContentType 'application/json' -Body $body
```

Gunakan pertanyaan yang sama di UI. Contoh lain yang didukung mode mock:

- `How many orders were placed each day?`
- `Which campaigns had the best ROAS?`
- `What was the daily net revenue?`

Mode `mock` tidak membutuhkan API key, tetapi tetap membutuhkan database dengan tabel Gold. Pertanyaannya harus cocok dengan daftar dalam `engine/llm/providers.py`; kode menormalkan kapitalisasi dan spasi, tetapi tidak menerjemahkan pertanyaan bebas.

## 9. Opsional: provider LLM untuk pertanyaan bebas

Jika ingin memakai adapter LLM yang tersedia pada proyek, atur `.env`:

```dotenv
NL2SQL_PROVIDER=openai
OPENAI_API_KEY=ISI_API_KEY_ANDA
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4.1-mini
```

Nama model di atas mengikuti default proyek, bukan hasil pengujian akses model pada akun Anda. Gunakan model yang tersedia pada akun/provider dan kompatibel dengan adapter proyek. Adapter mengirim request ke endpoint `/chat/completions`.

Hentikan API dengan **Ctrl+C**, lalu pada terminal API yang sebelumnya mengatur mode mock:

```powershell
$env:NL2SQL_PROVIDER = 'openai'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Ini diperlukan karena `$env:NL2SQL_PROVIDER='mock'` pada terminal mengalahkan isi `.env`. Pemakaian provider dapat dikenai biaya. Pertanyaan, konteks percakapan, dan schema catalog dikirim ke provider untuk membuat SQL; tinjau isinya sebelum memakai data sensitif.

## 10. Opsional: demo Gold di Neon

Mode ini hanya untuk memakai Gold sample yang sudah tersedia di Neon. PostgreSQL lokal, dataset raw, dan Airflow tidak diperlukan jika hanya menjalankan demo tersebut.

Isi `.env` dengan URL koneksi Neon milik Anda:

```dotenv
ANALYTICS_DB_TARGET=neon
NEON_DATABASE_URL=postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
NL2SQL_PROVIDER=mock
```

Placeholder di atas harus diganti dengan URL koneksi yang benar. Alternatif field `POSTGRES_*` masih didukung untuk Neon, tetapi `NEON_DATABASE_URL` lebih diprioritaskan jika terisi.

Restart API dengan target yang benar:

```powershell
$env:ANALYTICS_DB_TARGET = 'neon'
$env:NL2SQL_PROVIDER = 'mock'
uv run python -m uvicorn engine.api.main:app --host 127.0.0.1 --port 8000
```

Jalankan UI seperti langkah 8. Untuk LLM, gunakan konfigurasi langkah 9. Jangan mengarahkan `LOCAL_DATABASE_URL` ke Neon: kode membatasi nama profil pipeline ke `local`, tetapi alamat tujuan tetap berasal dari konfigurasi URL.

## 11. Perintah pemeriksaan tambahan

```powershell
# Test yang tersedia dalam proyek
uv run python -m pytest -q

# Periksa struktur kedua file Compose tanpa menyalakan service
docker compose -f docker-compose.yml config --quiet
docker compose -f airflow-docker/docker-compose.yml config --quiet

# Benchmark: membaca database dan dapat memanggil LLM sesuai provider aktif
uv run python -m engine.benchmarks.run_benchmark
```

Test generator tidak dikoleksi ketika paket `pipelines.generator` tidak tersedia, sesuai `tests/pipelines/conftest.py`. Lulus unit test tidak membuktikan seluruh DAG atau koneksi LLM berjalan.

## 12. Troubleshooting

| Masalah | Yang diperiksa |
|---|---|
| `uv` tidak dikenali | Instal UV dan buka ulang terminal agar PATH diperbarui. |
| Docker daemon tidak bisa dihubungi | Jalankan Docker Desktop dan tunggu sampai siap. |
| Port 5432 sudah dipakai | Periksa PostgreSQL atau container lain. Jika mengganti port host pada Compose utama, sesuaikan URL lokal di `.env` dan URL Airflow juga. |
| Port 8000/8501 sudah dipakai | Hentikan API/UI lama atau container terkait. Jika port API diubah, ubah `ENGINE_URL` pada terminal UI. |
| `PostgreSQL connection failed` | Periksa status PostgreSQL, URL target, user/password, dan environment variable terminal. Windows memakai `localhost`; Airflow memakai `host.docker.internal`. |
| `relation ops.pipeline_runs does not exist` | Jalankan inisialisasi schema pada langkah 6 sebelum trigger DAG. |
| `relation gold... does not exist` atau hasil kosong | Pastikan target engine sesuai database yang sudah dibangun dan DAG selesai. Mode Neon membutuhkan Gold sample yang sudah tersedia. |
| DAG tidak muncul | Periksa log container Airflow dan mount folder `../dags`. Jalankan `docker exec retail-airflow airflow dags list-import-errors`. |
| `No raw files found` | Periksa struktur `data/raw` dan mount `../data`; default `RETAIL_RAW_DIR` di container adalah `/opt/project/data/raw`. |
| `Mock provider does not have a benchmark query...` | Gunakan contoh pertanyaan persis dari langkah 8 atau aktifkan provider LLM. |
| API gagal karena `OPENAI_API_KEY` | Gunakan key yang valid untuk provider LLM atau set `NL2SQL_PROVIDER=mock`, lalu restart API. |
| HTTP 503 / `Analysis failed` | Baca log terminal API; pesan HTTP sengaja tidak memuat detail provider/database. |
| UI tidak bisa menghubungi engine | Pastikan API hidup dan Engine URL pada sidebar UI adalah `http://localhost:8000` untuk mode UV. |
| Quality gate gagal | Baca log task dan metadata `ops.quality_checks`/`ops.reconciliation_results`; perbaiki sumber atau transformasi sebelum menjalankan ulang. |

## 13. Menghentikan dan menjalankan kembali

Hentikan API/UI dengan **Ctrl+C** pada masing-masing terminal. Hentikan container tanpa menghapus volume:

```powershell
docker compose -f airflow-docker/docker-compose.yml stop
docker compose stop postgres
```

Untuk menjalankan kembali container yang sudah dibuat:

```powershell
docker compose start postgres
docker compose -f airflow-docker/docker-compose.yml start
```

Lalu jalankan API/UI seperti langkah 8. Setelah perubahan konfigurasi Compose, gunakan kembali `up -d` agar perubahan diterapkan. Hindari `down -v` kecuali memang ingin menghapus data volume PostgreSQL atau metadata Airflow.

## 14. Struktur proyek dan batas verifikasi

```text
airflow-docker/       Image dan Compose Airflow
dags/dag.py          Orkestrasi tujuh task pipeline
data/raw/            Dataset lokal
database/schemas/    DDL Bronze, Silver, Gold, Ops
pipelines/           Ingestion, transformasi, quality checks, CLI
shared/db.py         Koneksi PostgreSQL dan pemilihan profil
engine/              NL-to-SQL, eksekusi SQL, API, benchmark
ui/                  UI Streamlit dan HTTP client
tests/               Test proyek
evidence/            Bukti DAG dan contoh query
```

Panduan ini diperiksa terhadap source code, opsi CLI `--help`, struktur dataset, dan validitas kedua file Compose. Python pada `.venv` yang diperiksa adalah 3.11.16. Pemeriksaan ini tidak menjalankan instalasi ulang, menyalakan service, menulis ke database, memicu DAG, memanggil LLM, atau memverifikasi UI secara langsung. Konfigurasi Docker/Airflow bawaan ditujukan untuk latihan lokal; panduan ini bukan prosedur deployment produksi.
