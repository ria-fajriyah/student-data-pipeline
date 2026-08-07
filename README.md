# 🎓 Student Data Warehouse & Data Mart Pipeline

Proyek ini membangun **data pipeline (ETL)** berlapis menggunakan **Apache Airflow** untuk mengonsolidasikan data mahasiswa yang tersebar di berbagai sumber (data lake) menjadi satu data mart terpadu yang siap dianalisis, lengkap dengan mekanisme **data freshness check** untuk menjamin kualitas data sebelum diproses.

## 🧱 Arsitektur & Alur Data

Pipeline dibangun mengikuti pendekatan **layered architecture** (Data Lake → Data Warehouse → Data Mart):

```
                    Data Lake (datalake)
   ┌───────────┬──────────────┬───────────────┬──────────────┐
   │ biodata   │   details    │ field_study   │ specialization│  tuition_fees
   └─────┬─────┴──────┬───────┴───────┬───────┴───────┬──────┘
         ▼             ▼               ▼               ▼
   ┌─────────────────────────────────────────────────────────┐
   │        5 Airflow DAGs (dw_student_*)                    │
   │        extract_load(): extract → load ke datawarehouse  │
   └─────────────────────────────────────────────────────────┘
         │             │               │               │
         ▼             ▼               ▼               ▼
              Data Warehouse (datawarehouse.l1_student_*)
                              │
                              ▼
              ┌───────────────────────────────┐
              │   DAG all_students             │
              │   1. check_table_updates()     │
              │      → validasi data_updated_at│
              │        semua tabel sumber      │
              │   2. extract_load()            │
              │      → JOIN & agregasi data    │
              └───────────────────────────────┘
                              │
                              ▼
              Data Mart (datamart.l1_all_students)
```

### Tahapan Pipeline

1. **Layer Data Warehouse (5 DAG terpisah)** — masing-masing DAG (`dw_student_biodata`, `dw_student_details`, `dw_student_field_study`, `dw_student_specialization`, `dw_student_tuition_fees`) melakukan extract dari skema `datalake` dan load ke skema `datawarehouse`, dengan penambahan kolom `data_updated_at` sebagai penanda waktu pembaruan data.
2. **Layer Data Mart (DAG `all_students`)**:
   - **Data Freshness Check** — memvalidasi bahwa seluruh tabel sumber (`l1_student_biodata`, `l1_student_details`, `l1_student_field_study`, `l1_student_specialization`, `l1_student_tuition_fees`) sudah diperbarui pada hari yang sama sebelum proses lanjut, menggunakan parsing SQL (`sqlparse`) untuk mengekstrak nama tabel dari query secara dinamis.
   - **Extract & Transform** — menggabungkan (JOIN) data biodata, detail akademik, bidang studi, dan spesialisasi mahasiswa, serta menghitung total biaya kuliah aktual (`fees - discount_on_fees`) per mahasiswa.
   - **Load** — hasil akhir dimuat ke tabel `datamart.l1_all_students` sebagai satu tabel siap pakai untuk pelaporan/analisis.
3. **Cleanup** — setiap DAG memiliki task `delete_xcom` untuk membersihkan XCom setelah eksekusi selesai, menjaga metadata database Airflow tetap ringan.

## 📁 Struktur File

| File | Deskripsi |
|---|---|
| `all_students.py` | DAG data mart — validasi kesegaran data, join seluruh tabel warehouse, load ke `datamart.l1_all_students` |
| `student_biodata.py` | DAG data warehouse — biodata mahasiswa (nama, tanggal lahir) |
| `student_details.py` | DAG data warehouse — detail akademik (tahun masuk, tahun lulus) |
| `student_field_study.py` | DAG data warehouse — bidang studi & semester berjalan |
| `student_specialization.py` | DAG data warehouse — spesialisasi mahasiswa |
| `student_tuition_fees.py` | DAG data warehouse — biaya kuliah & diskon |
| `student_*.csv` | Sample dataset mentah untuk masing-masing entitas |

## 🗃️ Struktur Data

| Sumber | Kolom Utama |
|---|---|
| `student_biodata` | Student ID, Student Name, Date of Birth |
| `student_details` | Student ID, Student Name, Year of Admission, Expected Year of Graduation |
| `student_field_study` | Student ID, Field of Study, Current Semester |
| `student_specialization` | Student ID, Student Name, Specialization |
| `student_tuition_fees` | Student ID, Fees, Discount on Fees |

**Output akhir (`l1_all_students`)**: `student_id, name, field_of_study, specialization, year_of_admission, expected_year_of_graduation, actual_fees, data_updated_at`

## ⚙️ Tools & Tech Stack

- **PostgreSQL** — data lake, data warehouse, dan data mart
- **Apache Airflow** — orkestrasi & penjadwalan ETL (`PythonOperator`, `PostgresHook`, `XCom`)
- **Python** — `pandas` (transformasi data), `sqlparse` (parsing nama tabel dari query SQL untuk validasi otomatis)

## 🚀 Cara Menjalankan

1. Siapkan PostgreSQL dengan skema `datalake`, `datawarehouse`, dan `datamart`, serta Airflow connection `local_post`.
2. Letakkan seluruh DAG di folder `dags/` Airflow.
3. Jalankan (trigger) 5 DAG data warehouse (`dw_student_*`) terlebih dahulu.
4. Jalankan DAG `all_students` — DAG ini akan otomatis memvalidasi bahwa semua tabel sumber sudah diperbarui hari itu sebelum melanjutkan proses join dan load ke data mart.

## 📌 Fitur Utama

- **Modular ETL** — setiap entitas data punya DAG terpisah, memudahkan maintenance dan debugging.
- **Data Quality Check** — validasi otomatis kesegaran data sumber sebelum data mart dibangun, mencegah data mart terisi data usang.
- **Dynamic SQL Parsing** — ekstraksi nama tabel dari query secara terprogram menggunakan `sqlparse`, bukan hardcode manual.
- **Idempotent Load** — strategi `if_exists="replace"` menjaga data mart selalu merepresentasikan kondisi terbaru.

---
*README ini dibuat untuk mendokumentasikan alur data engineering pada proyek student data warehouse & data mart.*
