# Chilean Education System for School Absences and Early Truancy

## General Description

This document provides an overview of three key datasets related to the Chilean education system, focusing on student attendance and academic performance in various programs and schools registered with the Chilean Ministry of Education. These datasets allow for the analysis of educational trends and the impact of the COVID-19 pandemic.

The datasets include:

1. **Kinder**: Annual attendance data for children in early education programs.
2. **Assistance**: Monthly attendance data for students in schools.
3. **Performance**: Annual academic performance data of students.

## Dataset Details

### 1. Kinder
- Records the yearly attendance of children enrolled in three programs:
  1. **Sistema de Información General de Estudiantes (SIGE)**
  2. **Sistema de Captura JUNJI**
  3. **Sistema de Captura INTEGRA**
- Covered period: **2011 - 2023** (including the pandemic).
- Total observations: **9,879,622**.

### 2. Assistance
- Records the monthly attendance of students enrolled in schools registered with the Chilean Ministry of Education.
- Covered period: **2011 - 2023**, with a gap in **2020 and 2021** due to the pandemic.
- The academic year in Chile consists of **10 months**, running from **March to December**.
- Total observations: **382,055,737**.

### 3. Performance
- Records the yearly academic performance (including attendance) of students enrolled in schools.
- Covered period: **2002 - 2023**.
- Data sources:
  - **Registro de Estudiantes de Chile (RECH)** for **2002 - 2008**.
  - **Sistema de Información General de Estudiantes (SIGE)** for **2009 - 2023**.
- Total observations: **73,668,732**.

## Running the Pipeline

Everything runs from `masterfile.py`, the only file that defines paths and settings.

```bash
pip install -r requirements.txt
export CHILEEDUC_DATA=/path/to/data   # optional, defaults to ./data
python masterfile.py                  # scraping -> extraction -> unification -> sampling
python masterfile.py --steps unification --datasets performance
```

| Step | Module | Output |
|------|--------|--------|
| 1. Scraping | `pipeline/scraping.py` | `raw/<dataset>/<dataset>_<year>[_<month>].zip/.rar` |
| 2. Extraction | `pipeline/extraction.py` | `intermediate/<dataset>/*.parquet` (published columns, as text) and `docs/<dataset>/` (the PDFs and codebooks inside the archives) |
| 3. Unification | `pipeline/unification.py` | `clean/<dataset>/*.parquet`: same columns and types every year, regular education only. Keeps the mapped columns plus any published column present in at least half the years |
| 4. Dictionary | `pipeline/dictionary.py` | [`docs/dictionary/<dataset>.md`](docs/dictionary): every variable with its type, description, published names, years with data and coverage |
| 5. Sampling | `pipeline/sampling.py` | `sample/<dataset>_sample.parquet`: 2% of students (0.5% in assistance) with all their rows |

Rows keep their original frequency: student x month in assistance, student x year in kinder and performance.
The sample draws students rather than rows, with the same draw in every dataset, so a student's whole trajectory
is in the sample and the assistance sample is a subset of the performance one.

Every step skips outputs that already exist, so a rerun only processes new files; `--overwrite` redoes them.
Inputs are never overwritten. Each run writes a log to `logs/`.

Unification needs `config/data_mapping.xlsx` (see `config/README.md`). RAR archives need 7-Zip or unrar installed.

## Exploring the Data

The clean files of each dataset share one schema, so they read as a single table without concatenating them:

```python
import polars as pl

perf = pl.scan_parquet("data/clean/performance/*.parquet")
perf.group_by("year").agg(pl.len(), pl.col("prom_gral").mean()).sort("year").collect()
```

The same files can be queried with SQL through DuckDB, which runs locally with no server:

```python
import duckdb

duckdb.sql("""
    SELECT year, count(*) AS students, avg(prom_gral) AS gpa
    FROM 'data/clean/performance/*.parquet'
    GROUP BY year ORDER BY year
""").pl()
```

The datasets themselves are **not uploaded** due to their **large size**.

## To-Do List
- [x] Master file for data conformation, where it does not repeat every time the working directory.
- [ ] We need to map geographically the best bus routes from kids house to school with their frequencies.
