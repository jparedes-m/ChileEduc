# Chilean Education System for School Absences and Early Truancy

## General Description

This document provides an overview of three key datasets related to the Chilean education system, focusing on student attendance and academic performance in various programs and schools registered with the Chilean Ministry of Education. These datasets allow for the analysis of educational trends and the impact of the COVID-19 pandemic.

The datasets include:

1. **Kinder**: Annual attendance data for children in early education programs.
2. **Assistance**: Monthly attendance data for students in schools.
3. **Performance**: Annual academic performance data of students.

## To-Do List
- [x] Master file for data conformation, where it does not repeat every time the working directory.
- [ ] We need to map geographically the best bus routes from kids house to school with their frequencies.

## Dataset Details

A PDF version of this overview is in [`docs/Data Organization.pdf`](docs/Data%20Organization.pdf).

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

The code runs on your own computer; GitHub only keeps the versions of the code (the data never goes there).

```bash
pip install -r requirements.txt
python masterfile.py
```

`masterfile.py` loads the libraries, sets the paths and settings, and then runs each script of `code/` with
`source()`, like in R. To skip a step, comment out its `source()` line. Every script skips the files it already
made, so rerunning only processes new years. There are two flows:

**Flow 1, data (`code/`)**: the MINEDUC files as published, with the same columns, names and types every year.
No record is dropped.

| Script | What it does | Output |
|--------|--------------|--------|
| `0_functions.py` | Small functions used by several scripts | |
| `1_scraping.py` | Downloads the .zip/.rar files from datosabiertos.mineduc.cl | `data/raw/<dataset>/performance_2015.zip`, `assistance_2015_03.rar`, ... |
| `2_extraction.py` | Unpacks them and saves the data as parquet; keeps the PDFs and codebooks | `data/intermediate/<dataset>/*.parquet`, `data/docs/<dataset>/` |
| `3_unification.py` | Same columns, names and types every year | `data/clean/<dataset>/*.parquet` |
| `4_dictionary.py` | Data dictionary: type, description, codes, years and coverage of every variable | [`docs/dictionary/<dataset>.md`](docs/dictionary) |
| `5_sampling.py` | Sample of students (2%, 0.5% in assistance) with all their rows | `data/sample/<dataset>_sample.parquet` |

**Flow 2, analysis data (`code/analysis/`)**: who goes into the study.

| Script | What it does | Output |
|--------|--------------|--------|
| `analysis/1_selection.py` | One row per student and year, kinder (NT2) to 4° medio of regular education for children and youth. Drops special and adult education, the records of withdrawn or transferred-out students, and implausible ages; adds age and rezago | `data/analysis/panel/panel_<year>.parquet`, `data/analysis/withdrawals/` |
| `analysis/2_cohorts.py` | Follows the cohorts in `COHORTS`: kinder 2011-2015 to 8° básico, and 1° básico 2002-2012 to 4° medio | `data/analysis/cohorts.parquet`, `data/analysis/cohort_panel.parquet` |

In the panel, `grado` is 0 for kinder, 1-8 for 1° to 8° básico and 9-12 for 1° to 4° medio; the normal age is
`5 + grado` and `rezago` is how many years a student is above it. In the cohorts, `finished` means promoted in the
last grade, `left` means not finished and not seen in the last year of the data, and `censored` means still in
school at the end of the data.

Rows keep their original frequency: student x month in assistance, student x year in kinder and performance.

Unification needs `config/data_mapping.xlsx`, and the dictionary takes the variable descriptions from
`config/descriptions/` (see `config/README.md`). The MINEDUC documentation behind them is in [`docs/mineduc/`](docs/mineduc). RAR files need 7-Zip or unrar installed.
The original scripts are kept in `vintage/`.

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
