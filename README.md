# Chilean Education System for School Absences and Early Truancy

## General Description

This document provides an overview of three key datasets related to the Chilean education system, focusing on student attendance and academic performance in various programs and schools registered with the Chilean Ministry of Education. These datasets allow for the analysis of educational trends and the impact of the COVID-19 pandemic.

The datasets include:

1. **Kinder**: Annual attendance data for children in early education programs.
2. **Assistance**: Monthly attendance data for students in schools.
3. **Performance**: Annual academic performance data of students.

## Research Question

Do schools that sort students into courses by their grades (tracking) change how those students do? Chilean
schools with two or more courses per grade can put the students with the best GPA together. The study finds
the schools and years that do it, the GPA threshold that separates their courses, and compares the students
just below it (sent to the lower course) with those just above it (sent to the higher course): a regression
discontinuity. Both groups are followed in the next years: GPA, attendance, repetition, course and school
changes, finishing school, and moving to adult education.

### Design

1. **The children**: two cohorts, followed with `mrun` across the three datasets: kinder 2011-2015 up to
   8° básico, and 1° básico 2002-2012 up to 4° medio. Regular education for children and youth only, without
   withdrawn students (`code/analysis/1_selection.py` to `3_cohorts.py`).
2. **Which schools track**: in each school, teaching code, grade, shift and year with 2+ courses, how much of
   the spread of last year's GPA is between courses (`eta2`), against random reassignments of the same
   students (`p_value`). Only courses of the same shift and teaching code are compared, so they are alike
   (`4_tracking.py`).
3. **Telling sorting from inertia**: courses often move up a grade together, and then their GPAs differ
   because they had different teachers, not because the school sorted them. `persistence` says how many
   students stayed with their classmates; the sorting that matters is in the groups that were remixed
   (`reshuffled`).
4. **The threshold**: for each pair of courses next to each other in a tracked group, the prior GPA that best
   splits them, and how sharp it is (`sharpness`, the share of students on the right side of it).
5. **Just below and just above**: the students within `RD_BANDWIDTH` GPA points of a sharp threshold, with
   their outcomes that year and one and two years later, and how they finished school (`5_rd_sample.py`).
6. **The effect**: being above the threshold makes it more likely to be in the higher course, but not certain
   (a fuzzy discontinuity), so the effect of the higher course is estimated with being above the threshold as
   an instrument, comparing students of the same threshold (`6_rd_estimates.py`). Characteristics fixed before
   the assignment (sex, age, having repeated) should not jump at the threshold.

### Things to keep in mind

- **GPA is set by each teacher**, so a course with a harsher teacher has lower GPAs. GPA works as the running
  variable (it is what schools sort on), but as an outcome it mixes the student and the teacher. A standardized
  test (SIMCE, requested to the Agencia de Calidad de la Educación) would be a cleaner outcome.
- **Thresholds are estimated**, so students close to them should be compared within the same group (group
  fixed effects) and the results checked with other bandwidths and sharpness levels.
- **GPA has one decimal**, so the distance to the threshold only takes a few values (-0.2, -0.1, 0, 0.1, ...).
  Very narrow bandwidths leave too few of them on each side; the estimates use 0.2, 0.3 and 0.5.
- **The definition of a group changes in 2013**: from then on courses of different shifts are not compared,
  because the shift (`cod_jor`) is only published from 2013. Before, a morning and an afternoon course of the
  same grade count as one group.
- **Prior GPA 0 means withdrawn**, and is treated as missing. **2020 and 2021** have no attendance data and
  lenient promotion rules.

### Progress

- [x] Master file for data conformation, where it does not repeat every time the working directory.
- [x] The three MINEDUC datasets, clean, documented and with a data dictionary (flow 1).
- [x] Cohorts from kinder and from 1° básico, with how each child ended, including the move to adult education.
- [x] Which school-grade-years track, the GPA thresholds, and the students just below and above them.
- [x] Run tracking on the full data: about 21,000 groups with 2+ courses a year; 36% of them sort by GPA in
      2003 and 21% in 2023, but only about a fifth of those (falling over time) remixed their courses.
- [x] Regression discontinuity estimates, with a fixed effect per threshold and several bandwidths.
- [ ] Look at how common tracking is by grade and type of school, and why it falls over time.
- [ ] Robustness: other sharpness levels, a group definition without the shift in every year, placebo thresholds.
- [ ] Request student-level SIMCE scores, as an outcome not set by teachers.
- [ ] Map geographically the best bus routes from kids' houses to school, with their frequencies.

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
python code/masterfile_extraction.py   # flow 1: the MINEDUC data, clean
python code/masterfile_analysis.py     # flow 2: the data of the study
```

Each masterfile loads the libraries, sets the paths and settings, and then runs its scripts with `source()`,
like in R. To skip a step, comment out its `source()` line. Every script skips the files it already made, so
rerunning only processes new years.

**Flow 1, extraction (`code/masterfile_extraction.py`, scripts in `code/extraction/`)**: the MINEDUC files as
published, with the same columns, names and types every year. No record is dropped.

| Script | What it does | Output |
|--------|--------------|--------|
| `0_functions.py` | Small functions used by several scripts (also by flow 2) | |
| `1_scraping.py` | Downloads the .zip/.rar files from datosabiertos.mineduc.cl | `data/raw/<dataset>/performance_2015.zip`, `assistance_2015_03.rar`, ... |
| `2_extraction.py` | Unpacks them and saves the data as parquet; keeps the PDFs and codebooks | `data/intermediate/<dataset>/*.parquet`, `data/docs/<dataset>/` |
| `3_unification.py` | Same columns, names and types every year | `data/clean/<dataset>/*.parquet` |
| `4_dictionary.py` | Data dictionary: type, description, codes, years and coverage of every variable | [`docs/dictionary/<dataset>.md`](docs/dictionary) |
| `5_sampling.py` | Sample of students (2%, 0.5% in assistance) with all their rows | `data/sample/<dataset>_sample.parquet` |

Rows keep their original frequency: student x month in assistance, student x year in kinder and performance.

**Flow 2, analysis (`code/masterfile_analysis.py`, scripts in `code/analysis/`)**: who goes into the study.

| Script | What it does | Output |
|--------|--------------|--------|
| `1_selection.py` | One row per student and year, kinder (NT2) to 4° medio of regular education for children and youth. Drops special and adult education, the records of withdrawn or transferred-out students (kept in a withdrawals table) and implausible ages; adds age and rezago | `data/analysis/panel/`, `data/analysis/withdrawals/` |
| `2_attendance.py` | Yearly attendance of each student from the monthly assistance data | `data/analysis/attendance/` |
| `3_cohorts.py` | Follows the cohorts in `COHORTS` (kinder 2011-2015 to 8° básico, 1° básico 2002-2012 to 4° medio), joins the attendance, and says how each child ended | `data/analysis/cohort_panel.parquet`, `data/analysis/cohorts.parquet` |
| `4_tracking.py` | Which school-grade-shift-years sort students into courses by last year's GPA, whether their courses were remixed, and the GPA threshold between each pair of courses | `data/analysis/tracking/groups_<year>`, `courses_<year>`, `thresholds_<year>` |
| `5_rd_sample.py` | The students within `RD_BANDWIDTH` of a sharp threshold, with their outcomes that year, one and two years later, and at the end of school | `data/analysis/rd_sample.parquet` |
| `6_rd_estimates.py` | The effect of being sent to the higher course: first stage, reduced form and instrumental variables, local linear on each side of the threshold, with a fixed effect per threshold and errors clustered by school, for several bandwidths, with all thresholds and with remixed courses only; balance checks; plots | [`results/rd_estimates.csv`](results), [`results/rd_plots.png`](results) |

The tracking settings (course size, permutations, significance, bandwidth, sharpness) are in
`code/masterfile_analysis.py`. Set `TRACKING_SCHOOL_FRACTION = 0.05` to try it fast on 5% of the schools.

**The final data is `data/analysis/cohort_panel.parquet`**: one row per child and year, for every child in a
cohort, with

- the cohorts: `cohort_kinder` (year the child was in kinder, for kinder 2011-2015 followed to 8° básico) and
  `cohort_basica` (year the child entered 1° básico, for 2002-2012 followed to 4° medio). Each is empty when that
  year is not part of that cohort; a child in kinder in 2011 is in both from 1° básico on
- who and when: `year`, `mrun`, `gen_alu`, `fec_nac_alu`, `edad`, `rezago`
- where: `grado` (0 kinder, 1-8 = 1° to 8° básico, 9-12 = 1° to 4° medio), `rbd` (or `id_estab_parv` and
  `origen` in kinder), `cod_ense`, `let_cur` (the course), `cod_tip_cur`, `cod_jor`, `cod_depe`, `rural_rbd`, `cod_com_rbd`
- how it went: `prom_gral` (GPA, missing when 0), `asistencia` (yearly %, from performance), `sit_fin`
  (P promoted, R failed), `int_alu`, `n_registros` (records the student had in performance that year)
- from the monthly attendance data (none in 2020 and 2021): `asis_anual` (0 to 1), `asis_dias_asistidos`,
  `asis_dias_trabajados`, `asis_meses`, `asis_primer_mes` and `asis_ultimo_mes` (first and last month with a
  record), `asis_n_colegios` and `asis_n_cursos` (more than 1 means the student changed school or course that year)

and `data/analysis/cohorts.parquet` has one row per child and cohort: `finished` (promoted in the last grade),
`on_time`, `left` (not finished and not seen in the last year of the data), `exit_to` (where those who left
went: `adult_education`, `special_education`, `regular_outside_panel` or `not_seen_again`), `censored` (still in
school at the end of the data), `repetitions`, `withdrawals`, `years_seen`, `last_grade`.

`data/analysis/rd_sample.parquet` has one row per student and threshold: `distance` (prior GPA minus the
threshold), `above`, `in_high` (in the higher course), `threshold`, `sharpness`, the group's `eta2`, `p_value`,
`persistence` and `reshuffled`, `group` and `low_rank` (to compare within a group), own and other course size
and mean prior GPA, the student's `gen_alu`, `edad`, `rezago`, `repeater`, and their outcomes: `prom_gral`,
`asistencia`, `sit_fin` and `asis_anual` that year; the same plus `grado`, `same_school` and `course_rank` one
and two years later (`_t1`, `_t2`); and `finished`, `on_time` and `exit_to` for the cohort of 1° básico.

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
