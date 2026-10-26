# ChileEduc masterfile: analysis
#
# Flow 2 of the project: who goes into the study, built from the clean data of flow 1
# (code/masterfile_extraction.py, which has to run first). The result is a panel with one
# row per child and year, from kinder (NT2) to 4° medio of regular education for children
# and youth, with their grade, school, course, GPA, final status and yearly attendance, and
# the cohorts followed through it.
#
# Libraries and paths are defined here and only here. Each script in code/analysis/ is run
# with source(), just like in R: it sees every library, path and setting defined in this file.
#
# To skip a step, comment out its source() line at the bottom.
# Every script skips the years it already made, so rerunning only does new work.
#
#     python code/masterfile_analysis.py

# [0] Libraries ----
import re
import time
from pathlib import Path

import polars as pl

# [1] Paths ----
ROOT = Path(__file__).resolve().parents[1]  # the repo (this file is in code/)
DATA = ROOT / "data"                        # not in git: it is far too big

CLEAN = DATA / "clean"        # input: the clean data of flow 1
ANALYSIS = DATA / "analysis"  # 1_selection, 2_attendance, 3_cohorts: the data of the study
SAMPLE = DATA / "sample"      # 3_cohorts: the cohort panel for the students of the sample

# [2] Settings ----
# grado: 0 kinder, 1-8 = 1° to 8° básico, 9-12 = 1° to 4° medio. Normal age = 5 + grado.
REZAGO_MIN, REZAGO_MAX = -1, 3  # keep students from 1 year under to 3 years over the normal age for their grade
COHORTS = {  # each one becomes a column of the final data, holding the year the child entered it
    "cohort_kinder": {"start_grade": 0, "end_grade": 8, "entry_years": list(range(2011, 2016))},   # kinder to 8° básico
    "cohort_basica": {"start_grade": 1, "end_grade": 12, "entry_years": list(range(2002, 2013))},  # 1° básico to 4° medio
}
SAMPLE_FRACTION = 0.02  # share of students in the sample of the cohort panel
SEED = 42  # the same in masterfile_extraction.py, so both flows sample the same students


# [3] Run ----
def source(script):
    """Run a script of code/ as if its lines were written here, like source() in R, and time it."""
    path = ROOT / "code" / script
    print(f"\nScript: {script} ...")
    start = time.time()
    exec(compile(path.read_text(encoding="utf-8"), path, "exec"), globals())
    minutes, seconds = divmod(round(time.time() - start), 60)
    hours, minutes = divmod(minutes, 60)
    print(f"Done in {hours}h {minutes}min {seconds}s")


source("extraction/0_functions.py")  # period_of() and in_sample(), shared with flow 1
source("analysis/1_selection.py")
source("analysis/2_attendance.py")
source("analysis/3_cohorts.py")

# [End-of-file] ----
