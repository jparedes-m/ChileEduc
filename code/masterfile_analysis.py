# ChileEduc masterfile: analysis
#
# Flow 2 of the project: who goes into the study, built from the clean data of flow 1
# (code/masterfile_extraction.py, which has to run first). The result is a panel with one
# row per child and year, from kinder (NT2) to 4° medio of regular education for children
# and youth, with their grade, school, course, GPA, final status and yearly attendance, and
# the cohorts followed through it.
#
# Then it finds the school-grade-years that sort students into courses by GPA, the GPA threshold
# between courses, and the students just below and just above it, and estimates the effect of
# being sent to the higher course (a regression discontinuity).
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
import warnings
from pathlib import Path

import matplotlib
import numpy as np
import polars as pl
import pyfixest as pf  # regressions with fixed effects, instrumental variables and clustered errors

matplotlib.use("Agg")  # draw plots to files, without opening windows
# A threshold with a single student in the bandwidth cannot be compared with anyone: pyfixest drops it, as it
# should, and warns every time. The number of students used is in results/rd_estimates.csv (column n).
warnings.filterwarnings("ignore", message=r".*singleton fixed effect")
import matplotlib.pyplot as plt  # noqa: E402

# [1] Paths ----
ROOT = Path(__file__).resolve().parents[1]  # the repo (this file is in code/)
DATA = ROOT / "data"                        # not in git: it is far too big

CLEAN = DATA / "clean"        # input: the clean data of flow 1
ANALYSIS = DATA / "analysis"  # 1_selection to 5_rd_sample: the data of the study
SAMPLE = DATA / "sample"      # 3_cohorts: the cohort panel for the students of the sample
RESULTS = ROOT / "results"    # 6_rd_results: tables and plots (in git: they are small summaries)

# [2] Settings ----
# grado: 0 kinder, 1-8 = 1° to 8° básico, 9-12 = 1° to 4° medio. Normal age = 5 + grado.
REZAGO_MIN, REZAGO_MAX = -1, 3  # keep students from 1 year under to 3 years over the normal age for their grade
COHORTS = {  # each one becomes a column of the final data, holding the year the child entered it
    "cohort_kinder": {"start_grade": 0, "end_grade": 8, "entry_years": list(range(2011, 2016))},   # kinder to 8° básico
    "cohort_basica": {"start_grade": 1, "end_grade": 12, "entry_years": list(range(2002, 2013))},  # 1° básico to 4° medio
}
# Tracking (4_tracking) and the regression discontinuity (5_rd_sample, 6_rd_results)
TRACKING_MIN_COURSE_SIZE = 10   # students with a prior GPA a course needs to be compared
TRACKING_PERMUTATIONS = 100     # random reassignments to test sorting against
TRACKING_ALPHA = 0.05           # a group is tracked if sorting is this unlikely under random assignment
RESHUFFLE_MAX = 0.8             # a group was remixed if less than this share stayed with most of their course
TRACKING_SCHOOL_FRACTION = 1.0  # share of schools to run tracking on: 0.05 to try it fast, 1.0 for everything
RD_MIN_SHARPNESS = 0.85         # thresholds that put at least this share of the pair on the right side
RD_BANDWIDTH = 0.5              # GPA points around the threshold kept in the sample of students just below and above
RD_ESTIMATE_BANDWIDTHS = [0.2, 0.3, 0.5]  # bandwidths to estimate with (GPAs have 1 decimal, so 0.1 is too narrow)
RD_MAIN_BANDWIDTH = 0.3         # the one printed in the main table
# The specifications (main, remixed courses only, without students at the threshold, with controls, básica,
# media, 2003-2012, 2013-2023) are listed in SPECS at the top of analysis/6_rd_results.py.

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
source("analysis/4_tracking.py")
source("analysis/5_rd_sample.py")
source("analysis/6_rd_results.py")

# [End-of-file] ----
