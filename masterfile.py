# ChileEduc masterfile
#
# Runs the whole project from top to bottom. Libraries and paths are defined
# here and only here. Each script in code/ is run with source(), just like in R:
# it sees every library, path and setting defined in this file.
#
# Two flows:
#   1. Data (code/): the MINEDUC files, downloaded, unpacked and given the same columns and
#      types every year. Nothing is dropped: data/clean/ has every record MINEDUC published.
#   2. Analysis data (code/analysis/): who goes into the study. Regular education from kinder
#      to 4° medio, without withdrawn students, one row per student and year, and the cohorts.
#
# To skip a step, comment out its source() line at the bottom.
# Every script skips the files it already made, so rerunning only does new work.

# [0] Libraries ----
import codecs
import logging
import re
import shutil
import tempfile
import time
import unicodedata
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import patoolib  # .rar files; needs 7-Zip or unrar installed on the computer
import polars as pl
import requests
from bs4 import BeautifulSoup

logging.getLogger("patool").setLevel(logging.WARNING)  # hide patool's routine notes, keep its warnings

# [1] Paths ----
ROOT = Path(__file__).resolve().parent  # the folder of this file (the repo)
DATA = ROOT / "data"                    # not in git: it is far too big

RAW = DATA / "raw"                    # 1_scraping:    downloaded .zip/.rar files
INTERMEDIATE = DATA / "intermediate"  # 2_extraction:  one parquet per year, columns as published
DOCS = DATA / "docs"                  # 2_extraction:  PDFs and codebooks found inside the archives
CLEAN = DATA / "clean"                # 3_unification: one parquet per year, same columns every year
SAMPLE = DATA / "sample"              # 5_sampling:    a sample of students
ANALYSIS = DATA / "analysis"          # analysis/:     the panel and the cohorts of the study
MAPPING = ROOT / "config" / "data_mapping.xlsx"  # column names of each year
DESCRIPTIONS = ROOT / "config" / "descriptions"  # what each variable means, from the MINEDUC record layouts
DICTIONARY = ROOT / "docs" / "dictionary"        # 4_dictionary: data dictionary (in git)

# [2] Settings ----
DATASETS = {
    "kinder": {"url": "https://datosabiertos.mineduc.cl/matricula-educacion-parvularia/", "monthly": False},
    "assistance": {"url": "https://datosabiertos.mineduc.cl/asistencia-declarada-mensual-2/", "monthly": True},
    "performance": {"url": "https://datosabiertos.mineduc.cl/rendimiento-por-estudiante-2/", "monthly": False},
}
MIN_YEAR_SHARE = 0.5  # also keep published columns missing from the mapping if they appear in half the years
SAMPLE_FRACTION = {"kinder": 0.02, "assistance": 0.005, "performance": 0.02}  # share of students
SEED = 42

# Flow 2. grado: 0 kinder, 1-8 = 1° to 8° básico, 9-12 = 1° to 4° medio. Normal age = 5 + grado.
REZAGO_MIN, REZAGO_MAX = -1, 3  # keep students from 1 year under to 3 years over the normal age for their grade
COHORTS = {
    "kinder_8basico": {"start_grade": 0, "end_grade": 8, "entry_years": list(range(2011, 2016))},
    "1basico_4medio": {"start_grade": 1, "end_grade": 12, "entry_years": list(range(2002, 2013))},
}


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


source("0_functions.py")

# Flow 1: data
source("1_scraping.py")
source("2_extraction.py")
source("3_unification.py")
source("4_dictionary.py")
source("5_sampling.py")

# Flow 2: analysis data
source("analysis/1_selection.py")
source("analysis/2_cohorts.py")

# [End-of-file] ----
