# ChileEduc masterfile: extraction
#
# Flow 1 of the project: the MINEDUC files, downloaded, unpacked and given the same
# columns and types every year. Nothing is dropped: data/clean/ has every record MINEDUC
# published. Flow 2 (who goes into the study) is code/masterfile_analysis.py.
#
# Libraries and paths are defined here and only here. Each script in code/extraction/
# is run with source(), just like in R: it sees every library, path and setting
# defined in this file.
#
# To skip a step, comment out its source() line at the bottom.
# Every script skips the files it already made, so rerunning only does new work.
#
#     python code/masterfile_extraction.py

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
ROOT = Path(__file__).resolve().parents[1]  # the repo (this file is in code/)
DATA = ROOT / "data"                        # not in git: it is far too big

RAW = DATA / "raw"                    # 1_scraping:    downloaded .zip/.rar files
INTERMEDIATE = DATA / "intermediate"  # 2_extraction:  one parquet per year, columns as published
DOCS = DATA / "docs"                  # 2_extraction:  PDFs and codebooks found inside the archives
CLEAN = DATA / "clean"                # 3_unification: one parquet per year, same columns every year
SAMPLE = DATA / "sample"              # 5_sampling:    a sample of students
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
SEED = 42  # the same in masterfile_analysis.py, so both flows sample the same students


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


source("extraction/0_functions.py")
source("extraction/1_scraping.py")
source("extraction/2_extraction.py")
source("extraction/3_unification.py")
source("extraction/4_dictionary.py")
source("extraction/5_sampling.py")

# [End-of-file] ----
