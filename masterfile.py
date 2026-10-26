"""ChileEduc masterfile.

The only entry point of the project: checks the libraries, defines every path
and setting, and runs the pipeline steps in order.

    python masterfile.py                                  # every step in STEPS
    python masterfile.py --steps extraction unification   # only some steps
    python masterfile.py --datasets performance           # only some datasets
    python masterfile.py --overwrite                      # redo existing outputs
"""

# [0] Libraries ----
import argparse
import importlib.util
import logging
import os
import sys
import time
from pathlib import Path

REQUIRED = {  # import name: pip package
    "polars": "polars",
    "fastexcel": "fastexcel",
    "requests": "requests",
    "bs4": "beautifulsoup4",
    "patoolib": "patool",
}
missing = [pkg for mod, pkg in REQUIRED.items() if importlib.util.find_spec(mod) is None]
if missing:
    sys.exit(f"Missing libraries. Run: pip install {' '.join(missing)}")

from pipeline import extraction, sampling, scraping, unification  # noqa: E402

# [1] Paths ----
# The only place in the project where paths are defined. The data lives outside
# the repo (it is far too large for git): set CHILEEDUC_DATA to its folder,
# e.g. C:/Users/jparedesm/Dropbox/ChileEduc/data, or leave it unset to use ./data.
ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("CHILEEDUC_DATA", ROOT / "data"))

RAW_DIR = DATA_DIR / "raw"                     # downloaded archives
INTERMEDIATE_DIR = DATA_DIR / "intermediate"   # one Parquet per period, columns as published
CLEAN_DIR = DATA_DIR / "clean"                 # one Parquet per period, unified schema
SAMPLE_DIR = DATA_DIR / "sample"               # student sample for exploration
MAPPING_FILE = ROOT / "config" / "data_mapping.xlsx"
LOG_DIR = ROOT / "logs"

# [2] Settings ----
SOURCE = "https://datosabiertos.mineduc.cl"
DATASETS = {
    "kinder": {"url": f"{SOURCE}/matricula-educacion-parvularia/", "monthly": False},
    "assistance": {"url": f"{SOURCE}/asistencia-declarada-mensual-2/", "monthly": True},
    "performance": {"url": f"{SOURCE}/rendimiento-por-estudiante-2/", "monthly": False},
}
STEPS = ["scraping", "extraction", "unification", "sampling"]
SAMPLE_FRACTION = 0.02  # share of students, the same students in every dataset
SEED = 42


# [3] Run ----
def run_step(step: str, datasets: dict, overwrite: bool) -> None:
    if step == "scraping":
        scraping.run(datasets, RAW_DIR, overwrite)
    elif step == "extraction":
        extraction.run(datasets, RAW_DIR, INTERMEDIATE_DIR, overwrite)
    elif step == "unification":
        unification.run(datasets, INTERMEDIATE_DIR, CLEAN_DIR, MAPPING_FILE, overwrite)
    elif step == "sampling":
        sampling.run(datasets, CLEAN_DIR, SAMPLE_DIR, SAMPLE_FRACTION, SEED, overwrite)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    parser.add_argument("--datasets", nargs="+", choices=list(DATASETS), default=list(DATASETS))
    parser.add_argument("--overwrite", action="store_true", help="redo outputs that already exist")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_DIR / f"run_{time.strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"),
        ],
    )
    log = logging.getLogger("masterfile")
    log.info("Data folder: %s", DATA_DIR)

    datasets = {name: DATASETS[name] for name in args.datasets}
    for step in [s for s in STEPS if s in args.steps]:
        start = time.perf_counter()
        log.info("== %s ==", step)
        run_step(step, datasets, args.overwrite)
        log.info("== %s done in %.1f min ==", step, (time.perf_counter() - start) / 60)


if __name__ == "__main__":
    main()
