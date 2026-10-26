"""Step 2: unpack the raw archives and convert the data to Parquet right away.

Archives can be nested (zip inside rar inside zip...). Every CSV found is
transcoded to UTF-8 if needed and written as intermediate/<dataset>/<stem>.parquet.
All columns are kept as text here; typing happens in the unification step.
Everything else in the archive (PDF record layouts, codebooks, frequency tables)
is copied to docs/<dataset>/<archive name>/.
"""

import codecs
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path

import patoolib
import polars as pl

from pipeline.common import normalize, parse_period, period_stem

log = logging.getLogger(__name__)

# CSVs shipped next to the data that are not microdata (frequency tables, codebooks).
SKIP_NAMES = ("frecuencia", "diccionario", "libro de codigo", "readme")
CHUNK = 16 << 20


def unpack(archive: Path, dest: Path) -> None:
    """Extract an archive into dest, then any archive found inside it, recursively."""
    dest.mkdir(parents=True, exist_ok=True)
    if archive.suffix.lower() == ".zip":
        with zipfile.ZipFile(archive) as z:
            z.extractall(dest)
    else:
        patoolib.extract_archive(str(archive), outdir=str(dest), verbosity=-1, interactive=False)
    for inner in sorted(dest.rglob("*")):
        if inner.is_file() and inner.suffix.lower() in (".zip", ".rar"):
            unpack(inner, inner.with_name(inner.name + "_unpacked"))
            inner.unlink()


def is_utf8(path: Path) -> bool:
    decoder = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as f:
        try:
            while chunk := f.read(CHUNK):
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            return False
    return True


def to_utf8(path: Path) -> Path:
    """Return a UTF-8 version of the file. MINEDUC files that are not UTF-8 are Latin-1."""
    if is_utf8(path):
        return path
    out = path.with_name(path.stem + ".utf8.csv")
    with open(path, encoding="latin-1", newline="") as src, open(out, "w", encoding="utf-8", newline="") as dst:
        shutil.copyfileobj(src, dst, CHUNK)
    return out


def detect_separator(path: Path) -> str:
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline()
    return ";" if header.count(";") >= header.count(",") else ","


def csv_to_parquet(csv: Path, target: Path) -> None:
    csv = to_utf8(csv)
    lf = pl.scan_csv(csv, separator=detect_separator(csv), infer_schema=False)
    clean_names = {c: c.lstrip("﻿").strip() for c in lf.collect_schema().names()}
    partial = target.with_name(target.name + ".part")
    lf.rename(clean_names).sink_parquet(partial, compression="zstd")
    partial.replace(target)


def pick_data_files(csvs: list[Path], archive: Path, dataset: str, monthly: bool) -> dict[str, Path]:
    """Assign each CSV to a period, keeping the largest file when several share one."""
    fallback = parse_period(archive.name, monthly)
    chosen: dict[str, Path] = {}
    for csv in csvs:
        if any(word in normalize(csv.name) for word in SKIP_NAMES):
            continue
        period = parse_period(csv.name, monthly) or fallback
        if period is None:
            log.warning("[%s] no period for %s in %s, skipped", dataset, csv.name, archive.name)
            continue
        stem = period_stem(dataset, *period)
        if stem in chosen:
            small, big = sorted((chosen[stem], csv), key=lambda p: p.stat().st_size)
            log.warning("[%s] two files for %s; keeping %s, ignoring %s", dataset, stem, big.name, small.name)
            csv = big
        chosen[stem] = csv
    return chosen


def save_docs(files: list[Path], tmp: Path, docs_dir: Path) -> None:
    for f in files:
        target = docs_dir / f.relative_to(tmp)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, target)


def extract_archive(archive: Path, dataset: str, monthly: bool, out_dir: Path, docs_dir: Path, overwrite: bool) -> None:
    known = parse_period(archive.name, monthly)
    if known and not overwrite and (out_dir / f"{period_stem(dataset, *known)}.parquet").exists():
        log.debug("[%s] %s already extracted", dataset, archive.name)
        return

    with tempfile.TemporaryDirectory(dir=out_dir, prefix=".tmp_", ignore_cleanup_errors=True) as tmp:
        tmp = Path(tmp)
        unpack(archive, tmp)
        contents = [p for p in tmp.rglob("*") if p.is_file()]
        csvs = [p for p in contents if p.suffix.lower() == ".csv"]
        files = pick_data_files(csvs, archive, dataset, monthly)
        if not files:
            log.warning("[%s] no data CSV inside %s", dataset, archive.name)
        save_docs([p for p in contents if p not in files.values()], tmp, docs_dir / dataset / archive.stem)
        for stem, csv in files.items():
            target = out_dir / f"{stem}.parquet"
            if target.exists() and not overwrite:
                continue
            csv_to_parquet(csv, target)
            log.info("[%s] %s -> %s", dataset, archive.name, target.name)


def run(datasets: dict, raw_dir: Path, intermediate_dir: Path, docs_dir: Path, overwrite: bool = False) -> None:
    for dataset, cfg in datasets.items():
        out_dir = intermediate_dir / dataset
        out_dir.mkdir(parents=True, exist_ok=True)
        archives = sorted(p for p in (raw_dir / dataset).glob("*") if p.suffix.lower() in (".zip", ".rar"))
        log.info("[%s] %d archives to process", dataset, len(archives))
        for archive in archives:
            try:
                extract_archive(archive, dataset, cfg["monthly"], out_dir, docs_dir, overwrite)
            except Exception:
                log.exception("[%s] failed to extract %s", dataset, archive.name)
