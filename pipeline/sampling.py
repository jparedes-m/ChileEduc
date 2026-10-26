"""Step 4: draw a reproducible sample of students for quick exploration.

The sample is drawn by student (mrun), not by row, using the same rule in every
dataset: a sampled student keeps all their years and months in kinder,
assistance and performance, so cohorts can be followed inside the sample.
"""

import logging
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

_KNUTH = 2654435761  # multiplicative hash constant, spreads consecutive ids over [0, 2^32)


def in_sample(fraction: float, seed: int) -> pl.Expr:
    """Deterministic per-student draw; does not depend on the Polars version or the machine."""
    h = ((pl.col("mrun").cast(pl.UInt64) + seed) * _KNUTH) % (1 << 32)
    return h < int(fraction * (1 << 32))


def run(datasets: dict, clean_dir: Path, sample_dir: Path, fraction: float, seed: int, overwrite: bool = False) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    for dataset in datasets:
        files = sorted((clean_dir / dataset).glob("*.parquet"))
        target = sample_dir / f"{dataset}_sample.parquet"
        if not files:
            log.warning("[%s] no clean files, sample skipped", dataset)
            continue
        if target.exists() and not overwrite:
            log.info("[%s] %s already exists", dataset, target.name)
            continue
        partial = target.with_name(target.name + ".part")
        pl.scan_parquet(files).filter(in_sample(fraction, seed)).sink_parquet(partial, compression="zstd")
        partial.replace(target)
        rows = pl.scan_parquet(target).select(pl.len()).collect().item()
        log.info("[%s] %s: %d rows", dataset, target.name, rows)
