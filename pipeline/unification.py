"""Step 3: give every year the same columns, names, order and types, and apply sample filters.

Reads intermediate/<dataset>/*.parquet and writes clean/<dataset>/*.parquet, so
the inputs are never overwritten and the step can be rerun safely. All files of
a dataset share one schema, so they can be read together with
pl.scan_parquet("clean/<dataset>/*.parquet").

The mapping file has one sheet per dataset: a "desired_names" column plus one
column per period (2015, or 2015_03 for assistance) holding that year's original
column name. An optional "description" column feeds the data dictionary.

Published columns the mapping does not mention are kept too, under their own
name in lowercase, when they appear in at least min_year_share of the years.
"""

import logging
from collections import Counter
from pathlib import Path

import polars as pl

from pipeline.common import parse_stem
from pipeline.schemas import SCHEMAS

log = logging.getLogger(__name__)

# Regular (children and youth) education codes, Anexo II of the MINEDUC record layout:
# 10 parvularia, 110 basica, 310 media HC, 410-910 media TP and artistica.
# Everything else (special and adult education) is dropped.
REGULAR_ENSE = [10, 110, 310, 410, 510, 610, 710, 810, 910]


def sample_filter(dataset: str, year: int) -> pl.Expr:
    if dataset == "kinder":
        if year < 2012:
            return pl.col("cod_ense1_m") == 10  # drops special education
        # Regular early education, full-day or morning shift only.
        return (pl.col("cod_ense2_m") == 1) & pl.col("cod_jor_j").is_in([1, 2])
    return pl.col("cod_ense").is_in(REGULAR_ENSE)


def read_mapping(mapping_file: Path, dataset: str) -> pl.DataFrame:
    df = pl.read_excel(mapping_file, sheet_name=dataset, infer_schema_length=0)
    # Excel stores year headers as numbers; make them plain text such as "2015".
    return df.rename({c: str(c).strip().removesuffix(".0") for c in df.columns})


NOT_PERIODS = ("desired_names", "description")


def raw_coverage(src_dir: Path) -> dict[int, set[str]]:
    """Published column names (uppercase) present each year, read from the file headers only."""
    coverage: dict[int, set[str]] = {}
    for f in src_dir.glob("*.parquet"):
        year, _ = parse_stem(f.stem)
        coverage.setdefault(year, set()).update(c.upper() for c in pl.read_parquet_schema(f))
    return coverage


def mapped_names(mapping: pl.DataFrame) -> dict[str, str]:
    """Published name (uppercase) -> desired name, over every period of the mapping."""
    names = {}
    for period in (c for c in mapping.columns if c not in NOT_PERIODS):
        for desired, original in mapping.select("desired_names", period).drop_nulls().iter_rows():
            names[original.strip().upper()] = desired.strip().lower()
    return names


def extra_columns(mapping: pl.DataFrame, coverage: dict[int, set[str]], min_year_share: float) -> list[str]:
    taken = set(mapped_names(mapping)) | {d.strip().upper() for d in mapping["desired_names"].drop_nulls()}
    years = Counter(name for names in coverage.values() for name in names)
    return sorted(n for n, k in years.items() if k >= min_year_share * len(coverage) and n not in taken)


def period_key(year: int, month: int | None) -> str:
    return str(year) if month is None else f"{year}_{month:02d}"


def rename_plan(mapping: pl.DataFrame, key: str, columns: list[str], extras: list[str]) -> list[pl.Expr]:
    """Select the desired columns in order, then the extras; missing ones are filled with nulls."""
    if key not in mapping.columns:
        raise KeyError(f"mapping has no column for period {key}")
    by_lower = {c.lower(): c for c in columns}
    exprs = []
    for desired, original in mapping.select("desired_names", key).iter_rows():
        if desired is None:
            continue
        source = None
        if original is not None:
            original = original.strip()
            source = original if original in columns else by_lower.get(original.lower())
        name = desired.strip().lower()
        exprs.append(pl.col(source).alias(name) if source else pl.lit(None, pl.String).alias(name))
    by_upper = {c.upper(): c for c in columns}
    for extra in extras:
        source = by_upper.get(extra)
        exprs.append(pl.col(source).alias(extra.lower()) if source else pl.lit(None, pl.String).alias(extra.lower()))
    return exprs


def cast(name: str, dtype: pl.DataType) -> pl.Expr:
    col = pl.col(name).str.strip_chars()
    if dtype == pl.String:
        return col
    # Decimal commas (5,8) are common in MINEDUC files. Going through Float64 also accepts "10.0".
    as_float = col.str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)
    return as_float if dtype == pl.Float64 else as_float.cast(dtype, strict=False)


def lossy_casts(raw: pl.DataFrame, typed: pl.DataFrame, schema: dict) -> dict[str, int]:
    """Count values that were present as text but became null when typed."""
    counts = {}
    for c in typed.columns:
        if schema.get(c, pl.String) == pl.String:
            continue
        present = raw[c].str.strip_chars().fill_null("") != ""
        if n := (present & typed[c].is_null()).sum():
            counts[c] = n
    return counts


def unify_file(src: Path, target: Path, dataset: str, mapping: pl.DataFrame, extras: list[str]) -> None:
    year, month = parse_stem(src.stem)
    schema = SCHEMAS[dataset]

    raw = pl.read_parquet(src)
    renamed = raw.select(rename_plan(mapping, period_key(year, month), raw.columns, extras))
    if "year" not in renamed.columns:
        renamed = renamed.with_columns(pl.lit(str(year)).alias("year"))
    if month is not None and "month" not in renamed.columns:
        renamed = renamed.with_columns(pl.lit(str(month)).alias("month"))

    typed = renamed.select(cast(c, schema.get(c, pl.String)) for c in renamed.columns)
    for column, n in lossy_casts(renamed, typed, schema).items():
        log.warning("[%s] %s: %d values of %s could not be read as numbers", dataset, src.stem, n, column)

    clean = typed.filter(sample_filter(dataset, year))
    partial = target.with_name(target.name + ".part")
    clean.write_parquet(partial, compression="zstd")
    partial.replace(target)
    log.info("[%s] %s: %d rows kept of %d", dataset, target.name, clean.height, raw.height)


def run(
    datasets: dict, intermediate_dir: Path, clean_dir: Path, mapping_file: Path, min_year_share: float, overwrite: bool = False
) -> None:
    for dataset in datasets:
        out_dir = clean_dir / dataset
        out_dir.mkdir(parents=True, exist_ok=True)
        mapping = read_mapping(mapping_file, dataset)
        extras = extra_columns(mapping, raw_coverage(intermediate_dir / dataset), min_year_share)
        if extras:
            log.info("[%s] published columns kept outside the mapping: %s", dataset, ", ".join(extras))
        names = [c.lower() for c in mapping["desired_names"].drop_nulls()] + [e.lower() for e in extras]
        if untyped := [c for c in names if c not in SCHEMAS[dataset]]:
            log.info("[%s] kept as text (not in pipeline/schemas.py): %s", dataset, ", ".join(untyped))

        for src in sorted((intermediate_dir / dataset).glob("*.parquet")):
            target = out_dir / src.name
            if target.exists() and not overwrite:
                continue
            try:
                unify_file(src, target, dataset, mapping, extras)
            except Exception:
                log.exception("[%s] failed to unify %s", dataset, src.name)
