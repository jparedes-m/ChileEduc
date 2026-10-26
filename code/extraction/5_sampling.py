# [5] Sampling ----
# Saves a sample of students to data/sample/<dataset>_sample.parquet, for quick exploration.
#
# The sample picks students, not rows: a sampled student keeps all their rows (every year, and
# every month in assistance), so cohorts can be followed. Each student gets a fixed number from
# their mrun, and the sample is the students whose number is below SAMPLE_FRACTION. The number is
# the same in every dataset, so the 0.5% of students in the assistance sample are also in the 2%
# samples of kinder and performance. Samples already made are skipped.

SAMPLE.mkdir(parents=True, exist_ok=True)
for dataset in DATASETS:
    files = sorted((CLEAN / dataset).glob("*.parquet"))
    target = SAMPLE / f"{dataset}_sample.parquet"
    if not files or target.exists():
        continue

    partial = target.with_name(target.name + ".part")
    pl.scan_parquet(files).filter(in_sample(SAMPLE_FRACTION[dataset])).sink_parquet(partial, compression="zstd")
    partial.replace(target)
    rows = pl.scan_parquet(target).select(pl.len()).collect().item()
    print(f"{dataset}: {rows:,} rows in the sample")

# [End-of-file] ----
