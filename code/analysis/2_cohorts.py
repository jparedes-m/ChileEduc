# [A2] Cohorts ----
# Follows the cohorts in COHORTS through the panel of A1. A child enters a cohort the first year they
# are in its starting grade, as long as they were never in that grade or a higher one before (so a
# repeater is not counted as entering again). Their rows are followed until the cohort's last grade.
# Writes, on every run:
#   data/analysis/cohorts.parquet       one row per child and cohort: entry, last year and grade seen,
#                                       repetitions, withdrawals and how they ended
#   data/analysis/cohort_panel.parquet  the yearly rows of every cohort member, with their cohort
#   data/sample/cohort_panel_sample.parquet, the same for the students of the sample
#
# How each child ends:
#   finished  promoted in the cohort's last grade
#   on time   ... in the year expected without repeating (entry year + last grade - first grade)
#   left      not finished and not seen in the last year of the data: dropped out, moved to special or
#             adult education, or left the country
#   censored  not finished and still in school in the last year of the data

panel = pl.scan_parquet(ANALYSIS / "panel" / "panel_*.parquet")
withdrawals = pl.scan_parquet(ANALYSIS / "withdrawals" / "withdrawals_*.parquet")
last_year = panel.select(pl.col("year").max()).collect().item()

cohorts, cohort_rows = [], []
for name, cohort in COHORTS.items():
    first, last = cohort["start_grade"], cohort["end_grade"]
    entry = panel.filter(pl.col("grado") == first).group_by("mrun").agg(pl.col("year").min().alias("entry_year"))
    came_back = (  # already in this grade or higher before: a repeater, not a new entrant
        panel.join(entry, on="mrun")
        .filter((pl.col("year") < pl.col("entry_year")) & (pl.col("grado") >= first))
        .select("mrun").unique()
    )
    members = entry.join(came_back, on="mrun", how="anti").filter(pl.col("entry_year").is_in(cohort["entry_years"]))

    rows = (
        panel.join(members, on="mrun")
        .filter((pl.col("year") >= pl.col("entry_year")) & pl.col("grado").is_between(first, last))
        .with_columns(cohort=pl.lit(name))
    )
    left_school = (
        withdrawals.join(members, on="mrun")
        .filter((pl.col("year") >= pl.col("entry_year")) & pl.col("grado").is_between(first, last))
        .group_by("mrun").agg(pl.len().cast(pl.Int32).alias("withdrawals"))
    )
    summary = (
        rows.group_by("cohort", "mrun", "entry_year")
        .agg(
            pl.len().cast(pl.Int32).alias("years_seen"),
            pl.col("year").max().alias("last_year"),
            pl.col("grado").sort_by("year").last().alias("last_grade"),
            (pl.col("sit_fin") == "R").sum().cast(pl.Int32).alias("repetitions"),
            pl.col("year").filter((pl.col("grado") == last) & (pl.col("sit_fin") == "P")).min().alias("finish_year"),
        )
        .join(left_school, on="mrun", how="left")
        .with_columns(
            pl.col("withdrawals").fill_null(0),
            finished=pl.col("finish_year").is_not_null(),
            on_time=pl.col("finish_year") == pl.col("entry_year") + (last - first),
        )
        .with_columns(
            pl.col("on_time").fill_null(False),
            left=~pl.col("finished") & (pl.col("last_year") < last_year),
            censored=~pl.col("finished") & (pl.col("last_year") == last_year),
        )
    )
    cohorts.append(summary)
    cohort_rows.append(rows)

cohorts = pl.concat(cohorts).sort("cohort", "entry_year", "mrun").collect()
cohorts.write_parquet(ANALYSIS / "cohorts.parquet", compression="zstd")
pl.concat(cohort_rows).sink_parquet(ANALYSIS / "cohort_panel.parquet", compression="zstd")
SAMPLE.mkdir(parents=True, exist_ok=True)
pl.scan_parquet(ANALYSIS / "cohort_panel.parquet").filter(in_sample(SAMPLE_FRACTION["performance"])).sink_parquet(
    SAMPLE / "cohort_panel_sample.parquet", compression="zstd"
)

pl.Config.set_tbl_rows(40)
print(
    cohorts.group_by("cohort", "entry_year")
    .agg(
        pl.len().alias("children"),
        (100 * pl.col("finished").mean()).round(1).alias("% finished"),
        (100 * pl.col("on_time").mean()).round(1).alias("% on time"),
        (100 * pl.col("left").mean()).round(1).alias("% left"),
        (100 * pl.col("censored").mean()).round(1).alias("% censored"),
    )
    .sort("cohort", "entry_year")
)

# [End-of-file] ----
