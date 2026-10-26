# [A3] Cohorts ----
# Follows the cohorts in COHORTS through the panel of A1. A child enters a cohort the first year they
# are in its starting grade, as long as they were never in that grade or a higher one before (so a
# repeater is not counted as entering again). Their rows are followed until the cohort's last grade.
# A child can be in both cohorts (in kinder in 2011, in 1° básico in 2012).
# Writes, on every run:
#   data/analysis/cohort_panel.parquet  THE FINAL DATA: one row per child and year, for every child in a
#                                       cohort, with one column per cohort (cohort_kinder, cohort_basica)
#                                       holding the year the child entered it, or empty if that year is not
#                                       part of that cohort. Has the panel of A1 (grade, school, course, GPA,
#                                       final status) and the yearly attendance of A2
#   data/analysis/cohorts.parquet       one row per child and cohort: entry, last year and grade seen,
#                                       repetitions, withdrawals and how they ended
#   data/sample/cohort_panel_sample.parquet, the final data for the students of the sample
#
# How each child ends:
#   finished  promoted in the cohort's last grade
#   on time   ... in the year expected without repeating (entry year + last grade - first grade)
#   left      not finished and not seen in the last year of the data. exit_to says where they went, from
#             their later records in the clean performance data:
#               adult_education        moved to adult education (most of them)
#               special_education      moved to special education
#               regular_outside_panel  still in regular school, but outside the panel: too old for their
#                                      grade (rezago over REZAGO_MAX) or only a withdrawal record
#               not_seen_again         no later record: dropped out, left the country or died
#   censored  not finished and still in school in the last year of the data

REGULAR_ENSE = [110, 310, 410, 510, 610, 710, 810, 910]  # básica and media for children and youth
panel = pl.scan_parquet(ANALYSIS / "panel" / "panel_*.parquet")
performance = pl.scan_parquet(CLEAN / "performance" / "*.parquet").select("year", "mrun", "cod_ense")
withdrawals = pl.scan_parquet(ANALYSIS / "withdrawals" / "withdrawals_*.parquet")
attendance = pl.scan_parquet(ANALYSIS / "attendance" / "attendance_*.parquet")
last_year = panel.select(pl.col("year").max()).collect().item()


def in_cohort(name):
    """The rows of a child that belong to a cohort: from the entry year on, within the cohort's grades."""
    first, last = COHORTS[name]["start_grade"], COHORTS[name]["end_grade"]
    return pl.col(name).is_not_null() & (pl.col("year") >= pl.col(name)) & pl.col("grado").is_between(first, last)


# Entry year of every child in each cohort
rows = panel
for name, cohort in COHORTS.items():
    first = cohort["start_grade"]
    entry = panel.filter(pl.col("grado") == first).group_by("mrun").agg(pl.col("year").min().alias(name))
    came_back = (  # already in this grade or higher before: a repeater, not a new entrant
        panel.join(entry, on="mrun")
        .filter((pl.col("year") < pl.col(name)) & (pl.col("grado") >= first))
        .select("mrun").unique()
    )
    members = entry.join(came_back, on="mrun", how="anti").filter(pl.col(name).is_in(cohort["entry_years"]))
    rows = rows.join(members, on="mrun", how="left")

# One row per child and year: keep the years that belong to at least one of their cohorts
rows = (
    rows.with_columns(pl.when(in_cohort(name)).then(pl.col(name)).alias(name) for name in COHORTS)
    .filter(pl.any_horizontal(pl.col(name).is_not_null() for name in COHORTS))
    .join(attendance, on=["year", "mrun"], how="left")
)
first_columns = ["year", "mrun", *COHORTS, "grado"]
rows.select(*first_columns, pl.exclude(first_columns)).sort("mrun", "year").sink_parquet(
    ANALYSIS / "cohort_panel.parquet", compression="zstd"
)

# How each child ended, cohort by cohort
cohorts = []
for name, cohort in COHORTS.items():
    first, last = cohort["start_grade"], cohort["end_grade"]
    rows_in = pl.scan_parquet(ANALYSIS / "cohort_panel.parquet").filter(pl.col(name).is_not_null())
    entries = rows_in.select("mrun", pl.col(name).alias("entry_year")).unique()
    left_school = (
        withdrawals.join(entries, on="mrun")
        .filter((pl.col("year") >= pl.col("entry_year")) & pl.col("grado").is_between(first, last))
        .group_by("mrun").agg(pl.len().cast(pl.Int32).alias("withdrawals"))
    )
    summary = (
        rows_in.group_by("mrun", pl.col(name).alias("entry_year"))
        .agg(
            pl.len().cast(pl.Int32).alias("years_seen"),
            pl.col("year").max().alias("last_year"),
            pl.col("grado").sort_by("year").last().alias("last_grade"),
            (pl.col("sit_fin") == "R").sum().cast(pl.Int32).alias("repetitions"),
            pl.col("year").filter((pl.col("grado") == last) & (pl.col("sit_fin") == "P")).min().alias("finish_year"),
        )
        .join(left_school, on="mrun", how="left")
        .with_columns(
            pl.lit(name).alias("cohort"),
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
    later = (  # where the children who left went: their records after the last year in the cohort
        performance.join(summary.filter("left").select("mrun", "last_year"), on="mrun")
        .filter(pl.col("year") > pl.col("last_year"))
        .group_by("mrun")
        .agg(
            regular=pl.col("cod_ense").is_in(REGULAR_ENSE).any(),
            special=pl.col("cod_ense").is_between(200, 299).any(),
            adult=(~pl.col("cod_ense").is_in(REGULAR_ENSE) & ~pl.col("cod_ense").is_between(200, 299)).any(),
        )
    )
    summary = summary.join(later, on="mrun", how="left").with_columns(
        exit_to=pl.when(~pl.col("left")).then(None)
        .when(pl.col("adult")).then(pl.lit("adult_education"))
        .when(pl.col("special")).then(pl.lit("special_education"))
        .when(pl.col("regular")).then(pl.lit("regular_outside_panel"))
        .otherwise(pl.lit("not_seen_again"))
    ).drop("regular", "special", "adult")
    cohorts.append(summary)

cohorts = pl.concat(cohorts).select("cohort", "mrun", "entry_year", pl.exclude("cohort", "mrun", "entry_year"))
cohorts = cohorts.sort("cohort", "entry_year", "mrun").collect()
cohorts.write_parquet(ANALYSIS / "cohorts.parquet", compression="zstd")
SAMPLE.mkdir(parents=True, exist_ok=True)
pl.scan_parquet(ANALYSIS / "cohort_panel.parquet").filter(in_sample(SAMPLE_FRACTION)).sink_parquet(
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
        (100 * (pl.col("exit_to") == "adult_education").mean()).round(1).alias("% to adults"),
        (100 * (pl.col("exit_to") == "not_seen_again").mean()).round(1).alias("% not seen"),
        (100 * pl.col("censored").mean()).round(1).alias("% censored"),
    )
    .sort("cohort", "entry_year")
)

# [End-of-file] ----
