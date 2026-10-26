# [A5] Regression discontinuity sample ----
# The students close to a threshold of A4, to compare those just below it (sent to the lower course)
# with those just above it (sent to the higher course), and to follow both in the next years.
#
# Takes every threshold with sharpness >= RD_MIN_SHARPNESS and, from its two courses, the students whose
# prior GPA is within RD_BANDWIDTH of it. One row per student and threshold, rewritten on every run to
# data/analysis/rd_sample.parquet, with:
#   the design    distance (prior GPA - threshold), above (distance >= 0), in_high (is in the higher course),
#                 threshold, sharpness, and the group's eta2, p_value, persistence and reshuffled
#   the courses   group (school-grade-shift-year id) and low_rank (which pair), own and other course size and
#                 mean prior GPA, so courses can be compared with the ones they are most alike
#   the student   gen_alu, edad, rezago, repeater
#   outcomes      in the year of the assignment: prom_gral, asistencia, sit_fin, asis_anual; one and two years
#                 later (_t1, _t2): the same, plus grado, same_school and course_rank (rank of their course
#                 then, if it was in a group with 2+ courses); and for the cohort of 1° básico 2002-2012:
#                 finished, on_time and exit_to

GROUP = ["year", "rbd", "cod_ense", "grado", "cod_jor"]
tracking = ANALYSIS / "tracking"
panel_files = {period_of(f)[0]: f for f in (ANALYSIS / "panel").glob("panel_*.parquet")}
attendance = pl.scan_parquet(ANALYSIS / "attendance" / "attendance_*.parquet")
cohort_basica = (
    pl.scan_parquet(ANALYSIS / "cohorts.parquet").filter(pl.col("cohort") == "cohort_basica")
    .select("mrun", "finished", "on_time", "exit_to")
)


def later(year, suffix):
    """Where each student is `suffix` years later, and how it went."""
    if year not in panel_files:
        return None
    rows = pl.scan_parquet(panel_files[year]).select("mrun", "rbd", "cod_ense", "grado", "cod_jor", "let_cur",
                                                     "prom_gral", "asistencia", "sit_fin")
    courses_file = tracking / f"courses_{year}.parquet"
    if courses_file.exists():
        rows = rows.join(
            pl.scan_parquet(courses_file).select("rbd", "cod_ense", "grado", "cod_jor", "let_cur", "course_rank"),
            on=["rbd", "cod_ense", "grado", "cod_jor", "let_cur"], how="left", nulls_equal=True,
        )
    else:
        rows = rows.with_columns(course_rank=pl.lit(None, pl.Int32))
    rows = rows.join(attendance.filter(pl.col("year") == year).select("mrun", "asis_anual"), on="mrun", how="left")
    return rows.select(
        "mrun",
        *[pl.col(c).alias(f"{c}_{suffix}") for c in ["grado", "rbd", "course_rank", "prom_gral", "asistencia", "sit_fin", "asis_anual"]],
    )


samples = []
for year in sorted(panel_files):
    thresholds_file = tracking / f"thresholds_{year}.parquet"
    if not thresholds_file.exists():
        continue
    thresholds = pl.scan_parquet(thresholds_file).filter(pl.col("sharpness") >= RD_MIN_SHARPNESS)
    groups = pl.scan_parquet(tracking / f"groups_{year}.parquet").select("group", "eta2", "p_value", "persistence", "reshuffled")
    courses = pl.scan_parquet(tracking / f"courses_{year}.parquet").select("group", *GROUP, "let_cur", "course_rank", "n", "mean_prior")
    prior = pl.scan_parquet(panel_files[year - 1]).select("mrun", prior=pl.col("prom_gral"), prior_grado=pl.col("grado"))
    students = (
        pl.scan_parquet(panel_files[year])
        .select(*GROUP, "let_cur", "mrun", "gen_alu", "edad", "rezago", "prom_gral", "asistencia", "sit_fin")
        .join(prior, on="mrun")
        .join(courses, on=[*GROUP, "let_cur"], nulls_equal=True)
    )
    other = courses.select("group", other_rank="course_rank", other_n="n", other_mean_prior="mean_prior")
    sample = (
        students.join(thresholds.select("group", "low_rank", "threshold", "sharpness"), on="group")
        .filter((pl.col("course_rank") == pl.col("low_rank")) | (pl.col("course_rank") == pl.col("low_rank") + 1))
        .with_columns(distance=pl.col("prior") - pl.col("threshold"))
        .filter(pl.col("distance").abs() <= RD_BANDWIDTH)
        .with_columns(
            above=pl.col("distance") >= 0,
            in_high=pl.col("course_rank") == pl.col("low_rank") + 1,
            repeater=pl.col("prior_grado") == pl.col("grado"),
            other_rank=pl.when(pl.col("course_rank") == pl.col("low_rank")).then(pl.col("low_rank") + 1).otherwise(pl.col("low_rank")),
        )
        .join(other, on=["group", "other_rank"])
        .join(groups, on="group")
        .join(attendance.filter(pl.col("year") == year).select("mrun", "asis_anual"), on="mrun", how="left")
    )
    for offset, suffix in [(1, "t1"), (2, "t2")]:
        if (following := later(year + offset, suffix)) is not None:
            sample = sample.join(following, on="mrun", how="left").with_columns(
                (pl.col(f"rbd_{suffix}") == pl.col("rbd")).alias(f"same_school_{suffix}")
            ).drop(f"rbd_{suffix}")
    samples.append(sample.join(cohort_basica, on="mrun", how="left").collect())

columns = [
    "year", "mrun", "group", "low_rank", *GROUP[1:], "let_cur", "distance", "above", "in_high", "threshold",
    "sharpness", "eta2", "p_value", "persistence", "reshuffled", "n", "mean_prior", "other_n", "other_mean_prior",
    "gen_alu", "edad", "rezago", "repeater", "prior", "prom_gral", "asistencia", "sit_fin", "asis_anual",
]
rd = pl.concat(samples, how="diagonal_relaxed") if samples else pl.DataFrame()
if rd.height:
    rd = rd.select(*columns, pl.exclude(columns)).sort("year", "group", "low_rank", "distance")
rd.write_parquet(ANALYSIS / "rd_sample.parquet", compression="zstd")
print(f"  {rd.height:,} students within {RD_BANDWIDTH} points of {rd.select('year', 'group', 'low_rank').n_unique() if rd.height else 0:,} "
      f"thresholds with sharpness >= {RD_MIN_SHARPNESS}")
if rd.height:  # a first look: just below and just above the thresholds
    print(rd.group_by("above").agg(
        pl.len().alias("students"),
        pl.col("in_high").mean().round(3).alias("share in higher course"),
        pl.col("prom_gral").mean().round(2).alias("GPA that year"),
        *[pl.col(c).mean().round(2).alias("GPA a year later") for c in ["prom_gral_t1"] if c in rd.columns],
    ).sort("above"))

# [End-of-file] ----
