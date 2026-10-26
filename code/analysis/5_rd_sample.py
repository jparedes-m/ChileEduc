# [A5] Regression discontinuity sample ----
# The students close to a threshold of A4: those just below it (who should be in the lower course) and just
# above it (who should be in the higher one), followed in the next years. Only the half of the students that
# was not used to find the thresholds (see estimation_half() in 0_functions.py).
#
# Takes the thresholds with sharpness >= RD_MIN_SHARPNESS and, from their two courses, the students whose
# prior GPA is within RD_BANDWIDTH of them. One row per student and threshold, rewritten on every run to
# data/analysis/rd_sample.parquet, with:
#   the design    distance (prior GPA - threshold; 0 = exactly at it, counted above), above, in_high (in the
#                 higher course), threshold, sharpness, and the group's eta2, p_value, persistence, reshuffled
#   the courses   year, group and low_rank (they identify the threshold), own course size and mean prior GPA
#   the student   gen_alu, edad, repeater (in the same grade the year before), prior (GPA the year before)
#   outcomes      that year: prom_gral, sit_fin, asis_anual; one and two years later (_t1, _t2): the same, plus
#                 grado, same_school and course_rank; for the cohort of 1° básico: finished and exit_to

GROUP = ["year", "rbd", "cod_ense", "grado", "cod_jor"]
tracking = ANALYSIS / "tracking"
panel = pl.scan_parquet(ANALYSIS / "panel" / "panel_*.parquet")
attendance = pl.scan_parquet(ANALYSIS / "attendance" / "attendance_*.parquet").select("year", "mrun", "asis_anual")
courses = pl.scan_parquet(tracking / "courses_*.parquet")
outcomes = (  # what each student got each year, and the rank of their course if it was in a group
    panel.select(*GROUP, "let_cur", "mrun", "prom_gral", "sit_fin")
    .join(courses.select(*GROUP, "let_cur", "course_rank"), on=[*GROUP, "let_cur"], how="left", nulls_equal=True)
    .join(attendance, on=["year", "mrun"], how="left")
)


def years_later(k):
    """The outcomes k years later, with the year of the assignment, to join on it."""
    return outcomes.select(
        pl.col("year") - k, "mrun",
        *[pl.col(c).alias(f"{c}_t{k}") for c in ["grado", "rbd", "course_rank", "prom_gral", "sit_fin", "asis_anual"]],
    )


prior = panel.select(pl.col("year") + 1, "mrun", prior="prom_gral", prior_grado="grado")
rd = (
    outcomes.drop("course_rank")
    .join(panel.select("year", "mrun", "gen_alu", "edad"), on=["year", "mrun"])
    .join(prior, on=["year", "mrun"])
    .join(courses.select("year", "group", *GROUP[1:], "let_cur", "course_rank", "n", "mean_prior"), on=[*GROUP, "let_cur"], nulls_equal=True)
    .join(pl.scan_parquet(tracking / "thresholds_*.parquet").filter(pl.col("sharpness") >= RD_MIN_SHARPNESS)
          .select("year", "group", "low_rank", "threshold", "sharpness"), on=["year", "group"])
    .filter(pl.col("course_rank").is_between(pl.col("low_rank"), pl.col("low_rank") + 1))
    .with_columns(distance=(pl.col("prior") - pl.col("threshold")).round(4))  # GPAs have 1 decimal: 0.3, not 0.30000001
    .filter((pl.col("distance").abs() <= RD_BANDWIDTH) & ~estimation_half())
    .with_columns(
        above=pl.col("distance") >= 0,
        in_high=pl.col("course_rank") == pl.col("low_rank") + 1,
        repeater=pl.col("prior_grado") == pl.col("grado"),
    )
    .join(pl.scan_parquet(tracking / "groups_*.parquet").select("year", "group", "eta2", "p_value", "persistence", "reshuffled"),
          on=["year", "group"])
    .join(years_later(1), on=["year", "mrun"], how="left")
    .join(years_later(2), on=["year", "mrun"], how="left")
    .join(pl.scan_parquet(ANALYSIS / "cohorts.parquet").filter(pl.col("cohort") == "cohort_basica").select("mrun", "finished", "exit_to"),
          on="mrun", how="left")
    .with_columns((pl.col(f"rbd_t{k}") == pl.col("rbd")).alias(f"same_school_t{k}") for k in (1, 2))
    .drop("rbd_t1", "rbd_t2", "prior_grado")
    .sort("year", "group", "low_rank", "distance")
    .collect()
)
rd.write_parquet(ANALYSIS / "rd_sample.parquet", compression="zstd")
print(f"  {rd.height:,} students (the half not used to find the thresholds) within {RD_BANDWIDTH} points of "
      f"{rd.select('year', 'group', 'low_rank').n_unique():,} thresholds with sharpness >= {RD_MIN_SHARPNESS}")

# [End-of-file] ----
