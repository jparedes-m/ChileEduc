# [A4] Tracking ----
# Finds the school-grade-years that sort students into courses by last year's GPA, and the GPA threshold
# between each pair of courses.
#
# A group is a school, teaching code, grade, shift (cod_jor, published from 2013) and year with 2 or more
# simple courses (not combined) of at least TRACKING_MIN_COURSE_SIZE students with a GPA the year before.
#
# 1. Sorting: eta2 is the share of the variance of prior GPA that is between courses (0: all courses alike,
#    1: they do not overlap). p_value is the share of TRACKING_PERMUTATIONS random reassignments of the same
#    students with an eta2 at least as high. tracked: p_value < TRACKING_ALPHA.
# 2. Remixing: courses often move up a grade together (5°A becomes 6°A), and their GPAs then differ because
#    they had different teachers, not because the school sorted them. persistence is the share of students
#    still with most of their classmates; reshuffled: persistence < RESHUFFLE_MAX.
# 3. Thresholds: in tracked groups, courses are ranked by mean prior GPA (1 = lowest). For each pair of
#    courses next to each other, the threshold is the lowest prior GPA that sends students to the higher one:
#    the value that puts the most students on the right side. sharpness is that share (1 = a perfect cutoff).
#    It is found with the estimation half of the students only (estimation_half() in 0_functions.py); the
#    effects are measured with the other half (5_rd_sample.py), so they do not use the same students twice.
#
# Writes, per year, to data/analysis/tracking/: groups_<year>, courses_<year>, thresholds_<year>.
# Years already done are skipped. TRACKING_SCHOOL_FRACTION < 1 runs it on a share of the schools, to try it fast.

out = ANALYSIS / "tracking"
out.mkdir(parents=True, exist_ok=True)
panel_files = {period_of(f)[0]: f for f in (ANALYSIS / "panel").glob("panel_*.parquet")}
GROUP = ["year", "rbd", "cod_ense", "grado", "cod_jor"]
COURSE = [*GROUP, "let_cur"]


def eta2(frame):
    """Share of the variance of prior GPA that is between the courses of each group."""
    return (
        frame.with_columns(group_mean=pl.col("prior").mean().over("group"), course_mean=pl.col("prior").mean().over("group", "let_cur"))
        .group_by("group")
        .agg(between=((pl.col("course_mean") - pl.col("group_mean")) ** 2).sum(), total=((pl.col("prior") - pl.col("group_mean")) ** 2).sum())
        .select("group", eta2=pl.col("between") / pl.col("total"))
    )


for year in sorted(panel_files):
    if year - 1 not in panel_files or (out / f"groups_{year}.parquet").exists():
        continue

    # Students in simple courses from 2° básico on, with their GPA, school and course the year before
    students = (
        pl.read_parquet(panel_files[year])
        .filter((pl.col("grado") >= 2) & (pl.col("cod_tip_cur").fill_null(0) == 0) & pl.col("let_cur").is_not_null())
        .filter((pl.col("rbd").cast(pl.UInt64) * 2654435761 % 2**32) / 2**32 < TRACKING_SCHOOL_FRACTION)
        .select(*COURSE, "mrun")
        .join(pl.read_parquet(panel_files[year - 1]).select("mrun", prior="prom_gral", prior_rbd="rbd", prior_grado="grado",
                                                             prior_course="let_cur"), on="mrun")
        .filter(pl.col("prior").is_not_null())
    )

    # Groups: 2 or more courses big enough. nulls_equal: before 2013 the shift is empty and must match itself
    big = students.group_by(COURSE).len().filter(pl.col("len") >= TRACKING_MIN_COURSE_SIZE).drop("len")
    groups = big.group_by(GROUP).agg(courses=pl.len()).filter(pl.col("courses") >= 2).with_row_index("group")
    d = students.join(big, on=COURSE, nulls_equal=True).join(groups.select(*GROUP, "group"), on=GROUP, nulls_equal=True)

    # 1. Sorting, against random reassignments
    observed = eta2(d).with_columns(higher=pl.lit(0))
    for k in range(TRACKING_PERMUTATIONS):
        shuffled = eta2(d.with_columns(prior=pl.col("prior").shuffle(seed=SEED + k).over("group"))).rename({"eta2": "perm"})
        observed = observed.join(shuffled, on="group").with_columns(higher=pl.col("higher") + (pl.col("perm") >= pl.col("eta2"))).drop("perm")

    # 2. Remixing: among students in the same school the year before, how many are with most of their old course
    persistence = (
        d.filter(pl.col("prior_rbd") == pl.col("rbd"))
        .group_by("group", "let_cur", "prior_course").len()
        .group_by("group", "let_cur").agg(largest=pl.col("len").max(), total=pl.col("len").sum())
        .group_by("group").agg(persistence=pl.col("largest").sum() / pl.col("total").sum())
    )
    groups = (
        groups.join(observed.select("group", "eta2", p_value=(pl.col("higher") + 1) / (TRACKING_PERMUTATIONS + 1)), on="group")
        .join(persistence, on="group", how="left")
        .join(d.group_by("group").agg(students=pl.len(), repeaters=(pl.col("prior_grado") == pl.col("grado")).sum()), on="group")
        .with_columns(tracked=pl.col("p_value") < TRACKING_ALPHA, reshuffled=pl.col("persistence") < RESHUFFLE_MAX)
    )

    # Courses ranked by mean prior GPA (1 = lowest)
    courses = (
        d.group_by("group", "let_cur").agg(n=pl.len(), mean_prior=pl.col("prior").mean())
        .with_columns(course_rank=pl.col("mean_prior").rank("ordinal").over("group").cast(pl.Int32))
        .join(groups.select("group", *GROUP), on="group")
    )

    # 3. Thresholds: each student of a tracked group in the pair(s) of courses they belong to
    ranked = d.join(courses.select("group", "let_cur", "course_rank"), on=["group", "let_cur"]).join(
        groups.filter("tracked").select("group", "courses"), on="group")
    pairs = pl.concat(
        [ranked.filter(pl.col("course_rank").is_in([low, low + 1])).with_columns(low_rank=pl.lit(low, pl.Int32), high=pl.col("course_rank") == low + 1)
         for low in range(1, int(groups["courses"].max() or 2))],
        how="vertical",
    ).with_columns(estimation=estimation_half())
    # Every prior GPA of the pair is a candidate; only the estimation half counts. For a candidate c, students
    # on the right side are those of the lower course below c and those of the higher course at or above it.
    cutoffs = (
        pairs.group_by("group", "low_rank", "prior")
        .agg(low=(~pl.col("high") & pl.col("estimation")).sum(), high=(pl.col("high") & pl.col("estimation")).sum())
        .sort("group", "low_rank", "prior")
        .with_columns(
            low_below=pl.col("low").cum_sum().over("group", "low_rank") - pl.col("low"),
            high_below=pl.col("high").cum_sum().over("group", "low_rank") - pl.col("high"),
            n_low=pl.col("low").sum().over("group", "low_rank"),
            n_high=pl.col("high").sum().over("group", "low_rank"),
        )
        .with_columns(sharpness=(pl.col("low_below") + pl.col("n_high") - pl.col("high_below")) / (pl.col("n_low") + pl.col("n_high")))
    )
    thresholds = (
        cutoffs.filter((pl.col("n_low") > 0) & (pl.col("n_high") > 0))
        .sort("group", "low_rank", "sharpness", "prior", descending=[False, False, True, False])
        .group_by("group", "low_rank", maintain_order=True).first()  # the best split, and the lowest GPA if tied
        .select("group", "low_rank", threshold="prior", sharpness="sharpness", n_low="n_low", n_high="n_high")
        .join(groups.select("group", *GROUP), on="group")
    )

    for name, frame in {"courses": courses, "thresholds": thresholds, "groups": groups}.items():
        frame.write_parquet(out / f"{name}_{year}.parquet", compression="zstd")
    print(f"  {year}: {groups.height:,} groups with 2+ courses, {groups['tracked'].sum():,} tracked "
          f"({groups.filter('tracked')['reshuffled'].sum():,} of them reshuffled), {thresholds.height:,} thresholds")

# [End-of-file] ----
