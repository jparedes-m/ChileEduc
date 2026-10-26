# [A4] Tracking ----
# Finds the school-grade-years that sort students into courses by their GPA of the year before, and
# the GPA threshold that separates each pair of courses.
#
# A group is a school, teaching code, grade, shift (cod_jor, from 2013) and year with 2 or more
# simple courses (no combined courses) of at least TRACKING_MIN_COURSE_SIZE students with a GPA the
# year before. Comparing only courses of the same shift keeps the courses of a group alike.
#
# 1. Sorting: eta2 is the share of the variance of prior GPA that is between courses (0: every course
#    looks the same, 1: courses do not overlap). p_value is the share of TRACKING_PERMUTATIONS random
#    reassignments of the same students to the same courses with an eta2 at least as high. A group is
#    tracked when p_value < TRACKING_ALPHA.
# 2. Persistence: courses often move up a grade together (5°A becomes 6°A), and then their prior GPAs
#    also differ because they had different teachers, not because the school sorted them. persistence
#    is the share of the students that stayed with the classmates of most of their course; groups with
#    persistence < RESHUFFLE_MAX were remixed, which is when sorting by GPA can happen.
# 3. Thresholds: in tracked groups, courses are ranked by mean prior GPA (1 = lowest) and, for each pair
#    of courses next to each other, the threshold is the prior GPA that best splits them: students at or
#    above it in the higher course, below it in the lower one. sharpness is the share of the students
#    of the pair on the right side of it (1 = a perfect cutoff).
#
# Writes, per year, to data/analysis/tracking/: groups_<year>, courses_<year> and thresholds_<year>.
# Years already done are skipped. TRACKING_SCHOOL_FRACTION < 1 runs it on a share of the schools only,
# to try it fast.

out = ANALYSIS / "tracking"
out.mkdir(parents=True, exist_ok=True)
panel_files = {period_of(f)[0]: f for f in (ANALYSIS / "panel").glob("panel_*.parquet")}
GROUP = ["year", "rbd", "cod_ense", "grado", "cod_jor"]
COURSE = [*GROUP, "let_cur"]


def eta2(frame, x):
    """Share of the variance of x that is between the courses of each group."""
    return (
        frame.with_columns(group_mean=pl.col(x).mean().over("group"), course_mean=pl.col(x).mean().over("group", "let_cur"))
        .group_by("group")
        .agg(
            between=((pl.col("course_mean") - pl.col("group_mean")) ** 2).sum(),
            total=((pl.col(x) - pl.col("group_mean")) ** 2).sum(),
        )
        .select("group", eta2=pl.when(pl.col("total") > 0).then(pl.col("between") / pl.col("total")))
    )


for year in sorted(panel_files):
    if year - 1 not in panel_files or (out / f"groups_{year}.parquet").exists():
        continue
    now = pl.read_parquet(panel_files[year]).filter(pl.col("grado") >= 2)  # 1° básico has no prior GPA
    if TRACKING_SCHOOL_FRACTION < 1:
        now = now.filter((pl.col("rbd").cast(pl.UInt64) * 2654435761 % 2**32) / 2**32 < TRACKING_SCHOOL_FRACTION)
    before = pl.read_parquet(panel_files[year - 1], columns=["mrun", "rbd", "grado", "let_cur", "prom_gral"])
    students = (
        now.filter((pl.col("cod_tip_cur").fill_null(0) == 0) & pl.col("let_cur").is_not_null())
        .select(*COURSE, "mrun")
        .join(before.select("mrun", prior=pl.col("prom_gral"), prior_rbd=pl.col("rbd"), prior_grado=pl.col("grado"),
                            prior_course=pl.col("let_cur")), on="mrun", how="left")
    )

    # Groups with 2 or more courses big enough
    with_prior = students.filter(pl.col("prior").is_not_null())
    course_sizes = with_prior.group_by(COURSE).agg(n=pl.len())
    big = course_sizes.filter(pl.col("n") >= TRACKING_MIN_COURSE_SIZE)
    groups = big.group_by(GROUP).agg(courses=pl.len()).filter(pl.col("courses") >= 2).with_row_index("group")
    d = with_prior.join(big.select(COURSE), on=COURSE).join(groups.select(*GROUP, "group"), on=GROUP, nulls_equal=True)

    # 1. Sorting, against random reassignments
    observed = eta2(d, "prior").with_columns(higher=pl.lit(0, pl.Int32))
    for k in range(TRACKING_PERMUTATIONS):
        shuffled = eta2(d.with_columns(prior=pl.col("prior").shuffle(seed=SEED + k).over("group")), "prior")
        observed = observed.join(shuffled.rename({"eta2": "perm"}), on="group").with_columns(
            higher=pl.col("higher") + (pl.col("perm") >= pl.col("eta2")).cast(pl.Int32)
        ).drop("perm")

    # 2. Persistence: students who were in the same school the year before, and the prior course of each
    stayed = d.filter((pl.col("prior_rbd") == pl.col("rbd")) & pl.col("prior_course").is_not_null())
    persistence = (
        stayed.group_by("group", "let_cur", "prior_course").agg(n=pl.len())
        .group_by("group", "let_cur").agg(pl.col("n").max().alias("largest"), pl.col("n").sum().alias("total"))
        .group_by("group").agg(persistence=pl.col("largest").sum() / pl.col("total").sum())
    )

    groups = (
        groups.join(observed.select("group", "eta2", p_value=(pl.col("higher") + 1) / (TRACKING_PERMUTATIONS + 1)), on="group")
        .join(persistence, on="group", how="left")
        .join(d.group_by("group").agg(students=pl.len(), repeaters=(pl.col("prior_grado") == pl.col("grado")).sum()), on="group")
        .with_columns(tracked=pl.col("p_value") < TRACKING_ALPHA, reshuffled=pl.col("persistence") < RESHUFFLE_MAX)
    )

    # Courses ranked by mean prior GPA (1 = lowest), in every group
    courses = (
        d.group_by("group", "let_cur").agg(n=pl.len(), mean_prior=pl.col("prior").mean(), sd_prior=pl.col("prior").std())
        .with_columns(course_rank=pl.col("mean_prior").rank("ordinal").over("group").cast(pl.Int32))
        .join(groups.select("group", *GROUP), on="group")
    )

    # 3. Thresholds between courses next to each other, in tracked groups
    ranked = d.join(courses.select("group", "let_cur", "course_rank"), on=["group", "let_cur"]).join(
        groups.filter("tracked").select("group", "courses"), on="group"
    )
    pairs = pl.concat([
        ranked.filter(pl.col("course_rank").is_in([r, r + 1]) & (pl.col("courses") > r))
        .with_columns(low_rank=pl.lit(r, pl.Int32), high=pl.col("course_rank") == r + 1)
        for r in range(1, int(groups["courses"].max() or 1))
    ]) if groups.height else ranked.head(0).with_columns(low_rank=pl.lit(0, pl.Int32), high=pl.lit(True))
    cutoffs = (
        pairs.group_by("group", "low_rank", "prior").agg(n_low=(~pl.col("high")).sum(), n_high=pl.col("high").sum())
        .sort("group", "low_rank", "prior")
        .with_columns(
            low_below=pl.col("n_low").cum_sum().over("group", "low_rank") - pl.col("n_low"),
            high_below=pl.col("n_high").cum_sum().over("group", "low_rank") - pl.col("n_high"),
            total_low=pl.col("n_low").sum().over("group", "low_rank"),
            total_high=pl.col("n_high").sum().over("group", "low_rank"),
        )
        .with_columns(sharpness=(pl.col("low_below") + pl.col("total_high") - pl.col("high_below"))
                      / (pl.col("total_low") + pl.col("total_high")))
    )
    thresholds = (
        cutoffs.sort("sharpness", descending=True).group_by("group", "low_rank", maintain_order=True).first()
        .select("group", "low_rank", threshold=pl.col("prior"), sharpness="sharpness",
                n_low="total_low", n_high="total_high")
        .join(groups.select("group", *GROUP), on="group")
        .sort("group", "low_rank")
    )

    for name, frame in {"courses": courses, "thresholds": thresholds, "groups": groups}.items():
        frame.write_parquet(out / f"{name}_{year}.parquet", compression="zstd")
    print(f"  {year}: {groups.height:,} groups with 2+ courses, {groups['tracked'].sum():,} tracked "
          f"({groups.filter('tracked')['reshuffled'].sum():,} of them reshuffled), {thresholds.height:,} thresholds")

# [End-of-file] ----
