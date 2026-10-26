# [A6] Regression discontinuity estimates ----
# The effect of being sent to the higher course, for the students of A5 just below and just above a threshold.
#
# For each sample (all thresholds, and only those of groups that remixed their courses), bandwidth in
# RD_ESTIMATE_BANDWIDTHS and outcome, a local linear regression on each side of the threshold, with a fixed
# effect for each threshold (so students are only compared with those of the same school, grade, shift,
# year and pair of courses) and standard errors clustered by school:
#   first stage   in_high ~ above + distance + above x distance | threshold
#   reduced form  outcome ~ above + distance + above x distance | threshold
#   iv            outcome ~ in_high + distance + above x distance | threshold, with in_high instrumented by above
# The iv estimate is the effect of being in the higher course for the students who are there because they were
# above the threshold (fuzzy regression discontinuity). Balance rows are characteristics fixed before the
# assignment: they should not jump at the threshold.
# Writes, on every run, results/rd_estimates.csv and results/rd_plots.png (in git: they are small summaries).

OUTCOMES = {  # column: (label, kind)
    "female": ("female", "balance"),
    "edad": ("age", "balance"),
    "repeater": ("repeater (in the same grade the year before)", "balance"),
    "prom_gral": ("GPA, year of the assignment", "outcome"),
    "prom_gral_t1": ("GPA, 1 year later", "outcome"),
    "prom_gral_t2": ("GPA, 2 years later", "outcome"),
    "repeated": ("failed the grade, year of the assignment", "outcome"),
    "repeated_t1": ("failed the grade, 1 year later", "outcome"),
    "asis_anual": ("attendance (0-1), year of the assignment", "outcome"),
    "asis_anual_t1": ("attendance (0-1), 1 year later", "outcome"),
    "same_school_t1": ("same school 1 year later", "outcome"),
    "finished": ("finished 4° medio (cohort 1° básico)", "outcome"),
    "adult_education": ("moved to adult education (cohort 1° básico)", "outcome"),
}

def estimate(data, formula, coefficient):
    """Estimate, standard error, p-value and observations of one coefficient; None if it cannot be estimated."""
    try:
        fit = pf.feols(formula, data=data, vcov={"CRV1": "rbd"})
        row = fit.tidy().loc[coefficient]
        return {"estimate": row["Estimate"], "se": row["Std. Error"], "p_value": row["Pr(>|t|)"], "n": fit._N}
    except Exception as error:  # e.g. no variation in the outcome within the bandwidth
        print(f"    could not estimate {formula}: {type(error).__name__}")
        return None


RESULTS.mkdir(parents=True, exist_ok=True)
rd = pl.read_parquet(ANALYSIS / "rd_sample.parquet")
if rd.is_empty():
    print("  no students in the regression discontinuity sample: nothing to estimate")
else:
    rd = rd.with_columns(
        pair=pl.format("{}-{}-{}", "year", "group", "low_rank"),
        above=pl.col("above").cast(pl.Int8),
        in_high=pl.col("in_high").cast(pl.Int8),
        above_x_distance=pl.col("above").cast(pl.Float64) * pl.col("distance"),
        female=(pl.col("gen_alu") == 2).cast(pl.Int8),
        repeater=pl.col("repeater").cast(pl.Int8),
        repeated=(pl.col("sit_fin") == "R").cast(pl.Int8),
        repeated_t1=(pl.col("sit_fin_t1") == "R").cast(pl.Int8),
        same_school_t1=pl.col("same_school_t1").cast(pl.Int8),
        finished=pl.col("finished").cast(pl.Int8),
        adult_education=pl.when(pl.col("finished").is_not_null()).then((pl.col("exit_to") == "adult_education").fill_null(False)).cast(pl.Int8),
    )
    outcomes = {c: v for c, v in OUTCOMES.items() if c in rd.columns}  # e.g. no _t2 when the data ends a year later
    samples = {"all thresholds": rd, "remixed courses only": rd.filter(pl.col("reshuffled").fill_null(False))}


    rows = []
    for sample_name, sample in samples.items():
        for bandwidth in RD_ESTIMATE_BANDWIDTHS:
            data = sample.filter(pl.col("distance").abs() <= bandwidth + 1e-9)
            columns = ["pair", "rbd", "above", "in_high", "distance", "above_x_distance", *outcomes]
            df = data.select(columns).to_pandas()
            print(f"  {sample_name}, bandwidth {bandwidth}: {len(df):,} students, {df['pair'].nunique():,} thresholds")
            result = estimate(df, "in_high ~ above + distance + above_x_distance | pair", "above")
            if result:
                rows.append({"sample": sample_name, "bandwidth": bandwidth, "outcome": "in_high", "label": "in the higher course",
                             "kind": "first stage", "method": "first stage", "mean_below": df.loc[df.above == 0, "in_high"].mean(), **result})
            for outcome, (label, kind) in outcomes.items():
                if df[outcome].notna().sum() == 0:
                    continue
                mean_below = df.loc[df.above == 0, outcome].mean()
                for method, formula, coefficient in [
                    ("reduced form", f"{outcome} ~ above + distance + above_x_distance | pair", "above"),
                    ("iv", f"{outcome} ~ distance + above_x_distance | pair | in_high ~ above", "in_high"),
                ]:
                    if result := estimate(df, formula, coefficient):
                        rows.append({"sample": sample_name, "bandwidth": bandwidth, "outcome": outcome, "label": label,
                                     "kind": kind, "method": method, "mean_below": mean_below, **result})

    estimates = pl.DataFrame(rows)
    estimates.write_csv(RESULTS / "rd_estimates.csv")
    if estimates.is_empty():
        raise RuntimeError("No regression could be estimated; see the messages above")

    # The main table: iv estimates at RD_MAIN_BANDWIDTH (first stage for in_high)
    pl.Config.set_tbl_rows(40)
    pl.Config.set_tbl_cols(10)
    pl.Config.set_fmt_str_lengths(50)
    main = (
        estimates.filter((pl.col("bandwidth") == RD_MAIN_BANDWIDTH) & pl.col("method").is_in(["first stage", "iv"]))
        .with_columns(effect=pl.format("{} ({})", pl.col("estimate").round(3), pl.col("se").round(3)))
        .pivot(on="sample", index=["kind", "label"], values="effect", aggregate_function="first")
    )
    print(f"\nEffect of the higher course (iv; first stage for 'in the higher course'), bandwidth {RD_MAIN_BANDWIDTH}, "
          "standard errors clustered by school in parentheses:")
    print(main)

    # Plots: means by distance to the threshold, all thresholds, widest bandwidth
    widest = max(RD_ESTIMATE_BANDWIDTHS)
    binned = (
        rd.filter(pl.col("distance").abs() <= widest + 1e-9)
        .group_by(pl.col("distance").round(2))
        .agg(*[pl.col(c).mean() for c in ["in_high", "prom_gral_t1", "repeated_t1", "adult_education"] if c in rd.columns], n=pl.len())
        .sort("distance")
    )
    figure, axes = plt.subplots(1, 4, figsize=(16, 3.8))
    for ax, (column, title) in zip(axes, [("in_high", "In the higher course"), ("prom_gral_t1", "GPA, 1 year later"),
                                          ("repeated_t1", "Failed, 1 year later"), ("adult_education", "Moved to adult education")]):
        below, above = binned.filter(pl.col("distance") < 0), binned.filter(pl.col("distance") >= 0)
        ax.scatter(below["distance"], below[column], color="#1f77b4")
        ax.scatter(above["distance"], above[column], color="#d62728")
        ax.axvline(-0.05, color="grey", linestyle="--", linewidth=1)
        ax.set_title(title)
        ax.set_xlabel("prior GPA minus threshold")
    figure.suptitle(f"Students just below (blue) and above (red) a threshold, {binned['n'].sum():,} students")
    figure.tight_layout()
    figure.savefig(RESULTS / "rd_plots.png", dpi=120)
    plt.close(figure)
    print("\n  results/rd_estimates.csv and results/rd_plots.png written")

# [End-of-file] ----
