# [A6] Regression discontinuity results ----
# The effect of being sent to the higher course, for the students of A5. See "Reading the results" in the README.
#
# For each specification in SPECS, bandwidth in RD_ESTIMATE_BANDWIDTHS and outcome: a local linear regression
# on each side of the threshold, a fixed effect per threshold (students are only compared with those of the
# same school, grade, shift, year and pair of courses), and errors clustered by school.
#   first stage  in_high ~ above + distance + above x distance | threshold
#   effect (iv)  outcome ~ in_high + distance + above x distance | threshold, in_high instrumented by above
# Balance rows (fixed before the assignment) should not jump at the threshold.
#
# Writes, on every run, to results/ (in git, they are small summaries):
#   rd_estimates.csv     every estimate
#   rd_first_stage.png   the figure behind the design: share in the higher course and number of students at
#                        each distance to the threshold (main specification and remixed courses only)
#   rd_outcomes.png      the main outcomes at each distance to the threshold

OUTCOMES = {  # column: (label, kind)
    "female": ("female", "balance"),
    "edad": ("age", "balance"),
    "repeater": ("repeater (same grade the year before)", "balance"),
    "prom_gral": ("GPA, that year", "outcome"),
    "prom_gral_t1": ("GPA, 1 year later", "outcome"),
    "prom_gral_t2": ("GPA, 2 years later", "outcome"),
    "repeated": ("failed the grade, that year", "outcome"),
    "repeated_t1": ("failed the grade, 1 year later", "outcome"),
    "asis_anual": ("attendance (0-1), that year", "outcome"),
    "asis_anual_t1": ("attendance (0-1), 1 year later", "outcome"),
    "same_school_t1": ("same school 1 year later", "outcome"),
    "finished": ("finished 4° medio (cohort 1° básico)", "outcome"),
    "adult_education": ("moved to adult education (cohort 1° básico)", "outcome"),
}
CONTROLS = ["female", "edad", "repeater"]
SPECS = {  # name: (which students, with controls)
    "main": (pl.lit(True), False),                                # every threshold
    "remixed": (pl.col("reshuffled").fill_null(False), False),   # only groups that remixed their courses
    "donut": (pl.col("distance") != 0, False),                    # without the students exactly at the threshold
    "controls": (pl.lit(True), True),                             # controlling for CONTROLS
    "basica": (pl.col("grado") <= 8, False),                      # 2° to 8° básico
    "media": (pl.col("grado") >= 9, False),                       # 1° to 4° medio
    "2003-12": (pl.col("year") <= 2012, False),
    "2013-23": (pl.col("year") >= 2013, False),
}


def estimate(data, formula, coefficient):
    """Estimate, standard error, p-value and students used for one coefficient, or None if it cannot be estimated."""
    try:
        fit = pf.feols(formula, data=data, vcov={"CRV1": "rbd"})
        row = fit.tidy().loc[coefficient]
        return {"estimate": row["Estimate"], "se": row["Std. Error"], "p_value": row["Pr(>|t|)"], "n": fit._N}
    except Exception as error:  # e.g. no variation in the outcome within the bandwidth
        print(f"    could not estimate {formula}: {type(error).__name__}")
        return None


def binned_plot(axes, data, column, title, counts):
    """Mean of column at each distance to the threshold (blue below, red above), point size = students there."""
    bins = data.group_by("distance").agg(students=pl.len(), mean=pl.col(column).cast(pl.Float64).mean()).sort("distance")
    for side, color in [(bins.filter(pl.col("distance") < 0), "#1f77b4"), (bins.filter(pl.col("distance") >= 0), "#d62728")]:
        axes.scatter(side["distance"], side["mean"], s=30 + 170 * side["students"] / bins["students"].max(), color=color, zorder=3)
        if side.height >= 2:  # a line on each side, weighted by the students at each distance
            slope, intercept = np.polyfit(side["distance"], side["mean"], 1, w=side["students"])
            x = np.array([side["distance"].min(), side["distance"].max()])
            axes.plot(x, intercept + slope * x, color=color)
    axes.axvline(-0.05, color="grey", linestyle="--", linewidth=1)
    axes.set_title(title)
    if counts is not None:  # number of students at each distance, with the bar at the threshold outlined
        bars = counts.bar(bins["distance"], bins["students"], width=0.08,
                          color=["#d62728" if d >= 0 else "#1f77b4" for d in bins["distance"]],
                          edgecolor=["black" if d == 0 else "none" for d in bins["distance"]], linewidth=1.5)
        counts.bar_label(bars, labels=[f"{n:,}" for n in bins["students"]], fontsize=8)
        at = bins.filter(pl.col("distance") == 0)["students"]
        counts.set_title(f"students at each distance ({at[0] if at.len() else 0:,} exactly at the threshold)", fontsize=10)
        counts.set_ylim(0, bins["students"].max() * 1.15)
        counts.set_xlabel("prior GPA minus threshold (0 = exactly at it, counted above)")


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
        # Bins of 0.1 that never cross the threshold. GPAs have 1 decimal, so in practice each is one distance.
        distance=((pl.col("distance") / 0.1 + 1e-6).floor() * 0.1).round(1),
    )
    outcomes = {c: v for c, v in OUTCOMES.items() if c in rd.columns}  # e.g. no _t2 when the data ends a year later

    # Estimates
    rows = []
    for spec, (keep, with_controls) in SPECS.items():
        controls = "".join(f" + {c}" for c in CONTROLS) if with_controls else ""
        for bandwidth in RD_ESTIMATE_BANDWIDTHS:
            data = rd.filter(keep & (pl.col("distance").abs() <= bandwidth + 1e-9))
            df = data.select("pair", "rbd", "above", "in_high", "distance", "above_x_distance", *outcomes).to_pandas()
            print(f"  {spec}, bandwidth {bandwidth}: {len(df):,} students, {df['pair'].nunique():,} thresholds")
            fits = [("in_high", "in the higher course", "first stage",
                     f"in_high ~ above + distance + above_x_distance{controls} | pair", "above")]
            fits += [(c, label, kind, f"{c} ~ distance + above_x_distance{controls} | pair | in_high ~ above", "in_high")
                     for c, (label, kind) in outcomes.items() if not (with_controls and kind == "balance")]
            for outcome, label, kind, formula, coefficient in fits:
                if df[outcome].notna().any() and (result := estimate(df, formula, coefficient)):
                    rows.append({"spec": spec, "bandwidth": bandwidth, "outcome": outcome, "label": label, "kind": kind,
                                 "mean_below": df.loc[df.above == 0, outcome].mean(), **result})
    estimates = pl.DataFrame(rows)
    if estimates.is_empty():
        raise RuntimeError("No regression could be estimated; see the messages above")
    estimates.write_csv(RESULTS / "rd_estimates.csv")

    pl.Config.set_tbl_rows(40)
    pl.Config.set_tbl_cols(12)
    pl.Config.set_fmt_str_lengths(45)
    pl.Config.set_tbl_width_chars(250)
    pl.Config.set_tbl_hide_column_data_types(True)
    stars = pl.when(pl.col("p_value") < 0.01).then(pl.lit("***")).when(pl.col("p_value") < 0.05).then(pl.lit("**")) \
        .when(pl.col("p_value") < 0.1).then(pl.lit("*")).otherwise(pl.lit(""))
    print(f"\nEffect of being in the higher course (first stage: effect of being above the threshold), bandwidth "
          f"{RD_MAIN_BANDWIDTH}, standard error clustered by school in parentheses; * p<0.1, ** p<0.05, *** p<0.01")
    print(
        estimates.filter(pl.col("bandwidth") == RD_MAIN_BANDWIDTH)
        .with_columns(effect=pl.format("{}{} ({})", pl.col("estimate").round(3), stars, pl.col("se").round(3)))
        .pivot(on="spec", index=["kind", "label"], values="effect", aggregate_function="first")
    )

    # Figures, at the widest bandwidth
    rd = rd.filter(pl.col("distance").abs() <= max(RD_ESTIMATE_BANDWIDTHS) + 1e-9)
    figure, axes = plt.subplots(2, 2, figsize=(13, 8), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
    for column, (spec, keep) in enumerate([("all thresholds", pl.lit(True)), ("remixed courses only", SPECS["remixed"][0])]):
        data = rd.filter(keep)
        binned_plot(axes[0][column], data, "in_high", f"{spec}: {data.height:,} students", counts=axes[1][column])
        axes[0][column].set_ylim(-0.02, 1.02)
        axes[0][column].set_ylabel("share in the higher course")
        axes[1][column].set_ylabel("students")
    figure.suptitle("Being just above the threshold and being in the higher course\n"
                    "(each point: mean of the students at that distance; size: how many they are)")
    figure.tight_layout()
    figure.savefig(RESULTS / "rd_first_stage.png", dpi=130)
    plt.close(figure)

    shown = [c for c in ["prom_gral_t1", "repeated_t1", "asis_anual_t1", "adult_education"] if c in rd.columns]
    figure, axes = plt.subplots(1, len(shown), figsize=(4 * len(shown), 3.8))
    for ax, column in zip(np.atleast_1d(axes), shown):
        binned_plot(ax, rd, column, OUTCOMES[column][0], counts=None)
        ax.set_xlabel("prior GPA minus threshold")
    figure.suptitle(f"Outcomes just below (blue) and above (red) the threshold, {rd.height:,} students")
    figure.tight_layout()
    figure.savefig(RESULTS / "rd_outcomes.png", dpi=120)
    plt.close(figure)
    print("\n  results/rd_estimates.csv, rd_first_stage.png and rd_outcomes.png written")

# [End-of-file] ----
