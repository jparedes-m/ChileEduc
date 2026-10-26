# [A7] The figure that motivates the regression discontinuity ----
# For the students of A5, by distance to their threshold (prior GPA minus threshold, in steps of 0.1):
#   top     the share in the higher course, with a straight line fitted on each side of the threshold:
#           the jump at 0 is what the design uses
#   bottom  how many students there are at each distance, with the count written on each bar: the bar at 0
#           is the students exactly at the threshold
# Left: all thresholds. Right: only the groups that remixed their courses.
# Also prints the counts. Writes results/rd_first_stage.png on every run.

RESULTS.mkdir(parents=True, exist_ok=True)
rd = pl.read_parquet(ANALYSIS / "rd_sample.parquet")
if rd.is_empty():
    print("  no students in the regression discontinuity sample: no figure")
else:
    widest = max(RD_ESTIMATE_BANDWIDTHS)
    # Bins of 0.1 that never cross the threshold: [-0.1, 0) is below, [0, 0.1) above. GPAs have one decimal,
    # so in practice each bin is a single distance.
    rd = rd.filter(pl.col("distance").abs() <= widest + 1e-9).with_columns(
        distance=((pl.col("distance") / 0.1 + 1e-6).floor() * 0.1).round(1)
    )
    panels = {"All thresholds": rd, "Remixed courses only": rd.filter(pl.col("reshuffled").fill_null(False))}

    figure, axes = plt.subplots(2, 2, figsize=(13, 8), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
    for column, (title, data) in enumerate(panels.items()):
        bins = (
            data.group_by("distance")
            .agg(students=pl.len(), share_high=pl.col("in_high").cast(pl.Float64).mean())
            .sort("distance")
        )
        print(f"  {title}: {data.height:,} students, {data.select('year', 'group', 'low_rank').n_unique():,} thresholds")
        print(bins.with_columns(pl.col("share_high").round(3)))
        if bins.is_empty():
            continue
        below, above = bins.filter(pl.col("distance") < 0), bins.filter(pl.col("distance") >= 0)
        top, bottom = axes[0][column], axes[1][column]

        # Share in the higher course, and a line on each side (weighted by the students at each distance)
        for side, color in [(below, "#1f77b4"), (above, "#d62728")]:
            top.scatter(side["distance"], side["share_high"], s=40 + 160 * side["students"] / bins["students"].max(), color=color, zorder=3)
            if side.height >= 2:
                slope, intercept = np.polyfit(side["distance"], side["share_high"], 1, w=side["students"])
                x = np.array([side["distance"].min(), side["distance"].max()])
                top.plot(x, intercept + slope * x, color=color)
        top.axvline(-0.05, color="grey", linestyle="--", linewidth=1)
        top.set_ylim(-0.02, 1.02)
        top.set_ylabel("share in the higher course")
        top.set_title(f"{title}: {data.height:,} students")

        # Students at each distance
        colors = ["#d62728" if d >= 0 else "#1f77b4" for d in bins["distance"]]
        edges = ["black" if d == 0 else "none" for d in bins["distance"]]  # the bar of the students exactly at it
        bars = bottom.bar(bins["distance"], bins["students"], width=0.08, color=colors, edgecolor=edges, linewidth=1.5)
        bottom.bar_label(bars, labels=[f"{n:,}" for n in bins["students"]], fontsize=8)
        at = bins.filter(pl.col("distance") == 0)["students"]
        bottom.set_title(f"students at the threshold: {at[0] if at.len() else 0:,}", fontsize=10)
        bottom.axvline(-0.05, color="grey", linestyle="--", linewidth=1)
        bottom.set_ylabel("students")
        bottom.set_xlabel("prior GPA minus threshold (0 = exactly at the threshold, counted above)")
        bottom.set_ylim(0, bins["students"].max() * 1.15)

    figure.suptitle("Being just above the threshold and being in the higher course")
    figure.tight_layout()
    figure.savefig(RESULTS / "rd_first_stage.png", dpi=130)
    plt.close(figure)
    print("  results/rd_first_stage.png written")

# [End-of-file] ----
