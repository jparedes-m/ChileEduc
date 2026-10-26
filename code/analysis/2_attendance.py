# [A2] Attendance ----
# Yearly attendance of each student from the monthly attendance data (assistance), to join to the panel.
# Every record of the student counts, in any school: a student who changed school in a month has two
# records that month, so the month's days attended are added up (up to the days worked) and the days
# worked are those of the school that worked the most that month.
# Writes data/analysis/attendance/attendance_<year>.parquet, one row per student and year:
#   asis_dias_asistidos, asis_dias_trabajados  days attended and worked in the year
#   asis_anual                                 asis_dias_asistidos / asis_dias_trabajados (0 to 1)
#   asis_meses                                 months with attendance data (at most 10, March to December)
#   asis_primer_mes, asis_ultimo_mes           first and last month with a record (a student who leaves
#                                              school mid-year stops appearing)
#   asis_n_colegios, asis_n_cursos             schools and courses (rbd + teaching code + grade + letter)
#                                              the student had a record in: more than 1 means they changed
# There is no attendance data for 2020 and 2021. Years already done are skipped.

out = ANALYSIS / "attendance"
out.mkdir(parents=True, exist_ok=True)
files_by_year = {}
for f in (CLEAN / "assistance").glob("*.parquet"):
    files_by_year.setdefault(period_of(f)[0], []).append(f)

for year, files in sorted(files_by_year.items()):
    target = out / f"attendance_{year}.parquet"
    if target.exists():
        continue
    partial = target.with_name(target.name + ".part")
    records = (
        pl.scan_parquet(files)
        .select("mrun", "month", "rbd", "cod_ense", "cod_grado", "let_cur", "dias_asistidos", "dias_trabajados")
        .filter(pl.col("mrun").is_not_null())
    )
    days = (
        records.group_by("mrun", "month")
        .agg(pl.col("dias_asistidos").sum(), pl.col("dias_trabajados").max())
        .with_columns(dias_asistidos=pl.min_horizontal("dias_asistidos", "dias_trabajados"))
        .group_by("mrun")
        .agg(
            pl.col("dias_asistidos").sum().cast(pl.Int32).alias("asis_dias_asistidos"),
            pl.col("dias_trabajados").sum().cast(pl.Int32).alias("asis_dias_trabajados"),
            pl.col("month").n_unique().cast(pl.Int32).alias("asis_meses"),
            pl.col("month").min().cast(pl.Int32).alias("asis_primer_mes"),
            pl.col("month").max().cast(pl.Int32).alias("asis_ultimo_mes"),
        )
    )
    moves = records.group_by("mrun").agg(
        pl.col("rbd").n_unique().cast(pl.Int32).alias("asis_n_colegios"),
        pl.concat_str("rbd", "cod_ense", "cod_grado", "let_cur", separator="-").n_unique().cast(pl.Int32).alias("asis_n_cursos"),
    )
    (
        days.join(moves, on="mrun")
        .with_columns(
            pl.lit(year, pl.Int32).alias("year"),
            asis_anual=pl.when(pl.col("asis_dias_trabajados") > 0)
            .then(pl.col("asis_dias_asistidos") / pl.col("asis_dias_trabajados")),
        )
        .select("year", "mrun", "asis_anual", "asis_dias_asistidos", "asis_dias_trabajados", "asis_meses",
                "asis_primer_mes", "asis_ultimo_mes", "asis_n_colegios", "asis_n_cursos")
        .sink_parquet(partial, compression="zstd")
    )
    partial.replace(target)
    print(f"  {year}: {pl.scan_parquet(target).select(pl.len()).collect().item():,} students")

# [End-of-file] ----
