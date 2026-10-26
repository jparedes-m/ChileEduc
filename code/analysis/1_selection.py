# [A1] Selection ----
# Builds, from the clean data, the student-year panel the analysis uses:
#   - only kinder (NT2) to 4° medio of regular education for children and youth: no sala cuna, nivel
#     medio or pre-kinder, no special education, no adult education
#   - without the records of students who withdrew (sit_fin Y) or transferred out (sit_fin_r T). A
#     student who left one school and enrolled in another the same year keeps the row of the new one;
#     a student who withdrew and was not enrolled anywhere else goes to the withdrawals table
#   - one row per student and year, with their age and how many years they are over the normal age
#     for their grade (rezago). Rows outside REZAGO_MIN..REZAGO_MAX are dropped as implausible
#   - prom_gral 0 (no grade) and asistencia over 100 (input errors) set to missing
# Writes data/analysis/panel/panel_<year>.parquet and data/analysis/withdrawals/withdrawals_<year>.parquet.
# asistencia here is the yearly attendance (%) that performance reports; A2 adds the one from assistance.
# Years already done are skipped.
#
# grado: 0 kinder, 1-8 = 1° to 8° básico, 9-12 = 1° to 4° medio. Normal age: 5 + grado.

YOUTH_MEDIA = [310, 410, 510, 610, 710, 810, 910]  # media HC, TP (5 branches) and artística, children and youth
PERFORMANCE_COLUMNS = [
    "year", "mrun", "rbd", "cod_ense", "cod_grado", "let_cur", "cod_tip_cur", "cod_jor", "cod_depe", "rural_rbd",
    "cod_com_rbd", "estado_estab", "gen_alu", "fec_nac_alu", "prom_gral", "asistencia", "sit_fin", "sit_fin_r", "int_alu",
]
I32, I64, F64, STR = pl.Int32, pl.Int64, pl.Float64, pl.String
PANEL_SCHEMA = {  # the same in every year, so all years read as one table
    "year": I32, "mrun": I64, "grado": I32, "rbd": I32, "id_estab_parv": I64, "origen": I32, "cod_ense": I32,
    "let_cur": STR, "cod_tip_cur": I32, "cod_jor": I32, "cod_depe": I32, "dependencia_parv": I32, "rural_rbd": I32,
    "cod_com_rbd": I32, "estado_estab": I32, "gen_alu": I32, "fec_nac_alu": I32, "edad": I32, "rezago": I32,
    "prom_gral": F64, "asistencia": I32, "sit_fin": STR, "int_alu": I32, "n_registros": I32,
}


def age_on_march_31(birth):
    """Age on March 31, the cutoff of the Chilean school year (a child starts 1° básico having turned 6 by then)."""
    year, month = pl.col(birth) // 100, pl.col(birth) % 100
    known = (year >= 1950) & month.is_between(1, 12)  # 190001, 175301 and the like mean "no information"
    return pl.when(known).then(pl.col("year") - year - (month > 3).cast(pl.Int32))


panel_dir, withdrawals_dir = ANALYSIS / "panel", ANALYSIS / "withdrawals"
panel_dir.mkdir(parents=True, exist_ok=True)
withdrawals_dir.mkdir(parents=True, exist_ok=True)
performance_files = {period_of(f)[0]: f for f in (CLEAN / "performance").glob("*.parquet")}
kinder_files = {period_of(f)[0]: f for f in (CLEAN / "kinder").glob("*.parquet")}

for year in sorted(set(performance_files) | set(kinder_files)):
    target = panel_dir / f"panel_{year}.parquet"
    if target.exists():
        continue
    parts, withdrawals = [], pl.DataFrame(schema={"year": pl.Int32, "mrun": pl.Int64, "grado": pl.Int32, "rbd": pl.Int32})

    # School: básica and media for children and youth
    if year in performance_files:
        available = [c for c in PERFORMANCE_COLUMNS if c in pl.read_parquet_schema(performance_files[year])]
        school = pl.read_parquet(performance_files[year], columns=available).with_columns(
            grado=pl.when(pl.col("cod_ense") == 110).then(pl.col("cod_grado"))
            .when(pl.col("cod_ense").is_in(YOUTH_MEDIA)).then(pl.col("cod_grado") + 8)
        )
        school = school.filter(pl.col("grado").is_between(1, 12) & pl.col("mrun").is_not_null())
        left = ((pl.col("sit_fin") == "Y") | (pl.col("sit_fin_r") == "T")).fill_null(False)
        stayed = school.filter(~left)
        withdrawals = (
            school.filter(left).join(stayed.select("mrun"), on="mrun", how="anti")
            .unique("mrun", keep="first", maintain_order=True).select(withdrawals.columns)
        )
        # A student with more than one record left: keep the one where they attended most
        records = stayed.group_by("mrun").agg(pl.len().cast(pl.Int32).alias("n_registros"))
        stayed = (
            stayed.sort("asistencia", descending=True, nulls_last=True)
            .unique("mrun", keep="first", maintain_order=True)
            .join(records, on="mrun")
            .with_columns(origen=pl.lit(1, pl.Int32))
        )
        parts.append(stayed)

    # Kinder (NT2) in any of the three programs, without MINEDUC's special education
    if year in kinder_files:
        kinder = (
            pl.read_parquet(kinder_files[year])
            .filter((pl.col("nivel1") == 6) & ((pl.col("origen") != 1) | (pl.col("cod_ense1_m") == 10)))
            .filter(pl.col("mrun").is_not_null())
            .select(
                "year", "mrun", "rbd", "origen",
                pl.lit(0, pl.Int32).alias("grado"),
                pl.col("school_id").alias("id_estab_parv"),
                pl.col("dependencia").alias("dependencia_parv"),
                pl.col("rural_establishment").alias("rural_rbd"),
                pl.col("cod_com_estab").alias("cod_com_rbd"),
                pl.col("sex_stud").alias("gen_alu"),
                pl.col("birth_date_stud").alias("fec_nac_alu"),
                pl.lit(1, pl.Int32).alias("n_registros"),
            )
            .unique("mrun", keep="first", maintain_order=True)
        )
        parts.append(kinder)

    panel = (
        pl.concat(parts, how="diagonal_relaxed")
        .sort("grado", descending=True)
        .unique("mrun", keep="first", maintain_order=True)  # in kinder and in school the same year: keep school
        .with_columns(edad=age_on_march_31("fec_nac_alu"))
        .with_columns(rezago=pl.col("edad") - 5 - pl.col("grado"))
    )
    plausible = pl.col("rezago").is_between(REZAGO_MIN, REZAGO_MAX) | pl.col("rezago").is_null()
    implausible = panel.filter(~plausible).height
    panel = panel.filter(plausible).with_columns(
        prom_gral=pl.when(pl.col("prom_gral") > 0).then(pl.col("prom_gral")),
        asistencia=pl.when(pl.col("asistencia") <= 100).then(pl.col("asistencia")),
    )
    panel = panel.select(
        (pl.col(c) if c in panel.columns else pl.lit(None)).cast(dtype).alias(c) for c, dtype in PANEL_SCHEMA.items()
    ).sort("mrun")

    partial = target.with_name(target.name + ".part")
    panel.write_parquet(partial, compression="zstd")
    partial.replace(target)
    withdrawals.write_parquet(withdrawals_dir / f"withdrawals_{year}.parquet", compression="zstd")
    kinder_rows = panel.filter(pl.col("grado") == 0).height
    print(f"  {year}: {panel.height:,} students ({kinder_rows:,} in kinder), {withdrawals.height:,} withdrew, "
          f"{implausible:,} dropped for an implausible age, {panel['edad'].null_count():,} without age")

# [End-of-file] ----
