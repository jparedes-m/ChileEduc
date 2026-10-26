# [3] Unification ----
# Gives every year of a dataset the same columns, names, order and types, and keeps
# regular education only. Reads data/intermediate/<dataset>/ and writes data/clean/<dataset>/
# (the inputs are never overwritten). Years already unified are skipped.
#
# The mapping file (config/data_mapping.xlsx) has one sheet per dataset: a "desired_names"
# column, an optional "description" column, and one column per year (2015, or 2015_03 for
# assistance) with that year's original column name.
#
# Published columns that the mapping does not mention are kept too, under their own name
# in lowercase, if they appear in at least MIN_YEAR_SHARE of the years.

# Column types. Columns not listed here are kept as text.
I32, I64, F64, STR = pl.Int32, pl.Int64, pl.Float64, pl.String
SCHOOL = {
    "rbd": I32, "dgv_rbd": I32, "nom_rbd": STR, "cod_reg_rbd": I32, "nom_reg_rbd_a": STR,
    "cod_pro_rbd": I32, "cod_com_rbd": I32, "nom_com_rbd": STR, "cod_deprov_rbd": I32,
    "nom_deprov_rbd": STR, "rural_rbd": I32, "cod_depe": I32, "cod_depe2": I32,
}
TYPES = {
    "kinder": {
        "year": I32, "month": I32, "mrun": I64, "sex_stud": I32, "birth_date_stud": I32,
        "school_id": I64, "rbd": I32, "id_estab_j": I64, "id_estab_i": I64, "nom_estab": STR,
        "cod_reg_estab": I32, "cod_pro_estab": I32, "cod_com_estab": I32, "nom_reg_estab": STR,
        "nom_reg_a_estab": STR, "nom_pro_estab": STR, "nom_com_estab": STR,
        "cod_deprov_estab": I32, "nom_deprov_estab": STR, "latitude": F64, "longitude": F64,
        "rural_establishment": I32, "origen": I32, "dependencia": I32, "nivel1": I32,
        "nivel2": I32, "cod_ense1_m": I32, "cod_grado_m": I32, "let_cur_m": STR,
        "cod_tip_cur_m": I32, "cod_depe1_m": I32, "cod_ense2_m": I32, "estado_estab_m": I32,
        "corr_gru_j": I64, "cod_prog_j": I32, "desc_prog_j": STR, "cod_nivel_j": I32,
        "desc_nivel_j": STR, "cod_modal_j": I32, "desc_modal_j": STR, "cod_jor_j": I32,
        "nom_jor_j": STR, "desc_mod_i": STR, "desc_niv_i": STR, "cod_nivel_i": I32,
        "cod_grupo_i": I64, "tipo_sostenedor": I32, "formal": I32, "asis_real_j": F64,
        "asis_poten_j": F64, "dias_trab_grupo_j": F64,
    },
    "assistance": {
        "year": I32, "month": I32, **SCHOOL, "cod_ense": I32, "cod_ense2": I32, "cod_grado": I32,
        "let_cur": STR, "mrun": I64, "gen_alu": I32, "fec_nac_alu": I32, "cod_com_alu": I32,
        "nom_com_alu": STR, "dias_asistidos": I32, "dias_trabajados": I32, "asis_promedio": F64,
    },
    "performance": {
        "year": I32, **SCHOOL, "estado_estab": I32, "cod_ense": I32, "cod_ense2": I32,
        "cod_grado": I32, "let_cur": STR, "cod_jor": I32, "cod_tip_cur": I32, "cod_des_cur": I32,
        "mrun": I64, "gen_alu": I32, "fec_nac_alu": STR, "edad_alu": I32, "cod_reg_alu": I32,
        "cod_com_alu": I32, "nom_com_alu": STR, "cod_rama": I32, "cod_sec": I32, "cod_espe": I32,
        "prom_gral": F64, "asistencia": I32, "sit_fin": STR, "sit_fin_r": STR, "cod_men": I32,
    },
}

# Regular education (cod_ense, Anexo II of the MINEDUC record layout): 10 parvularia,
# 110 basica, 310 media HC, 410-910 media TP and artistica. Special and adult education are dropped.
REGULAR_ENSE = [10, 110, 310, 410, 510, 610, 710, 810, 910]


def as_type(column, dtype):
    """Text -> number. Accepts decimal commas (6,5) and '10.0'. What cannot be read becomes null."""
    text = pl.col(column).str.strip_chars()
    if dtype == pl.String:
        return text
    number = text.str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)
    return number.cast(dtype, strict=False)


for dataset in DATASETS:
    out = CLEAN / dataset
    out.mkdir(parents=True, exist_ok=True)
    files = sorted((INTERMEDIATE / dataset).glob("*.parquet"))
    mapping = read_mapping(dataset)
    desired = [d.strip().lower() for d in mapping["desired_names"]]

    # Published columns missing from the mapping that appear in at least MIN_YEAR_SHARE of the years
    in_mapping = {d.upper() for d in desired}
    for year_col in year_columns(mapping):
        in_mapping |= {name.strip().upper() for name in mapping[year_col].drop_nulls()}
    columns_by_year = {}
    for f in files:
        columns_by_year.setdefault(period_of(f)[0], set()).update(c.upper() for c in pl.read_parquet_schema(f))
    years_with = Counter(c for cols in columns_by_year.values() for c in cols)
    extras = sorted(c for c, n in years_with.items() if n >= MIN_YEAR_SHARE * len(columns_by_year) and c not in in_mapping)
    print(f"{dataset}: {len(files)} files, {len(desired)} mapped columns + {len(extras)} extra: {', '.join(extras)}")

    for f in files:
        target = out / f.name
        if target.exists():
            continue
        year, month = period_of(f)
        key = str(year) if month is None else f"{year}_{month:02d}"
        if key not in mapping.columns:
            print(f"  {f.stem}: the mapping has no column {key}, skipped")
            continue
        raw = pl.read_parquet(f)
        published = {c.upper(): c for c in raw.columns}  # match names without caring about upper/lower case

        # Rename: each column takes its original column of this year, or nulls if it has none
        originals = list(zip(desired, mapping[key])) + [(e.lower(), e) for e in extras]  # extras keep their name
        columns = []
        for name, original in originals:
            found = published.get(original.strip().upper()) if original else None
            columns.append(pl.col(found).alias(name) if found else pl.lit(None, pl.String).alias(name))
        df = raw.select(columns)
        if "year" not in df.columns:
            df = df.with_columns(pl.lit(str(year)).alias("year"))
        if month is not None and "month" not in df.columns:
            df = df.with_columns(pl.lit(str(month)).alias("month"))

        # Types, and a warning for values that could not be read as numbers
        types = TYPES[dataset]
        typed = df.select(as_type(c, types.get(c, pl.String)) for c in df.columns)
        for c in typed.columns:
            lost = ((df[c].str.strip_chars().fill_null("") != "") & typed[c].is_null()).sum()
            if lost:
                print(f"  {f.stem}: {lost} values of {c} are not numbers, set to null")

        # Regular education only
        if dataset == "kinder" and year < 2012:
            keep = pl.col("cod_ense1_m") == 10  # no special education
        elif dataset == "kinder":
            keep = (pl.col("cod_ense2_m") == 1) & pl.col("cod_jor_j").is_in([1, 2])  # full day or morning
        else:
            keep = pl.col("cod_ense").is_in(REGULAR_ENSE)
        clean = typed.filter(keep)

        partial = target.with_name(target.name + ".part")
        clean.write_parquet(partial, compression="zstd")
        partial.replace(target)
        print(f"  {f.stem}: {clean.height:,} rows kept of {raw.height:,}")
