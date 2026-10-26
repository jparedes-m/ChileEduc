# [4] Dictionary ----
# Writes docs/dictionary/<dataset>.md from the clean data:
#   - every variable with its type, description, original names, years with data, % non-missing and range
#   - rows per year
#   - every column MINEDUC published, with the years it appears in (shows what the mapping is missing)
# Descriptions come from the "description" column of the mapping file. Rewritten on every run.


def years_text(years):
    """[2002, 2003, 2004, 2011] -> '2002-2004, 2011'."""
    years, groups = sorted(y for y in years if y is not None), []
    for y in years:
        if groups and y == groups[-1][1] + 1:
            groups[-1][1] = y
        else:
            groups.append([y, y])
    return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in groups)


def md_table(header, rows):
    clean = lambda v: "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    return lines + ["| " + " | ".join(clean(v) for v in row) + " |" for row in rows]


DICTIONARY.mkdir(parents=True, exist_ok=True)
for dataset in DATASETS:
    files = sorted((CLEAN / dataset).glob("*.parquet"))
    if not files:
        print(f"{dataset}: no clean files yet")
        continue
    data = pl.scan_parquet(files)
    schema = data.collect_schema()
    variables = [c for c in schema.names() if c != "year"]
    numeric = [c for c in variables if schema[c].is_numeric()]

    # Non-missing values of each variable per year, and min/max of the numeric ones (two passes over the data)
    per_year = (
        data.group_by("year")
        .agg(pl.len().alias("rows"), *[pl.col(c).is_not_null().sum() for c in variables])
        .sort("year")
        .collect()
    )
    lows_highs = data.select(
        *[pl.col(c).min().alias(f"{c} min") for c in numeric],
        *[pl.col(c).max().alias(f"{c} max") for c in numeric],
    ).collect().row(0, named=True)

    # Descriptions and original names, from the mapping
    mapping = read_mapping(dataset)
    desired = [d.strip().lower() for d in mapping["desired_names"]]
    descriptions = dict(zip(desired, mapping["description"])) if "description" in mapping.columns else {}
    originals = {d: set() for d in desired}
    for year_col in year_columns(mapping):
        for d, original in zip(desired, mapping[year_col]):
            if original:
                originals[d].add(original.strip())

    total = per_year["rows"].sum()
    variable_rows = [["year", schema["year"], "", "", years_text(per_year["year"]), "100.0", ""]]
    for c in variables:
        variable_rows.append([
            c,
            schema[c],
            descriptions.get(c),
            ", ".join(sorted(originals.get(c, {c.upper()}))),  # extras were published under their own name
            years_text(per_year.filter(pl.col(c) > 0)["year"]),
            f"{100 * per_year[c].sum() / total:.1f}",
            f"{lows_highs[c + ' min']} to {lows_highs[c + ' max']}" if c in numeric else "",
        ])

    # Every published column and its years, from the headers of the intermediate files
    published_in = {}
    for f in (INTERMEDIATE / dataset).glob("*.parquet"):
        for c in pl.read_parquet_schema(f):
            published_in.setdefault(c.upper(), set()).add(period_of(f)[0])
    used_as = {o.upper(): d for d, names in originals.items() for o in names}
    published_rows = sorted(
        ([c, years_text(ys), len(ys), used_as.get(c) or (c.lower() if c.lower() in schema else "not kept")]
         for c, ys in published_in.items()),
        key=lambda r: (-r[2], r[0]),
    )

    lines = [
        f"# {dataset.capitalize()}: data dictionary",
        "",
        "Made by `code/4_dictionary.py`; do not edit by hand. Descriptions come from the `description` column of",
        "`config/data_mapping.xlsx`.",
        "",
        f"{total:,} rows, {per_year.height} years.",
        "",
        "## Variables",
        "",
        *md_table(["variable", "type", "description", "original names", "years with data", "% non-missing", "range"],
                  variable_rows),
        "",
        "## Rows per year",
        "",
        *md_table(["year", "rows"], [[y, f"{n:,}"] for y, n in per_year.select("year", "rows").iter_rows()]),
        "",
        "## Columns published by MINEDUC",
        "",
        *md_table(["published name", "years", "number of years", "clean variable"], published_rows),
        "",
    ]
    (DICTIONARY / f"{dataset}.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"{dataset}: docs/dictionary/{dataset}.md written")

# [End-of-file] ----
