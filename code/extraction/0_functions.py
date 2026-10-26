# [0] Functions used by more than one script ----

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
    "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def normalize(text):
    """Lowercase without accents: 'Matrícula' -> 'matricula'."""
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c)).lower()


def file_name_for(dataset, original_name, monthly):
    """Standard name from a MINEDUC file name, or None if the name has no year.

    'Rendimiento-2015.zip'                        -> 'performance_2015'
    'Asistencia-declarada-mensual-marzo-2015.rar' -> 'assistance_2015_03'
    """
    name = normalize(original_name)
    year = re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", name)  # a year alone, not inside a date like 20160202
    if year is None:
        return None
    if not monthly:
        return f"{dataset}_{year.group()}"
    month = re.search(r"(?<![a-z])(" + "|".join(MONTHS) + r")(?![a-z])", name)
    if month is None:
        return None
    return f"{dataset}_{year.group()}_{MONTHS[month.group(1)]:02d}"


def period_of(path):
    """Year and month of a standard file: 'assistance_2015_03.parquet' -> (2015, 3), 'performance_2015' -> (2015, None)."""
    year, month = re.search(r"_(\d{4})(?:_(\d{2}))?$", path.stem).groups()
    return int(year), (int(month) if month else None)


def read_mapping(dataset):
    mapping = pl.read_excel(MAPPING, sheet_name=dataset, infer_schema_length=0)  # everything as text
    mapping = mapping.rename(lambda c: str(c).strip().removesuffix(".0"))  # year headers: 2015.0 -> "2015"
    return mapping.filter(pl.col("desired_names").is_not_null())


def year_columns(mapping):
    return [c for c in mapping.columns if c != "desired_names"]

def in_sample(fraction):
    """The students of the sample: each mrun gets a fixed number in [0, 1), the same on any computer and in
    every dataset, and the sample is the students below fraction (so a smaller sample is inside a bigger one)."""
    return ((pl.col("mrun").cast(pl.UInt64) + SEED) * 2654435761 % 2**32) / 2**32 < fraction


def estimation_half():
    """Half of the students of each year, the same on any computer: the regression discontinuity finds the
    thresholds with this half and measures the effects with the other one."""
    return ((pl.col("mrun").cast(pl.UInt64) + pl.col("year").cast(pl.UInt64) * 7919 + SEED) * 2654435761 % 2**32) < 2**31

# [End-of-file] ----
