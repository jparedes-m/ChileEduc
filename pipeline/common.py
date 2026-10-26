"""Helpers shared by the pipeline steps."""

import re
import unicodedata

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

# A standalone year: not part of a longer number such as a 20160202 date stamp.
_YEAR = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
_MONTH = re.compile(r"(?<![a-z])(" + "|".join(MONTHS) + r")(?![a-z])")


def normalize(text: str) -> str:
    """Lowercase and strip accents, so 'Matrícula' and 'matricula' compare equal."""
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c)).lower()


def parse_period(name: str, monthly: bool) -> tuple[int, int | None] | None:
    """Read (year, month) from a MINEDUC file name.

    'Asistencia-declarada-mensual-marzo-2015.rar' -> (2015, 3)
    'Rendimiento-2015.zip'                        -> (2015, None)
    Returns None when the name does not carry the period.
    """
    name = normalize(name)
    year = _YEAR.search(name)
    if year is None:
        return None
    if not monthly:
        return int(year.group()), None
    month = _MONTH.search(name)
    if month is None:
        return None
    return int(year.group()), MONTHS[month.group(1)]


def period_stem(dataset: str, year: int, month: int | None) -> str:
    """Standard file name (without extension): performance_2015, assistance_2015_03."""
    return f"{dataset}_{year}" if month is None else f"{dataset}_{year}_{month:02d}"


def parse_stem(stem: str) -> tuple[int, int | None]:
    """Inverse of period_stem: 'assistance_2015_03' -> (2015, 3)."""
    match = re.search(r"_(\d{4})(?:_(\d{2}))?$", stem)
    if match is None:
        raise ValueError(f"{stem} is not a standard <dataset>_<year>[_<month>] name")
    year, month = match.groups()
    return int(year), None if month is None else int(month)
