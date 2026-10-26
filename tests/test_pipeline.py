"""End-to-end test of extraction -> unification -> sampling on small synthetic files."""

import zipfile
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from pipeline import dictionary, extraction, sampling, scraping, unification
from pipeline.common import parse_period

DATASETS = {"performance": {"url": "", "monthly": False}, "assistance": {"url": "", "monthly": True}}


@pytest.mark.parametrize(
    "name, monthly, expected",
    [
        ("Rendimiento-2015.zip", False, (2015, None)),
        ("20160202_Rendimiento_2015_PUBL.csv", False, (2015, None)),
        ("Asistencia-declarada-mensual-Septiembre-2019.rar", True, (2019, 9)),
        ("asistencia_marzo_2012_20120420.csv", True, (2012, 3)),
        ("Matrícula-Parvularia-2011.rar", False, (2011, None)),
        ("Asistencia-2019.rar", True, None),
        ("libro_codigos.pdf", False, None),
    ],
)
def test_parse_period(name, monthly, expected):
    assert parse_period(name, monthly) == expected


def test_archive_links_and_plan(tmp_path):
    html = """<a href="/files/Rendimiento-2014.zip">a</a>
              <a href="https://x.cl/f/Rendimiento-2015.rar?v=1">b</a>
              <a href="/files/Rendimiento-2014.zip">dup</a>
              <a href="/files/manual.pdf">pdf</a>"""
    links = scraping._archive_links(html, "https://datosabiertos.mineduc.cl/page/")
    assert links == [
        "https://datosabiertos.mineduc.cl/files/Rendimiento-2014.zip",
        "https://x.cl/f/Rendimiento-2015.rar?v=1",
    ]
    plan = scraping.plan_downloads(links, "performance", False, tmp_path)
    assert [p.name for p in plan.values()] == ["performance_2014.zip", "performance_2015.rar"]


def write_zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)


@pytest.fixture
def project(tmp_path):
    raw = tmp_path / "raw"
    (raw / "performance").mkdir(parents=True)
    (raw / "assistance").mkdir(parents=True)

    # Latin-1 CSV with decimal commas and a BOM-free header, plus a frequency table to ignore.
    perf = (
        "AGNO;RBD;COD_ENSE;COD_GRADO;LET_CUR;MRUN;PROM_GRAL;NOM_RBD;COD_MEN\n"  # COD_MEN is not in the mapping
        "2015;100;110;5;A;1;6,5;Escuela Ñuñoa;1\n"
        "2015;100;110;5;B;2;4,1;Escuela Ñuñoa;0\n"
        "2015;100;211;5;A;3;5,0;Escuela Ñuñoa;0\n"  # special education, dropped
        "2015;100;310;1;A;4;x;Escuela Ñuñoa;0\n"  # unreadable grade
    ).encode("latin-1")
    write_zip(raw / "performance" / "performance_2015.zip", {
        "Rendimiento_2015/20160101_Rendimiento_2015.csv": perf,
        "Rendimiento_2015/Frecuencia_2015.csv": b"a;b\n1;2\n",
        "Rendimiento_2015/Registro_2015.pdf": b"%PDF-1.4",
    })

    # A nested archive: the monthly CSV sits in a zip inside the zip, UTF-8 with BOM.
    inner = tmp_path / "inner.zip"
    asis = "﻿AGNO;MES;MRUN;COD_ENSE;DIAS_ASISTIDOS\n2015;3;1;110;18\n2015;3;2;110;20\n".encode()
    write_zip(inner, {"asistencia_marzo_2015.csv": asis})
    write_zip(raw / "assistance" / "assistance_2015_03.zip", {"carpeta/asistencia.zip": inner.read_bytes()})

    mapping = tmp_path / "data_mapping.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "performance"
    ws.append(["desired_names", 2015, "description"])
    for row in [["year", "AGNO", "School year"], ["rbd", "RBD", "School id | RBD"], ["cod_ense", "COD_ENSE"], ["cod_grado", "COD_GRADO"],
                ["let_cur", "LET_CUR"], ["mrun", "MRUN"], ["prom_gral", "PROM_GRAL"], ["nom_rbd", "nom_rbd"],
                ["sit_fin", None]]:
        ws.append(row)
    ws = wb.create_sheet("assistance")
    ws.append(["desired_names", "2015_03"])
    for row in [["mrun", "MRUN"], ["cod_ense", "COD_ENSE"], ["dias_asistidos", "DIAS_ASISTIDOS"]]:
        ws.append(row)
    wb.save(mapping)
    return tmp_path, mapping


def test_pipeline_end_to_end(project):
    root, mapping = project
    extraction.run(DATASETS, root / "raw", root / "intermediate", root / "docs")
    assert sorted(p.name for p in (root / "intermediate").rglob("*.parquet")) == [
        "assistance_2015_03.parquet", "performance_2015.parquet"]
    docs = root / "docs" / "performance" / "performance_2015" / "Rendimiento_2015"
    assert sorted(p.name for p in docs.iterdir()) == ["Frecuencia_2015.csv", "Registro_2015.pdf"]

    unification.run(DATASETS, root / "intermediate", root / "clean", mapping, min_year_share=0.5)
    perf = pl.read_parquet(root / "clean" / "performance" / "performance_2015.parquet")
    assert perf.columns == [
        "year", "rbd", "cod_ense", "cod_grado", "let_cur", "mrun", "prom_gral", "nom_rbd", "sit_fin", "cod_men"]
    assert perf["cod_men"].to_list() == [1, 0, 0]
    assert perf["mrun"].to_list() == [1, 2, 4]
    assert perf["prom_gral"].to_list() == [6.5, 4.1, None]
    assert perf["nom_rbd"][0] == "Escuela Ñuñoa"
    assert perf.schema["year"] == pl.Int32 and perf.schema["sit_fin"] == pl.String

    asis = pl.read_parquet(root / "clean" / "assistance" / "assistance_2015_03.parquet")
    # AGNO and MES are not in the assistance mapping, so they are kept as extras next to year and month.
    assert asis.columns == ["mrun", "cod_ense", "dias_asistidos", "agno", "mes", "year", "month"]
    assert asis["month"].to_list() == [3, 3]

    dictionary.run(DATASETS, root / "intermediate", root / "clean", mapping, root / "dictionary")
    text = (root / "dictionary" / "performance.md").read_text()
    assert "| rbd | Int32 | School id \\| RBD | RBD | 2015 | 100.0 | 100 to 100 |" in text
    assert "| COD_MEN | 2015 | 1 | cod_men |" in text

    sampling.run(DATASETS, root / "clean", root / "sample", {"performance": 1.0, "assistance": 1.0}, seed=42)
    assert pl.read_parquet(root / "sample" / "performance_sample.parquet").height == 3


def test_rerun_skips_existing(project, caplog):
    root, mapping = project
    extraction.run(DATASETS, root / "raw", root / "intermediate", root / "docs")
    target = root / "intermediate" / "performance" / "performance_2015.parquet"
    before = target.stat().st_mtime_ns
    extraction.run(DATASETS, root / "raw", root / "intermediate", root / "docs")
    assert target.stat().st_mtime_ns == before


def test_sample_is_by_student_and_stable():
    df = pl.DataFrame({"mrun": list(range(1, 100_001)) * 2})
    picked = df.filter(sampling.in_sample(0.02, 42))
    students = picked["mrun"].unique()
    assert 1_800 < students.len() < 2_200
    assert picked.height == 2 * students.len()  # every row of a sampled student is kept
    assert picked.equals(df.filter(sampling.in_sample(0.02, 42)))
    smaller = df.filter(sampling.in_sample(0.005, 42))["mrun"].unique()
    assert set(smaller) <= set(students)  # the 0.5% sample is inside the 2% one
