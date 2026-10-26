"""Runs both masterfiles on small fake MINEDUC files (everything except scraping) and checks the outputs.

    pip install pytest openpyxl
    pytest
"""

import re
import shutil
import subprocess
import sys
import unicodedata
import zipfile
from pathlib import Path

import openpyxl
import polars as pl
import pytest

REPO = Path(__file__).resolve().parents[1]


def functions():
    namespace = {"re": re, "unicodedata": unicodedata, "pl": pl}
    exec((REPO / "code" / "extraction" / "0_functions.py").read_text(encoding="utf-8"), namespace)
    return namespace


@pytest.mark.parametrize("original, monthly, expected", [
    ("Rendimiento-2015.zip", False, "performance_2015"),
    ("20160202_Rendimiento_2015_PUBL.csv", False, "performance_2015"),
    ("Asistencia-declarada-mensual-Septiembre-2019.rar", True, "performance_2019_09"),
    ("Matrícula-Parvularia-2011.rar", False, "performance_2011"),
    ("Asistencia-2019.rar", True, None),
    ("manual.pdf", False, None),
])
def test_file_name_for(original, monthly, expected):
    assert functions()["file_name_for"]("performance", original, monthly) == expected


@pytest.mark.parametrize("path", sorted((REPO / "config" / "descriptions").glob("*.csv")), ids=lambda p: p.name)
def test_descriptions_files_read(path):
    df = pl.read_csv(path, infer_schema=False)
    assert df.columns == ["variable", "description", "values", "notes"]
    assert df["variable"].is_unique().all() and df["description"].is_not_null().all()


def write_zip(path, members):
    with zipfile.ZipFile(path, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)


PERFORMANCE_HEADER = "AGNO;RBD;COD_ENSE;COD_GRADO;LET_CUR;MRUN;FEC_NAC_ALU;PROM_GRAL;ASISTENCIA;NOM_RBD;SIT_FIN;SIT_FIN_R"
PERFORMANCE_2016 = [            # performance 2015 is a Latin-1 file below; 2016 tests the analysis flow
    "2016;100;110;6;A;1;200501;0,0;20;Escuela Uno;Y;T",   # moved to school 200 during the year...
    "2016;200;110;6;A;1;200501;6,0;75;Escuela Dos;P;P",   # ...and is kept there
    "2016;100;110;6;B;2;200401;0,0;10;Escuela Uno;Y;Y",   # withdrew, enrolled nowhere else
    "2016;100;310;2;A;4;199901;5,5;90;Escuela Uno;P;P",
    "2016;100;110;1;A;7;198001;5,0;90;Escuela Uno;P;P",   # 36 years old in 1° básico: implausible
    "2016;100;110;1;A;8;201001;6,1;95;Escuela Uno;P;P",   # kinder in 2015, 1° básico in 2016
    "2016;100;211;1;A;9;201001;6,0;95;Escuela Uno;P;P",   # special education
]
PERFORMANCE_COLUMNS = ["year", "rbd", "cod_ense", "cod_grado", "let_cur", "mrun", "fec_nac_alu", "prom_gral",
                       "asistencia", "nom_rbd", "sit_fin", "sit_fin_r"]
KINDER_COLUMNS = ["year", "mrun", "origen", "nivel1", "cod_ense1_m", "cod_jor_j", "rbd", "school_id", "dependencia",
                  "rural_establishment", "cod_com_estab", "sex_stud", "birth_date_stud"]


def write_mapping(path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "performance"
    ws.append(["desired_names", 2015, 2016])
    for name in PERFORMANCE_COLUMNS:
        published = {"year": "AGNO", "nom_rbd": "nom_rbd"}.get(name, name.upper())  # nom_rbd: case does not matter
        ws.append([name, None if name.startswith("sit_fin") else published, published])  # no SIT_FIN in 2015
    ws = wb.create_sheet("assistance")
    ws.append(["desired_names", "2015_03"])
    # "ANIO" is not in the file: year must come from the file name
    for row in [["year", "ANIO"], ["month", "MES"], ["mrun", "MRUN"], ["cod_ense", "COD_ENSE"], ["fec_nac_alu", "FEC_NAC_ALU"],
                ["dias_asistidos", "DIAS_ASISTIDOS"], ["dias_trabajados", "DIAS_TRABAJADOS"]]:
        ws.append(row)
    ws = wb.create_sheet("kinder")
    ws.append(["desired_names", 2015])
    for name in KINDER_COLUMNS:
        ws.append([name, {"year": "AGNO", "school_id": "ID_ESTAB", "rural_establishment": "RURAL_ESTAB", "sex_stud": "GEN_ALU",
                           "birth_date_stud": "FEC_NAC_ALU"}.get(name, name.upper())])
    wb.save(path)


@pytest.fixture
def project(tmp_path):
    """A copy of the repo with fake raw data, run without the scraping step."""
    shutil.copytree(REPO / "code", tmp_path / "code")
    master = tmp_path / "code" / "masterfile_extraction.py"
    master.write_text(master.read_text(encoding="utf-8").replace('source("extraction/1_scraping.py")', ""), encoding="utf-8")
    (tmp_path / "config").mkdir()
    write_mapping(tmp_path / "config" / "data_mapping.xlsx")
    (tmp_path / "config" / "descriptions").mkdir()
    (tmp_path / "config" / "descriptions" / "performance.csv").write_text(
        "variable,description,values,notes\n"
        "rbd,School id | RBD,,\n"
        "sit_fin,Final status,P: Promoted; R: Failed,\n",
        encoding="utf-8",
    )

    raw = tmp_path / "data" / "raw"
    for dataset in ("performance", "assistance", "kinder"):
        (raw / dataset).mkdir(parents=True)

    # Latin-1, decimal commas, a column missing from the mapping (COD_MEN), and documentation next to the data
    perf = (
        "AGNO;RBD;COD_ENSE;COD_GRADO;LET_CUR;MRUN;FEC_NAC_ALU;PROM_GRAL;ASISTENCIA;NOM_RBD;COD_MEN\n"
        "2015;100;110;5;A;1;200501;6,5;95;Escuela Ñuñoa;1\n"
        "2015;100;110;5;B;2;200401;4,1;80;Escuela Ñuñoa;0\n"
        "2015;100;211;5;A;3;200501;5,0;90;Escuela Ñuñoa;0\n"  # special education
        "2015;100;310;1;A;4;199901;x;150;Escuela Ñuñoa;0\n"   # unreadable grade, attendance over 100
    ).encode("latin-1")
    write_zip(raw / "performance" / "performance_2015.zip", {
        "Rendimiento 2015/20160101_Rendimiento_2015.csv": perf,
        "Rendimiento 2015/Frecuencia_2015.csv": b"a;b\n1;2\n",
        "Rendimiento 2015/Registro_2015.pdf": b"%PDF-1.4",
    })
    write_zip(raw / "performance" / "performance_2016.zip",
              {"Rendimiento_2016.csv": "\n".join([PERFORMANCE_HEADER, *PERFORMANCE_2016]).encode()})

    # Kinder: one program per row; only MINEDUC rows have cod_ense1_m, only JUNJI rows have cod_jor_j
    kinder = (
        "AGNO;MRUN;ORIGEN;NIVEL1;COD_ENSE1_M;COD_JOR_J;RBD;ID_ESTAB;DEPENDENCIA;RURAL_ESTAB;COD_COM_ESTAB;GEN_ALU;FEC_NAC_ALU\n"
        "2015;8;1;6;10;;100;100;1;0;13101;1;201001\n"   # MINEDUC kinder
        "2015;2;1;6;211;;100;100;1;0;13101;1;200401\n"  # MINEDUC special education (and in school)
        "2015;3;2;6;.;3;;5;4;0;13101;2;201002\n"        # JUNJI kinder, afternoon
        "2015;10;3;5;.;;;6;5;1;13101;2;201101\n"        # INTEGRA pre-kinder
    ).encode()
    write_zip(raw / "kinder" / "kinder_2015.zip", {"Parvularia_2015.csv": kinder})

    # A zip inside a zip, UTF-8 with BOM
    inner = tmp_path / "inner.zip"
    write_zip(inner, {"asistencia_marzo_2015.csv": (
        "\ufeffAGNO;MES;MRUN;COD_ENSE;FEC_NAC_ALU;DIAS_ASISTIDOS;DIAS_TRABAJADOS\n"
        "2015;3;1;110;20050314;18;20\n"
        "2015;3;2;110;200506;20;20\n"
        "2015;3;5;110;1990517;5;20\n"
        "2015;3;6;110;Sep 16 1962;0;20\n"
        "2015;3;8;10;201001;8;20\n"    # kinder child who changed school in March: two records...
        "2015;3;8;10;201001;9;18\n"    # ...so 17 days attended of 20 worked
    ).encode()})
    write_zip(raw / "assistance" / "assistance_2015_03.zip", {"carpeta/asistencia.zip": inner.read_bytes()})

    for masterfile in ("code/masterfile_extraction.py", "code/masterfile_analysis.py"):
        result = subprocess.run([sys.executable, masterfile], cwd=tmp_path, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    return tmp_path


def test_extraction(project):
    assert (project / "data" / "intermediate" / "performance" / "performance_2015.parquet").exists()
    docs = project / "data" / "docs" / "performance" / "performance_2015" / "Rendimiento 2015"
    assert sorted(p.name for p in docs.iterdir()) == ["Frecuencia_2015.csv", "Registro_2015.pdf"]


def test_unification_keeps_every_record(project):
    perf = pl.read_parquet(project / "data" / "clean" / "performance" / "performance_2015.parquet")
    assert perf.columns == [*PERFORMANCE_COLUMNS, "cod_men"]
    assert perf["mrun"].to_list() == [1, 2, 3, 4]  # special education too
    assert perf["prom_gral"].to_list() == [6.5, 4.1, 5.0, None]
    assert perf["nom_rbd"][0] == "Escuela Ñuñoa"
    assert perf.schema["year"] == pl.Int32 and perf.schema["sit_fin"] == pl.String

    asis = pl.read_parquet(project / "data" / "clean" / "assistance" / "assistance_2015_03.parquet")
    assert asis.columns == ["year", "month", "mrun", "cod_ense", "fec_nac_alu", "dias_asistidos", "dias_trabajados",
                            "agno"]  # AGNO is unmapped: an extra
    assert asis["fec_nac_alu"].to_list() == [200503, 200506, 199005, 196209, 201001, 201001]  # every format: AAAAMM
    assert asis["year"].to_list() == [2015] * 6
    assert asis["month"].to_list() == [3] * 6

    kinder = pl.read_parquet(project / "data" / "clean" / "kinder" / "kinder_2015.parquet")
    assert kinder["mrun"].to_list() == [8, 2, 3, 10]


def test_dictionary(project):
    text = (project / "docs" / "dictionary" / "performance.md").read_text(encoding="utf-8")
    assert "| rbd | Int32 | School id \\| RBD | RBD | 2015-2016 | 100.0 | 100 to 200 |" in text
    assert "| sit_fin | String | Final status | SIT_FIN | 2016 | 63.6 |  |" in text
    assert "- **sit_fin**: P: Promoted; R: Failed." in text
    assert "| COD_MEN | 2015 | 1 | cod_men |" in text


def test_sampling(project):
    sample = pl.read_parquet(project / "data" / "sample" / "performance_sample.parquet")
    assert set(sample["mrun"]) <= set(range(1, 11))


def panel(project):
    return pl.read_parquet(project / "data" / "analysis" / "panel" / "panel_*.parquet").sort("year", "mrun")


def test_panel_selection(project):
    p = panel(project)
    rows = p.select("year", "mrun", "grado", "rbd").rows()
    assert rows == [
        (2015, 1, 5, 100), (2015, 2, 5, 100), (2015, 3, 0, None), (2015, 4, 9, 100), (2015, 8, 0, 100),
        (2016, 1, 6, 200), (2016, 4, 10, 100), (2016, 8, 1, 100),
    ]  # no special education (3 in 2015 school, 9), no pre-kinder (10), no withdrawn rows, no 36-year-old (7)
    assert p.filter(pl.col("year") == 2015)["rezago"].to_list() == [0, 1, 0, 2, 0]
    assert p.filter((pl.col("year") == 2015) & (pl.col("mrun") == 4))[["prom_gral", "asistencia"]].row(0) == (None, None)
    withdrawals = pl.read_parquet(project / "data" / "analysis" / "withdrawals" / "withdrawals_2016.parquet")
    assert withdrawals[["mrun", "grado", "rbd"]].rows() == [(2, 6, 100)]


def test_cohorts(project):
    cohorts = pl.read_parquet(project / "data" / "analysis" / "cohorts.parquet")
    kinder = cohorts.filter(pl.col("cohort") == "kinder_8basico").sort("mrun")
    assert kinder[["mrun", "entry_year", "last_grade", "left", "censored"]].rows() == [
        (3, 2015, 0, True, False), (8, 2015, 1, False, True),
    ]
    cohort_panel = pl.read_parquet(project / "data" / "analysis" / "cohort_panel.parquet").sort("year")
    child = cohort_panel.filter(pl.col("mrun") == 8)
    assert child["grado"].to_list() == [0, 1]
    assert child[["asis_dias_asistidos", "asis_dias_trabajados", "asis_meses"]].row(0) == (17, 20, 1)
    assert child["asis_anual"].to_list() == [0.85, None]  # no attendance data for 2016


def test_rerun_keeps_existing_files(project):
    target = project / "data" / "clean" / "performance" / "performance_2015.parquet"
    before = target.stat().st_mtime_ns
    subprocess.run([sys.executable, "code/masterfile_extraction.py"], cwd=project, check=True, capture_output=True)
    assert target.stat().st_mtime_ns == before


def test_sample_is_by_student():
    seed, fraction = 42, 0.02
    number = ((pl.col("mrun").cast(pl.UInt64) + seed) * 2654435761 % 2**32) / 2**32
    df = pl.DataFrame({"mrun": list(range(1, 100_001)) * 2})
    picked = df.filter(number < fraction)
    students = set(picked["mrun"])
    assert 1_800 < len(students) < 2_200
    assert picked.height == 2 * len(students)  # all rows of a sampled student
    assert set(df.filter(number < 0.005)["mrun"]) <= students  # smaller samples are inside bigger ones

# [End-of-file] ----
