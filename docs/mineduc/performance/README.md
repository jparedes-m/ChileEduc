# Performance: MINEDUC documentation

Documents that MINEDUC ships inside the performance archives (`Rendimiento por estudiante`), one copy of
each distinct version. The pipeline also copies them, year by year, to `data/docs/performance/`.

| File | What it is | Archives it came in |
|------|------------|---------------------|
| `ER_Rendimiento_2002-2023.pdf` | Record layout (esquema de registro): every variable, its codes, and the years it exists | 2023 |
| `ER_Rendimiento_2002-2020.pdf` | Older record layout. Its Anexo II lists adult education codes (160, 161, 163, 360-362, 460, 461, ...) that the 2023 version no longer lists | 2002-2020 (19 identical copies) |
| `Recomendaciones_uso_BBDD.pdf` | MINEDUC's advice on using the files (September 2014) | 2002-2020 (19 identical copies) |
| `frecuencias/` | Official frequency tables of each year, to check our counts against | 2013-2023 |

The record layouts of 2021 and 2022 were not kept: they are the 2020 version with one more year in the
tables. Every file removed from this folder is still in the git history.

## Points from the record layouts

- **Rows per year**: our raw counts match the official ones for every year (3,376,045 in 2002 ... 3,584,330 in 2023).
- **2023 adds special education** (`cod_ense` 211-299), which is why 2023 has more rows than other years.
  The analysis panel keeps regular education of children and youth only.
- **2023 is missing 2 records**: MINEDUC removed the only two students with non-binary gender to protect their identity.
- **`prom_gral` is 0,0 for withdrawn students** (and often for adult and special education). Treat 0 as no grade.
- **Functioning schools**: from 2015, `estado_estab = 1` gives the students in schools that are open.
- **Encoding**: files up to 2017 use a local encoding, not UTF-8; extraction converts them.
- **`edad_alu`**: from 2016, ages not validated with the Registro Civil are left empty.
- **`int_alu`** (students with special educational needs, Decreto 170/2010) exists only 2003-2013, and
  **`gd_alu`** (grupo diferencial) only 2013-2015.

The variable descriptions used by the data dictionary are in `config/descriptions/performance.csv`.
