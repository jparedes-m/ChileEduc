# Kinder: MINEDUC documentation

Documents that MINEDUC ships inside the kinder archives (`Matrícula Educación Parvularia`), one copy of
each distinct version. The pipeline also copies them, year by year, to `data/docs/kinder/`.

| File | What it is | Archives it came in |
|------|------------|---------------------|
| `ER_Parvularia_2011-2023.pdf` | Record layout (esquema de registro): every variable, its codes, and the years it exists | 2023 |
| `ER_Parvularia_2011-2022.pdf` | Earlier version, with other codes for `COD_NIVEL_I` and without code 11 in `COD_NIVEL_J` | 2019, 2022 |
| `ER_Parvularia_2011-2021.pdf` | Earlier version, same codes as the 2022 one | 2011-2018, 2020, 2021 |
| `tabulaciones/` | Official tabulations of each year, to check our counts against | 2014-2018, 2020-2023 |

Every file removed from this folder is still in the git history.

## Points from the record layouts

- **Three programs in one file**: each row comes from MINEDUC (SIGE), JUNJI or INTEGRA (`origen` 1, 2, 3).
  Variables ending in `_M` only exist for MINEDUC rows, `_J` for JUNJI and `_I` for INTEGRA. A child in more
  than one program appears once: MINEDUC's record wins, otherwise one is picked at random.
- **The analysis panel keeps kinder (NT2) of the three programs** and drops special education from MINEDUC
  (`cod_ense1_m` other than 10). JUNJI and INTEGRA have no special education code.
- **Level codes changed**: `cod_grado_m` changed in 2019, `cod_nivel_i` has other codes in the 2023 layout
  than in earlier ones, and `cod_nivel_j` gained code 11. `nivel1` (6 levels) and `nivel2` (3 levels) are
  comparable across programs and years.
- **Reference date**: the data is as of April 30 of each year, downloaded in August (November in 2011).
- **Row counts**: our raw counts match the official ones except in 2013 (9 fewer rows), 2020 (6,490 fewer)
  and 2021 (8,604 fewer). The data was read in full (one CSV per archive, no broken lines): the archives hold
  an earlier version of the data than the one the record layout counts (the 2020 CSV is dated 2020-12-01 and
  the 2021 one 2021-11-12, while the layout is from 2021-11-29). Worth downloading those years again when the
  MINEDUC site is back.
- **`.` means no information or does not apply** in numeric columns; unification leaves it empty.

The variable descriptions used by the data dictionary are in `config/descriptions/kinder.csv`.
