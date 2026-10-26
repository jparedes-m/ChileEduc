# Assistance: MINEDUC documentation

Documents that MINEDUC ships inside the monthly attendance archives (`Asistencia declarada mensual`). Each
month comes with its own record layout, but within a year they only differ in the table of row counts, so
one per year is kept (December's, which has the counts of the whole year). The pipeline also copies every
month's documents to `data/docs/assistance/`.

| File | What it is | Archives it came in |
|------|------------|---------------------|
| `ER_Asistencia_2023.pdf` | Record layout (esquema de registro): every variable, its codes, the years it exists, and the official row count of every month since 2011 | 2023 (one version per month) |
| `ER_Asistencia_2011-2015.pdf` ... `ER_Asistencia_2022.pdf` | Earlier record layouts | each year |
| `Recomendaciones_uso_BBDD.pdf` | MINEDUC's advice on using the files (same as in performance) | 2011-2019 |
| `frecuencias/` | Official frequency tables of each month, to check our counts against | October 2013 onwards |

Every file removed from this folder is still in the git history.

## Points from the record layouts

- **Rows are records, not students**: a student can have more than one row in a month if they changed
  school, course or level. The layout gives both counts for each month.
- **Row counts**: our raw counts match the official ones (checked for March to September 2011).
- **No 2020 or 2021**: MINEDUC stopped producing this data during the pandemic; for 2020 only March exists,
  and it is not in our data.
- **`asis_promedio` is a proportion** (0 to 1) of days attended over days worked in the month.
- **`dias_asistidos` can be 0** for students who left the school but remain in the system.
- **Teaching codes**: `cod_ense` includes parvularia (10) and special education (211-299) here, unlike
  performance before 2023. The analysis panel keeps regular education of children and youth only.
- **Grade codes**: Anexo IV has one table for 2011-2018 and another from 2019.

The variable descriptions used by the data dictionary are in `config/descriptions/assistance.csv`.
