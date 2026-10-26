`data_mapping.xlsx`: the column names of each year. One sheet per dataset (`kinder`, `assistance`,
`performance`), a `desired_names` column, and one column per period (`2015`, or `2015_03` for assistance)
with that period's original column name. The pipeline cannot unify the data without it.

`descriptions/<dataset>.csv`: what each variable means (`description`), its codes (`values`) and
anything to keep in mind when using it (`notes`). Written by hand from the MINEDUC record layouts in
`docs/mineduc/<dataset>/`; the data dictionary (`docs/dictionary/`) takes them from here.
