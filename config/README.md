Put `data_mapping.xlsx` here and commit it: one sheet per dataset (`kinder`, `assistance`,
`performance`), a `desired_names` column, and one column per period (`2015`, or `2015_03`
for assistance) with that period's original column name. The pipeline cannot unify the
data without it.
An optional `description` column fills the descriptions of the data dictionary
(`docs/dictionary/`); the PDFs extracted to `data/docs/` are the source for them.
