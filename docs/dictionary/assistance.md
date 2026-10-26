# Assistance: data dictionary

Made by `code/4_dictionary.py`; do not edit by hand. Descriptions, codes and notes come from
`config/descriptions/assistance.csv`, written from the MINEDUC record layouts in `docs/mineduc/assistance/`.

343,238,468 rows, 11 years.

## Variables

| variable | type | description | original names | years with data | % non-missing | range |
|---|---|---|---|---|---|---|
| year | Int32 | Año del proceso (AGNO) |  | 2011-2019, 2022-2023 | 100.0 |  |
| month | Int32 | Mes escolar (MES_ESCOLAR) | MES_ESCOLAR | 2011-2019, 2022-2023 | 100.0 | 3 to 12 |
| rbd | Int32 | Rol base de datos del establecimiento | RBD | 2011-2019, 2022-2023 | 100.0 | 1 to 42154 |
| dgv_rbd | Int32 | Dígito verificador del RBD | DGV_RBD | 2011-2019, 2022-2023 | 100.0 | 0 to 9 |
| nom_rbd | String | Nombre del establecimiento | NOM_RBD | 2011-2019, 2022-2023 | 100.0 |  |
| cod_reg_rbd | Int32 | Código de la región en que se ubica el establecimiento | COD_REG_RBD | 2011-2019, 2022-2023 | 100.0 | 1 to 16 |
| nom_reg_rbd_a | String | Nombre abreviado de la región en que se ubica el establecimiento | NOM_REG_RBD_A | 2018-2019, 2022-2023 | 31.6 |  |
| cod_pro_rbd | Int32 | Código de la provincia en que se ubica el establecimiento | COD_PRO_RBD | 2014-2019, 2022-2023 | 70.5 | 11 to 163 |
| cod_com_rbd | Int32 | Código oficial (SUBDERE) de la comuna en que se ubica el establecimiento | COD_COM_RBD | 2011-2019, 2022-2023 | 100.0 | 1101 to 16305 |
| nom_com_rbd | String | Nombre de la comuna en que se ubica el establecimiento | NOM_COM_RBD | 2011-2019, 2022-2023 | 100.0 |  |
| cod_deprov_rbd | Int32 | Código del Departamento Provincial de Educación del establecimiento | COD_DEPROV_RBD | 2015-2019, 2022-2023 | 64.1 | 11 to 151 |
| nom_deprov_rbd | String | Nombre del Departamento Provincial de Educación del establecimiento | NOM_DEPROV_RBD | 2015-2019, 2022-2023 | 64.1 |  |
| rural_rbd | Int32 | Área geográfica del establecimiento | RURAL_RBD | 2011-2019, 2022-2023 | 100.0 | 0 to 1 |
| cod_depe | Int32 | Dependencia administrativa | COD_DEPE | 2011-2019, 2022-2023 | 100.0 | 1 to 8 |
| cod_depe2 | Int32 | Dependencia del establecimiento (agrupada) | COD_DEPE2 | 2014-2019, 2022-2023 | 70.5 | 1 to 5 |
| cod_ense | Int32 | Tipo de enseñanza | COD_ENSE | 2011-2019, 2022-2023 | 100.0 | 10 to 910 |
| cod_ense2 | Int32 | Niveles de enseñanza agrupados | COD_ENSE2 | 2016-2019, 2022-2023 | 55.0 | 1 to 7 |
| cod_grado | Int32 | Código de grado | COD_GRADO | 2011-2019, 2022-2023 | 100.0 | 1 to 8 |
| let_cur | String | Letra del curso | LET_CUR | 2011-2019, 2022-2023 | 100.0 |  |
| mrun | Int64 | Identificador único del estudiante (máscara del RUN) | MRUN | 2011-2019, 2022-2023 | 100.0 | 17 to 27592487 |
| gen_alu | Int32 | Sexo del estudiante | GEN_ALU | 2011-2019, 2022-2023 | 100.0 | 0 to 2 |
| fec_nac_alu | Int32 | Fecha de nacimiento del estudiante (AAAAMM) | FEC_NAC_ALU | 2011-2019, 2022-2023 | 98.2 | 190000 to 20140314 |
| cod_com_alu | Int32 | Código oficial de la comuna de residencia (autodeclarado y voluntario) | COD_COM_ALU | 2011-2019, 2022-2023 | 100.0 | 0 to 16305 |
| nom_com_alu | String | Nombre de la comuna de residencia | NOM_COM_ALU | 2011-2019, 2022-2023 | 100.0 |  |
| dias_asistidos | Int32 | Número total de días asistidos en el mes | DIAS_ASISTIDOS | 2011-2019, 2022-2023 | 100.0 | 0 to 31 |
| dias_trabajados | Int32 | Número total de días trabajados en el mes | DIAS_TRABAJADOS | 2011-2019, 2022-2023 | 100.0 | 0 to 28 |
| asis_promedio | Float64 | Proporción de días asistidos respecto de los días trabajados en el mes (0 a 1) | ASIS_PROMEDIO | 2011-2019, 2022-2023 | 100.0 | 0.0 to 2.21428571428571 |

## Codes and notes

- **year**: No hay 2020 ni 2021: MINEDUC dejó de elaborar la base por la pandemia (de 2020 solo existe marzo, que no está en nuestros datos).
- **month**: 3 (marzo) a 12 (diciembre). El año escolar chileno tiene 10 meses.
- **rbd**: Identificador del colegio; el mismo que en performance.
- **cod_reg_rbd**: Ver Anexo I.
- **nom_reg_rbd_a**: Ver Anexo I. Desde septiembre de 2018.
- **cod_pro_rbd**: Ver Anexo I. Desde junio de 2014.
- **cod_deprov_rbd**: Desde 2015.
- **nom_deprov_rbd**: Desde 2015.
- **rural_rbd**: 0: Urbana; 1: Rural.
- **cod_depe**: 1: Corporación Municipal; 2: Municipal DAEM; 3: Particular Subvencionado; 4: Particular Pagado; 5: Corporación de Administración Delegada; 6: Servicio Local de Educación. Los datos traen también valores 7 y 8, que ningún esquema de registro documenta.
- **cod_depe2**: 1: Municipal; 2: Particular Subvencionado; 3: Particular Pagado (o no subvencionado); 4: Corporación de Administración Delegada (DL 3166); 5: Servicio Local de Educación. Desde junio de 2014.
- **cod_ense**: 10: Educación parvularia; 110: Básica; 310: Media HC niños y jóvenes; 410, 510, 610, 710, 810: Media TP niños y jóvenes; 910: Media artística niños y jóvenes; 165, 167 y los que terminan en 63: adultos; 211-299: educación especial. Ver Anexo II. La base limpia conserva parvularia (10) y la enseñanza regular de niños y jóvenes.
- **cod_ense2**: 1: Educación parvularia; 2: Básica niños; 3: Básica adultos; 4: Educación especial; 5: Media HC jóvenes; 6: Media HC adultos; 7: Media TP y artística jóvenes; 8: Media TP y artística adultos. Desde 2016. Ver Anexo III.
- **cod_grado**: Con cod_ense 10: 1-5 = sala cuna a kinder. Con 110: 1-8 = 1° a 8° básico. Con media: 1-4 = 1° a 4° medio. Ver Anexo IV; hay una tabla para 2011-2018 y otra desde 2019.
- **let_cur**: Con rbd, cod_ense y cod_grado identifica el curso.
- **mrun**: Un estudiante puede tener más de una fila en un mes si se cambió de colegio, de curso o de nivel.
- **gen_alu**: 0: Sin información; 1: Hombre; 2: Mujer; 3: No binario. En diciembre de 2023 MINEDUC eliminó 2 registros con género no binario para proteger su identidad.
- **fec_nac_alu**: 190001, 180001: Sin información. Algunos valores vienen como AAAAMMDD (el máximo es 20140314). Las fechas atípicas se deben a errores al ingresar el RUN en el SIGE.
- **cod_com_alu**: 0: Sin información (valor por defecto).
- **dias_asistidos**: Puede ser 0 para retirados que siguen en el sistema. Los datos llegan hasta 31 días, más que el máximo de días trabajados (28).
- **asis_promedio**: Es una proporción, no un porcentaje. Hay valores sobre 1 (hasta 2,2) donde los días asistidos superan a los trabajados: tratarlos como errores.

## Rows per year

| year | rows |
|---|---|
| 2011 | 30,909,602 |
| 2012 | 30,063,167 |
| 2013 | 31,087,255 |
| 2014 | 31,236,822 |
| 2015 | 31,217,620 |
| 2016 | 30,912,902 |
| 2017 | 31,068,721 |
| 2018 | 31,101,194 |
| 2019 | 31,432,230 |
| 2022 | 32,298,219 |
| 2023 | 31,910,736 |

## Columns published by MINEDUC

| published name | years | number of years | clean variable |
|---|---|---|---|
| AGNO | 2011-2019, 2022-2023 | 11 | year |
| ASIS_PROMEDIO | 2011-2019, 2022-2023 | 11 | asis_promedio |
| COD_COM_ALU | 2011-2019, 2022-2023 | 11 | cod_com_alu |
| COD_COM_RBD | 2011-2019, 2022-2023 | 11 | cod_com_rbd |
| COD_DEPE | 2011-2019, 2022-2023 | 11 | cod_depe |
| COD_ENSE | 2011-2019, 2022-2023 | 11 | cod_ense |
| COD_GRADO | 2011-2019, 2022-2023 | 11 | cod_grado |
| COD_REG_RBD | 2011-2019, 2022-2023 | 11 | cod_reg_rbd |
| DGV_RBD | 2011-2019, 2022-2023 | 11 | dgv_rbd |
| DIAS_ASISTIDOS | 2011-2019, 2022-2023 | 11 | dias_asistidos |
| DIAS_TRABAJADOS | 2011-2019, 2022-2023 | 11 | dias_trabajados |
| FEC_NAC_ALU | 2011-2019, 2022-2023 | 11 | fec_nac_alu |
| GEN_ALU | 2011-2019, 2022-2023 | 11 | gen_alu |
| LET_CUR | 2011-2019, 2022-2023 | 11 | let_cur |
| MES_ESCOLAR | 2011-2019, 2022-2023 | 11 | month |
| MRUN | 2011-2019, 2022-2023 | 11 | mrun |
| NOM_COM_ALU | 2011-2019, 2022-2023 | 11 | nom_com_alu |
| NOM_COM_RBD | 2011-2019, 2022-2023 | 11 | nom_com_rbd |
| NOM_RBD | 2011-2019, 2022-2023 | 11 | nom_rbd |
| RBD | 2011-2019, 2022-2023 | 11 | rbd |
| RURAL_RBD | 2011-2019, 2022-2023 | 11 | rural_rbd |
| COD_DEPE2 | 2014-2019, 2022-2023 | 8 | cod_depe2 |
| COD_PRO_RBD | 2014-2019, 2022-2023 | 8 | cod_pro_rbd |
| COD_DEPROV_RBD | 2015-2019, 2022-2023 | 7 | cod_deprov_rbd |
| NOM_DEPROV_RBD | 2015-2019, 2022-2023 | 7 | nom_deprov_rbd |
| COD_ENSE2 | 2016-2019, 2022-2023 | 6 | cod_ense2 |
| NOM_REG_RBD_A | 2018-2019, 2022-2023 | 4 | nom_reg_rbd_a |
| SAL_ESTADO | 2013 | 1 | not kept |
