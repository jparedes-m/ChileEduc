# Assistance: data dictionary

Made by `code/4_dictionary.py`; do not edit by hand. Descriptions, codes and notes come from
`config/descriptions/assistance.csv`, written from the MINEDUC record layouts in `docs/mineduc/assistance/`.

343,238,468 rows, 11 years.

## Variables

| variable | type | description | original names | years with data | % non-missing | range |
|---|---|---|---|---|---|---|
| year | Int32 |  |  | 2011-2019, 2022-2023 | 100.0 |  |
| month | Int32 |  | MES_ESCOLAR | 2011-2019, 2022-2023 | 100.0 | 3 to 12 |
| rbd | Int32 |  | RBD | 2011-2019, 2022-2023 | 100.0 | 1 to 42154 |
| dgv_rbd | Int32 |  | DGV_RBD | 2011-2019, 2022-2023 | 100.0 | 0 to 9 |
| nom_rbd | String |  | NOM_RBD | 2011-2019, 2022-2023 | 100.0 |  |
| cod_reg_rbd | Int32 |  | COD_REG_RBD | 2011-2019, 2022-2023 | 100.0 | 1 to 16 |
| nom_reg_rbd_a | String |  | NOM_REG_RBD_A | 2018-2019, 2022-2023 | 31.6 |  |
| cod_pro_rbd | Int32 |  | COD_PRO_RBD | 2014-2019, 2022-2023 | 70.5 | 11 to 163 |
| cod_com_rbd | Int32 |  | COD_COM_RBD | 2011-2019, 2022-2023 | 100.0 | 1101 to 16305 |
| nom_com_rbd | String |  | NOM_COM_RBD | 2011-2019, 2022-2023 | 100.0 |  |
| cod_deprov_rbd | Int32 |  | COD_DEPROV_RBD | 2015-2019, 2022-2023 | 64.1 | 11 to 151 |
| nom_deprov_rbd | String |  | NOM_DEPROV_RBD | 2015-2019, 2022-2023 | 64.1 |  |
| rural_rbd | Int32 |  | RURAL_RBD | 2011-2019, 2022-2023 | 100.0 | 0 to 1 |
| cod_depe | Int32 |  | COD_DEPE | 2011-2019, 2022-2023 | 100.0 | 1 to 8 |
| cod_depe2 | Int32 |  | COD_DEPE2 | 2014-2019, 2022-2023 | 70.5 | 1 to 5 |
| cod_ense | Int32 |  | COD_ENSE | 2011-2019, 2022-2023 | 100.0 | 10 to 910 |
| cod_ense2 | Int32 |  | COD_ENSE2 | 2016-2019, 2022-2023 | 55.0 | 1 to 7 |
| cod_grado | Int32 |  | COD_GRADO | 2011-2019, 2022-2023 | 100.0 | 1 to 8 |
| let_cur | String |  | LET_CUR | 2011-2019, 2022-2023 | 100.0 |  |
| mrun | Int64 |  | MRUN | 2011-2019, 2022-2023 | 100.0 | 17 to 27592487 |
| gen_alu | Int32 |  | GEN_ALU | 2011-2019, 2022-2023 | 100.0 | 0 to 2 |
| fec_nac_alu | Int32 |  | FEC_NAC_ALU | 2011-2019, 2022-2023 | 98.2 | 190000 to 20140314 |
| cod_com_alu | Int32 |  | COD_COM_ALU | 2011-2019, 2022-2023 | 100.0 | 0 to 16305 |
| nom_com_alu | String |  | NOM_COM_ALU | 2011-2019, 2022-2023 | 100.0 |  |
| dias_asistidos | Int32 |  | DIAS_ASISTIDOS | 2011-2019, 2022-2023 | 100.0 | 0 to 31 |
| dias_trabajados | Int32 |  | DIAS_TRABAJADOS | 2011-2019, 2022-2023 | 100.0 | 0 to 28 |
| asis_promedio | Float64 |  | ASIS_PROMEDIO | 2011-2019, 2022-2023 | 100.0 | 0.0 to 2.21428571428571 |

## Codes and notes


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
