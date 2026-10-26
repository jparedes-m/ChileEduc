# Performance: data dictionary

Made by `code/4_dictionary.py`; do not edit by hand. Descriptions come from the `description` column of
`config/data_mapping.xlsx`.

69,820,859 rows, 22 years.

## Variables

| variable | type | description | original names | years with data | % non-missing | range |
|---|---|---|---|---|---|---|
| year | Int32 |  |  | 2002-2023 | 100.0 |  |
| rbd | Int32 |  | RBD, rbd | 2002-2023 | 100.0 | 1 to 42211 |
| dgv_rbd | Int32 |  | DGV_RBD, dgv_rbd | 2002-2023 | 100.0 | 0 to 9 |
| nom_rbd | String |  | NOM_RBD, nom_rbd | 2002-2023 | 100.0 |  |
| cod_reg_rbd | Int32 |  | COD_REG_RBD, cod_reg_rbd | 2002-2023 | 100.0 | 0 to 16 |
| nom_reg_rbd_a | String |  | NOM_REG_RBD_A | 2018-2023 | 26.9 |  |
| cod_pro_rbd | Int32 |  | COD_PRO_RBD, cod_pro_rbd | 2013-2023 | 48.8 | 11 to 163 |
| cod_com_rbd | Int32 |  | COD_COM_RBD, cod_com_rbd | 2002-2023 | 100.0 | 0 to 16305 |
| nom_com_rbd | String |  | NOM_COM_RBD, nom_com_rbd | 2002-2023 | 100.0 |  |
| cod_deprov_rbd | Int32 |  | COD_DEPROV_RBD, cod_deprov_rbd | 2015-2023 | 40.0 | 11 to 151 |
| nom_deprov_rbd | String |  | NOM_DEPROV_RBD, nom_deprov_rbd | 2015-2023 | 40.0 |  |
| cod_depe | Int32 |  | COD_DEPE, cod_depe | 2002-2023 | 100.0 | 0 to 6 |
| cod_depe2 | Int32 |  | COD_DEPE2, cod_depe2 | 2014-2023 | 44.4 | 1 to 5 |
| rural_rbd | Int32 |  | RURAL_RBD, rural_rbd | 2002-2023 | 100.0 | 0 to 1 |
| estado_estab | Int32 |  | ESTADO_ESTAB, estado_estab | 2015-2023 | 40.0 | 1 to 3 |
| cod_ense | Int32 |  | COD_ENSE, cod_ense | 2002-2023 | 100.0 | 10 to 910 |
| cod_ense2 | Int32 |  | COD_ENSE2, cod_ense2 | 2014-2023 | 44.4 | 2 to 7 |
| cod_grado | Int32 |  | COD_GRADO, cod_grado | 2002-2023 | 100.0 | 1 to 8 |
| let_cur | String |  | LET_CUR, let_cur | 2002-2023 | 100.0 |  |
| cod_jor | Int32 |  | COD_JOR, cod_jor | 2013-2023 | 48.8 | 1 to 4 |
| cod_tip_cur | Int32 |  | COD_TIP_CUR, cod_tip_cur | 2013-2023 | 48.8 | 0 to 6 |
| cod_des_cur | Int32 |  | COD_DES_CUR, cod_des_cur | 2016-2023 | 35.7 | 0 to 3 |
| mrun | Int64 |  | MRUN, mrun | 2002-2023 | 100.0 | 3 to 27620610 |
| gen_alu | Int32 |  | GEN_ALU, gen_alu | 2002-2023 | 100.0 | 0 to 2 |
| fec_nac_alu | String |  | FEC_NAC_ALU, fec_nac_alu | 2002-2023 | 100.0 |  |
| edad_alu | Int32 |  | EDAD_ALU, edad_alu | 2016-2023 | 35.7 | 0 to 97 |
| cod_reg_alu | Int32 |  | COD_REG_ALU, cod_reg_alu | 2013-2023 | 48.8 | 0 to 16 |
| cod_com_alu | Int32 |  | COD_COM_ALU, cod_com_alu | 2002-2023 | 100.0 | 0 to 16305 |
| nom_com_alu | String |  | NOM_COM_ALU, nom_com_alu | 2002-2023 | 100.0 |  |
| cod_rama | Int32 |  | COD_RAMA, cod_rama | 2013-2023 | 48.8 | 0 to 900 |
| cod_sec | Int32 |  | COD_SEC, cod_sec | 2003-2023 | 61.7 | 0 to 930 |
| cod_espe | Int32 |  | COD_ESPE, cod_espe | 2003-2023 | 61.7 | 0 to 93006 |
| prom_gral | Float64 |  | PROM_GRAL, prom_gral | 2002-2023 | 100.0 | 0.0 to 7.0 |
| asistencia | Int32 |  | ASISTENCIA, asistencia | 2002-2023 | 100.0 | 0 to 994 |
| sit_fin | String |  | SIT_FIN, sit_fin | 2002-2023 | 100.0 |  |
| sit_fin_r | String |  | SIT_FIN_R, sit_fin_r | 2002-2023 | 100.0 |  |
| cod_men | Int32 |  | COD_MEN | 2019-2023 | 22.5 | 0 to 72007004 |
| int_alu | String |  | INT_ALU | 2003-2013 | 50.9 |  |

## Rows per year

| year | rows |
|---|---|
| 2002 | 3,263,156 |
| 2003 | 3,384,525 |
| 2004 | 3,364,299 |
| 2005 | 3,360,592 |
| 2006 | 3,323,036 |
| 2007 | 3,270,570 |
| 2008 | 3,210,616 |
| 2009 | 3,163,095 |
| 2010 | 3,156,914 |
| 2011 | 3,141,580 |
| 2012 | 3,120,296 |
| 2013 | 3,070,075 |
| 2014 | 3,040,337 |
| 2015 | 3,045,404 |
| 2016 | 3,035,045 |
| 2017 | 3,056,902 |
| 2018 | 3,100,666 |
| 2019 | 3,139,074 |
| 2020 | 3,017,325 |
| 2021 | 3,070,631 |
| 2022 | 3,251,880 |
| 2023 | 3,234,841 |

## Columns published by MINEDUC

| published name | years | number of years | clean variable |
|---|---|---|---|
| AGNO | 2002-2023 | 22 | year |
| ASISTENCIA | 2002-2023 | 22 | asistencia |
| COD_COM_ALU | 2002-2023 | 22 | cod_com_alu |
| COD_COM_RBD | 2002-2023 | 22 | cod_com_rbd |
| COD_DEPE | 2002-2023 | 22 | cod_depe |
| COD_ENSE | 2002-2023 | 22 | cod_ense |
| COD_GRADO | 2002-2023 | 22 | cod_grado |
| COD_REG_RBD | 2002-2023 | 22 | cod_reg_rbd |
| DGV_RBD | 2002-2023 | 22 | dgv_rbd |
| FEC_NAC_ALU | 2002-2023 | 22 | fec_nac_alu |
| GEN_ALU | 2002-2023 | 22 | gen_alu |
| LET_CUR | 2002-2023 | 22 | let_cur |
| MRUN | 2002-2023 | 22 | mrun |
| NOM_COM_ALU | 2002-2023 | 22 | nom_com_alu |
| NOM_COM_RBD | 2002-2023 | 22 | nom_com_rbd |
| NOM_RBD | 2002-2023 | 22 | nom_rbd |
| PROM_GRAL | 2002-2023 | 22 | prom_gral |
| RBD | 2002-2023 | 22 | rbd |
| RURAL_RBD | 2002-2023 | 22 | rural_rbd |
| SIT_FIN | 2002-2023 | 22 | sit_fin |
| SIT_FIN_R | 2002-2023 | 22 | sit_fin_r |
| COD_ESPE | 2003-2023 | 21 | cod_espe |
| COD_SEC | 2003-2023 | 21 | cod_sec |
| COD_JOR | 2013-2023 | 11 | cod_jor |
| COD_PRO_RBD | 2013-2023 | 11 | cod_pro_rbd |
| COD_RAMA | 2013-2023 | 11 | cod_rama |
| COD_REG_ALU | 2013-2023 | 11 | cod_reg_alu |
| COD_TIP_CUR | 2013-2023 | 11 | cod_tip_cur |
| INT_ALU | 2003-2013 | 11 | int_alu |
| COD_DEPE2 | 2014-2023 | 10 | cod_depe2 |
| COD_ENSE2 | 2014-2023 | 10 | cod_ense2 |
| COD_DEPROV_RBD | 2015-2023 | 9 | cod_deprov_rbd |
| ESTADO_ESTAB | 2015-2023 | 9 | estado_estab |
| NOM_DEPROV_RBD | 2015-2023 | 9 | nom_deprov_rbd |
| COD_DES_CUR | 2016-2023 | 8 | cod_des_cur |
| EDAD_ALU | 2016-2023 | 8 | edad_alu |
| LET_RBD | 2002-2009 | 8 | not kept |
| NUM_RBD | 2002-2009 | 8 | not kept |
| NOM_REG_RBD_A | 2018-2023 | 6 | nom_reg_rbd_a |
| COD_MEN | 2019-2023 | 5 | cod_men |
| GD_ALU | 2013-2015 | 3 | not kept |
| FEC_ING_ALU | 2011 | 1 | not kept |
