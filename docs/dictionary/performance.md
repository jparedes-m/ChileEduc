# Performance: data dictionary

Made by `code/4_dictionary.py`; do not edit by hand. Descriptions, codes and notes come from
`config/descriptions/performance.csv`, written from the MINEDUC record layouts in `docs/mineduc/performance/`.

69,820,859 rows, 22 years.

## Variables

| variable | type | description | original names | years with data | % non-missing | range |
|---|---|---|---|---|---|---|
| year | Int32 | Año escolar (AGNO) |  | 2002-2023 | 100.0 |  |
| rbd | Int32 | Rol base de datos del establecimiento | RBD | 2002-2023 | 100.0 | 1 to 42211 |
| dgv_rbd | Int32 | Dígito verificador del RBD | DGV_RBD | 2002-2023 | 100.0 | 0 to 9 |
| nom_rbd | String | Nombre del establecimiento | NOM_RBD | 2002-2023 | 100.0 |  |
| cod_reg_rbd | Int32 | Código de la región en que se ubica el establecimiento | COD_REG_RBD | 2002-2023 | 100.0 | 0 to 16 |
| nom_reg_rbd_a | String | Nombre abreviado de la región en que se ubica el establecimiento | NOM_REG_RBD_A | 2018-2023 | 26.9 |  |
| cod_pro_rbd | Int32 | Código oficial de la provincia en que se ubica el establecimiento | COD_PRO_RBD | 2013-2023 | 48.8 | 11 to 163 |
| cod_com_rbd | Int32 | Código oficial (SUBDERE) de la comuna en que se ubica el establecimiento | COD_COM_RBD | 2002-2023 | 100.0 | 0 to 16305 |
| nom_com_rbd | String | Nombre de la comuna en que se ubica el establecimiento | NOM_COM_RBD | 2002-2023 | 100.0 |  |
| cod_deprov_rbd | Int32 | Código del Departamento Provincial de Educación al que pertenece el establecimiento | COD_DEPROV_RBD | 2015-2023 | 40.0 | 11 to 151 |
| nom_deprov_rbd | String | Nombre del Departamento Provincial de Educación al que pertenece el establecimiento | NOM_DEPROV_RBD | 2015-2023 | 40.0 |  |
| cod_depe | Int32 | Dependencia del establecimiento | COD_DEPE | 2002-2023 | 100.0 | 0 to 6 |
| cod_depe2 | Int32 | Dependencia del establecimiento (agrupada) | COD_DEPE2 | 2014-2023 | 44.4 | 1 to 5 |
| rural_rbd | Int32 | Índice de ruralidad del establecimiento | RURAL_RBD | 2002-2023 | 100.0 | 0 to 1 |
| estado_estab | Int32 | Estado del establecimiento | ESTADO_ESTAB | 2015-2023 | 40.0 | 1 to 3 |
| cod_ense | Int32 | Código de enseñanza | COD_ENSE | 2002-2023 | 100.0 | 10 to 910 |
| cod_ense2 | Int32 | Niveles de enseñanza agrupados | COD_ENSE2 | 2014-2023 | 44.4 | 2 to 7 |
| cod_grado | Int32 | Código de grado | COD_GRADO | 2002-2023 | 100.0 | 1 to 8 |
| let_cur | String | Letra del curso | LET_CUR | 2002-2023 | 100.0 |  |
| cod_jor | Int32 | Jornada en la que asiste a clases | COD_JOR | 2013-2023 | 48.8 | 1 to 4 |
| cod_tip_cur | Int32 | Índice de tipo de curso | COD_TIP_CUR | 2013-2023 | 48.8 | 0 to 6 |
| cod_des_cur | Int32 | Descripción del curso (solo media TP) | COD_DES_CUR | 2016-2023 | 35.7 | 0 to 3 |
| mrun | Int64 | Máscara del RUN del estudiante | MRUN | 2002-2023 | 100.0 | 3 to 27620610 |
| gen_alu | Int32 | Género del estudiante | GEN_ALU | 2002-2023 | 100.0 | 0 to 2 |
| fec_nac_alu | String | Fecha de nacimiento del estudiante (AAAAMM) | FEC_NAC_ALU | 2002-2023 | 100.0 |  |
| edad_alu | Int32 | Edad al 30 de junio del año escolar | EDAD_ALU | 2016-2023 | 35.7 | 0 to 97 |
| cod_reg_alu | Int32 | Código de la región de residencia del estudiante | COD_REG_ALU | 2013-2023 | 48.8 | 0 to 16 |
| cod_com_alu | Int32 | Código oficial (SUBDERE) de la comuna de residencia del estudiante (autodeclarado y voluntario) | COD_COM_ALU | 2002-2023 | 100.0 | 0 to 16305 |
| nom_com_alu | String | Nombre de la comuna de residencia del estudiante | NOM_COM_ALU | 2002-2023 | 100.0 |  |
| cod_rama | Int32 | Código de rama (solo media TP y artística) | COD_RAMA | 2013-2023 | 48.8 | 0 to 900 |
| cod_sec | Int32 | Código de sector económico (solo media TP y artística) | COD_SEC | 2003-2023 | 61.7 | 0 to 930 |
| cod_espe | Int32 | Código de especialidad (solo media TP y artística) | COD_ESPE | 2003-2023 | 61.7 | 0 to 93006 |
| prom_gral | Float64 | Promedio general anual | PROM_GRAL | 2002-2023 | 100.0 | 0.0 to 7.0 |
| asistencia | Int32 | Porcentaje anual de asistencia | ASISTENCIA | 2002-2023 | 100.0 | 0 to 994 |
| sit_fin | String | Situación de promoción al cierre del año escolar | SIT_FIN | 2002-2023 | 100.0 |  |
| sit_fin_r | String | Situación de promoción al cierre del año escolar, con indicador de traslado | SIT_FIN_R | 2002-2023 | 100.0 |  |
| cod_men | Int32 | Mención (media TP) | COD_MEN | 2019-2023 | 22.5 | 0 to 72007004 |
| int_alu | Int32 | Indicador de alumno integrado: estudiante con necesidades educativas especiales permanentes o transitorias (Decreto 170/2010) | INT_ALU | 2003-2013 | 50.9 | 0 to 1 |

## Codes and notes

- **rbd**: Identificador del colegio.
- **cod_reg_rbd**: Ver Anexo I. Ñuble (16) existe desde septiembre de 2018.
- **nom_reg_rbd_a**: Ver Anexo I.
- **cod_pro_rbd**: Ver Anexo I.
- **cod_depe**: 1: Corporación Municipal; 2: Municipal DAEM; 3: Particular Subvencionado; 4: Particular Pagado (o no subvencionado); 5: Corporación de Administración Delegada (DL 3166); 6: Servicio Local de Educación.
- **cod_depe2**: 1: Municipal; 2: Particular Subvencionado; 3: Particular Pagado (o no subvencionado); 4: Corporación de Administración Delegada (DL 3166); 5: Servicio Local de Educación.
- **rural_rbd**: 0: Urbano; 1: Rural.
- **estado_estab**: 1: Funcionando; 2: En receso; 3: Cerrado; 4: Autorizado sin matrícula. Desde 2015, ESTADO_ESTAB = 1 da los estudiantes en establecimientos funcionando.
- **cod_ense**: 110: Básica; 310: Media HC niños y jóvenes; 410, 510, 610, 710, 810: Media TP niños y jóvenes (comercial, industrial, técnica, agrícola, marítima); 910: Media artística niños y jóvenes; 160-167 y los que terminan en 60-63 (360, 463, ...): adultos; 211-299: educación especial. Ver Anexo II. La base limpia conserva solo enseñanza regular de niños y jóvenes. Educación especial aparece en la base desde 2023. En 2006 hay 41 filas con cod_ense 10 (parvularia), código que no figura en el esquema de esta base.
- **cod_ense2**: 2: Básica niños; 3: Básica adultos; 4: Educación especial; 5: Media HC jóvenes; 6: Media HC adultos; 7: Media TP y artística jóvenes; 8: Media TP y artística adultos. Ver Anexo III.
- **cod_grado**: Con cod_ense 110: 1-8 = 1° a 8° básico. Con media niños y jóvenes: 1-4 = 1° a 4° medio. Ver Anexo IV; el significado depende de cod_ense.
- **let_cur**: Con rbd, cod_ense y cod_grado identifica el curso.
- **cod_jor**: 1: Mañana; 2: Tarde; 3: Mañana y tarde; 4: Vespertina / Nocturna.
- **cod_tip_cur**: 0: Curso simple; 1-6: Curso combinado. En básica niños se pueden combinar 1° a 6° y/o 7° y 8°.
- **cod_des_cur**: 0: No aplica; 1: Sólo liceo; 2: Dual; 3: Otro.
- **mrun**: Identificador del estudiante.
- **gen_alu**: 0: Sin información; 1: Masculino; 2: Femenino. En 2023 MINEDUC eliminó 2 registros con género no binario para proteger su identidad.
- **edad_alu**: Desde 2016, las edades no validadas con el Registro Civil (99 o más; 25 o más, o 5 o menos en básica y media niños y jóvenes) están vacías.
- **cod_reg_alu**: 0: Sin información.
- **cod_com_alu**: 0: Sin información.
- **cod_rama**: 0: Ciclo general / Sin información; 400: Comercial; 500: Industrial; 600: Técnica; 700: Agrícola; 800: Marítima; 900: Artística. Ver Anexo V. Ciclo general = 1° y 2° medio.
- **cod_sec**: Ver Anexo VI.
- **cod_espe**: Ver Anexo VI.
- **prom_gral**: No es necesariamente el promedio aritmético de los subsectores. Los retirados tienen 0,0: tratar 0 como sin nota. Entre 3% y 8% de las filas tiene 0 cada año (más bajo en 2020-2021).
- **asistencia**: En 2009 hay 23 valores sobre 100 (máximo 994), probablemente errores de digitación: tratarlos como faltantes.
- **sit_fin**: P: Promovido; R: Reprobado; Y: Retirado; vacío: Sin información.
- **sit_fin_r**: P: Promovido; R: Reprobado; Y: Retirado; T: Trasladado; vacío: Sin información. Trasladado: se cambió de establecimiento, o de curso en el mismo establecimiento, durante el año.
- **cod_men**: Ver Anexo X. Código de 8 dígitos: especialidad + mención.
- **int_alu**: 0: No; 1: Sí; .: Sin información.

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
