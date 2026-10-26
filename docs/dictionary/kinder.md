# Kinder: data dictionary

Made by `code/extraction/4_dictionary.py`; do not edit by hand. Descriptions, codes and notes come from
`config/descriptions/kinder.csv`, written from the MINEDUC record layouts in `docs/mineduc/kinder/`.

9,879,622 rows, 13 years.

## Variables

| variable | type | description | original names | years with data | % non-missing | range |
|---|---|---|---|---|---|---|
| year | Int32 | Año del proceso (AGNO) |  | 2011-2023 | 100.0 |  |
| month | Int32 | Mes del proceso (MES) | MES | 2013-2023 | 85.8 | 8 to 8 |
| mrun | Int64 | Máscara del RUN del alumno | MRUN | 2011-2023 | 100.0 | 31 to 27575951 |
| sex_stud | Int32 | Sexo del alumno (SEXO_ALU, desde 2019 GEN_ALU) | GEN_ALU, SEXO_ALU | 2011-2023 | 100.0 | 0 to 2 |
| birth_date_stud | Int32 | Fecha de nacimiento del alumno (AAAAMM) | FEC_NAC_ALU | 2011-2023 | 100.0 | 190001 to 202308 |
| school_id | Int64 | Identificador del establecimiento (ID_ESTAB) | ID_ESTAB | 2011-2023 | 100.0 | 2 to 16305010 |
| rbd | Int32 | Rol base de datos del establecimiento (MINEDUC) | RBD | 2014-2023 | 52.0 | 2 to 42178 |
| id_estab_j | Int64 | Identificador del establecimiento JUNJI | ID_ESTAB_J | 2014-2023 | 18.1 | 1101001 to 16305010 |
| id_estab_i | Int64 | Identificador del establecimiento INTEGRA | ID_ESTAB_I | 2014-2023 | 8.2 | 10501 to 172102 |
| nom_estab | String | Nombre del establecimiento | NOM_ESTAB | 2011-2023 | 100.0 |  |
| cod_reg_estab | Int32 | Código de la región del establecimiento | COD_REG_ESTAB | 2011-2023 | 100.0 | 1 to 16 |
| cod_pro_estab | Int32 | Código de la provincia del establecimiento | COD_PRO_ESTAB | 2011-2023 | 100.0 | 1 to 163 |
| cod_com_estab | Int32 | Código oficial (SUBDERE) de la comuna del establecimiento | COD_COM_ESTAB | 2011-2023 | 100.0 | 1101 to 16305 |
| nom_reg_estab | String | Nombre de la región del establecimiento | NOM_REG_ESTAB | 2011-2023 | 100.0 |  |
| nom_reg_a_estab | String | Nombre abreviado de la región del establecimiento | NOM_REG_A_ESTAB | 2019-2023 | 38.6 |  |
| nom_pro_estab | String | Nombre de la provincia del establecimiento | NOM_PRO_ESTAB | 2011-2023 | 100.0 |  |
| nom_com_estab | String | Nombre de la comuna del establecimiento | NOM_COM_ESTAB | 2011-2023 | 100.0 |  |
| cod_deprov_estab | Int32 | Código del Departamento Provincial de Educación del establecimiento | COD_DEPROV_ESTAB | 2015-2023 | 60.4 | 0 to 151 |
| nom_deprov_estab | String | Nombre del Departamento Provincial de Educación del establecimiento | NOM_DEPROV_ESTAB | 2015-2023 | 67.7 |  |
| latitude | Float64 | Latitud del establecimiento (LATITUD) | LATITUD | 2016-2023 | 61.7 | -3346156999.0 to 0.0 |
| longitude | Float64 | Longitud del establecimiento (LONGITUD) | LONGITUD | 2016-2023 | 61.7 | -7081698299.0 to 0.0 |
| rural_establishment | Int32 | Área geográfica del establecimiento (RURAL_ESTAB) | RURAL_ESTAB | 2011-2023 | 98.3 | 0 to 1 |
| origen | Int32 | Origen del dato: institución que reporta al niño | ORIGEN | 2011-2023 | 100.0 | 1 to 3 |
| dependencia | Int32 | Dependencia administrativa del establecimiento | DEPENDENCIA | 2011-2023 | 100.0 | 1 to 6 |
| nivel1 | Int32 | Nivel del alumno, en 6 niveles | NIVEL1 | 2011-2023 | 100.0 | 1 to 6 |
| nivel2 | Int32 | Nivel del alumno, en 3 niveles | NIVEL2 | 2011-2023 | 100.0 | 1 to 3 |
| cod_ense1_m | Int32 | Código de nivel de enseñanza (MINEDUC) | COD_ENSE1_M | 2011-2023 | 66.3 | 10 to 299 |
| cod_grado_m | Int32 | Grado en que está matriculado el alumno (MINEDUC) | COD_GRADO_M | 2011-2023 | 66.3 | 1 to 25 |
| let_cur_m | String | Letra del curso del alumno (MINEDUC) | LET_CUR_M | 2012-2023 | 90.1 |  |
| cod_tip_cur_m | Int32 | Índice de tipo de curso (MINEDUC) | COD_TIP_CUR_M | 2019-2023 | 25.6 | 0 to 2 |
| cod_depe1_m | Int32 | Código de dependencia (MINEDUC) | COD_DEPE1_M | 2011-2023 | 66.3 | 1 to 6 |
| cod_ense2_m | Int32 | Recodificación del código de enseñanza (MINEDUC) | COD_ENSE2_M | 2013-2023 | 57.0 | 1 to 4 |
| estado_estab_m | Int32 | Estado de funcionamiento del establecimiento (MINEDUC) | ESTADO_ESTAB_M | 2015-2023 | 46.8 | 1 to 3 |
| corr_gru_j | Int64 | Grupo JUNJI | CORR_GRU_J | 2012-2023 | 21.7 | 0 to 20 |
| cod_prog_j | Int32 | Código de programa JUNJI | COD_PROG_J | 2012-2023 | 21.7 | 1 to 6 |
| desc_prog_j | String | Descripción del programa JUNJI | DESC_PROG_J | 2011-2023 | 93.7 |  |
| cod_nivel_j | Int32 | Código de nivel JUNJI | COD_NIVEL_J | 2011-2023 | 23.4 | 0 to 11 |
| desc_nivel_j | String | Descripción del nivel JUNJI | DESC_NIVEL_J | 2012-2023 | 86.7 |  |
| cod_modal_j | Int32 | Código de modalidad JUNJI | COD_MODAL_J | 2011-2023 | 23.4 | 1 to 94 |
| desc_modal_j | String | Nombre de la modalidad JUNJI | DESC_MODAL_J | 2011-2023 | 93.7 |  |
| cod_jor_j | Int32 | Código de jornada (JUNJI) | COD_JOR_J | 2012-2023 | 20.9 | 1 to 6 |
| nom_jor_j | String | Descripción de la jornada (JUNJI) | NOM_JOR_J | 2012-2023 | 86.7 |  |
| desc_mod_i | String | Modalidad del establecimiento INTEGRA | DESC_MOD_I | 2011-2023 | 92.6 |  |
| desc_niv_i | String | Descripción del nivel INTEGRA | DESC_NIV_I | 2012-2023 | 85.6 |  |
| cod_nivel_i | Int32 | Código de nivel INTEGRA | COD_NIVEL_I | 2011-2023 | 10.3 | 1 to 10 |
| cod_grupo_i | Int64 | Código de grupo INTEGRA | COD_GRUPO_I | 2011-2023 | 10.3 | 1 to 6 |
| tipo_sostenedor | Int32 | Código de sostenedor | TIPO_SOSTENEDOR | 2021-2023 | 3.0 | 1 to 3 |
| formal | Int32 | Indicador de educación formal en modalidades y programas | FORMAL | 2020-2023 | 30.4 | 0 to 1 |
| asis_real_j | Float64 | Asistencia real (JUNJI) | ASIS_REAL_J | 2011-2022 | 21.4 | 0.0 to 23.0 |
| asis_poten_j | Float64 | Asistencia potencial (JUNJI) | ASIS_POTEN_J | 2011-2022 | 21.7 | 0.0 to 40.0 |
| dias_trab_grupo_j | Float64 | Días trabajados por el grupo JUNJI | DIAS_TRAB_GRUPO_J | 2011-2022 | 20.9 | 0.0 to 30.0 |
| let_gru_j | String | Letra del grupo JUNJI | LET_GRU_J | 2012-2022 | 79.3 |  |

## Codes and notes

- **month**: Desde 2013. La información es al 30 de abril de cada año, descargada en agosto (2011: noviembre).
- **mrun**: Identificador del estudiante; el mismo que en assistance y performance.
- **sex_stud**: 0: Sin información; 1: Hombre; 2: Mujer.
- **birth_date_stud**: 190001: Sin información. Publicada como AAAAMM todos los años.
- **school_id**: Un establecimiento se identifica con school_id y origen juntos.
- **rbd**: Desde 2014. Solo filas MINEDUC.
- **id_estab_j**: Desde 2014. Solo filas JUNJI.
- **id_estab_i**: Desde 2014. Solo filas INTEGRA.
- **nom_reg_a_estab**: Desde 2019.
- **cod_deprov_estab**: 0: No aplica. Desde 2015.
- **nom_deprov_estab**: en blanco: No aplica. Desde 2015.
- **latitude**: Desde 2016. Solo referencial según MINEDUC. Valores como "(EN LAVANTAMIENTO)" o "NO APLICA" quedan vacíos.
- **longitude**: Desde 2016. Solo referencial según MINEDUC.
- **rural_establishment**: 0: Urbana; 1: Rural.
- **origen**: 1: MINEDUC (SIGE); 2: JUNJI; 3: INTEGRA. Cada fila viene de un solo origen. Si un niño aparece en más de uno, MINEDUC deja el registro de MINEDUC, o uno al azar entre los demás.
- **dependencia**: 1: Municipal; 2: Particular Subvencionado; 3: Particular Pagado; 4: JUNJI; 5: INTEGRA; 6: Servicio Local de Educación. 1-3 y 6 corresponden a MINEDUC.
- **nivel1**: 1: Sala cuna menor; 2: Sala cuna mayor; 3: Medio menor; 4: Medio mayor; 5: Transición menor; 6: Transición mayor. Comparable entre los tres orígenes.
- **nivel2**: 1: Sala cuna; 2: Medio; 3: Transición. Comparable entre los tres orígenes.
- **cod_ense1_m**: 10: Educación parvularia; 211-219: Educación especial; 299: Opción 4 Programa de Integración Escolar; .: No aplica. Solo filas MINEDUC; en JUNJI e INTEGRA es "." y queda vacío. El panel de análisis saca las filas MINEDUC distintas de 10 (educación especial).
- **cod_grado_m**: Con cod_ense1_m 10, hasta 2018: 1 Sala cuna; 2 Medio menor; 3 Medio mayor; 4 Pre-kinder; 5 Kinder. Desde 2019: 1 Sala cuna mayor; 2 Medio menor; 3 Medio mayor; 4 Pre-kinder; 5 Kinder; 6 Sala cuna menor; 7-10 Heterogéneos. Los códigos cambiaron en 2019 (ver Anexo II del esquema 2011-2023).
- **let_cur_m**: Desde 2012.
- **cod_tip_cur_m**: 0: Curso simple; 1-2: Curso combinado; 99: Sin información. Desde 2019. Solo se pueden combinar pre-kinder y kinder.
- **cod_depe1_m**: 1: Corp. Municipal; 2: Municipal (DAEM); 3: Particular Subvencionado; 4: Particular Pagado; 6: Servicio Local de Educación.
- **cod_ense2_m**: 1: Educación parvularia; 4: Educación especial. Desde 2013. Solo filas MINEDUC.
- **estado_estab_m**: 1: Funcionando; 2: En receso; 3: Cerrado; 4: Autorizado sin matrícula. Desde 2015. Los niños en establecimientos funcionando son todos los de JUNJI e INTEGRA más los de MINEDUC con estado_estab_m = 1.
- **corr_gru_j**: Desde 2012.
- **cod_prog_j**: 1: Jardín infantil de administración directa; 2: Jardín infantil alternativo; 3: Educativo para la familia; 4: Jardín infantil VTF; 6: Convenio alimentación. Desde 2012.
- **cod_nivel_j**: 0: Sala cuna heterogénea; 1: Sala cuna menor; 2: Sala cuna mayor; 3: Medio menor; 4: Medio mayor; 5: Transición I; 6: Transición II; 7: Sala cuna no convencional; 8: Medio y transición no convencional; 9: Medio y transición convencional; 11: Nivel medio mixto. Así en el esquema 2011-2023; el 11 no existe en versiones anteriores. Para comparar años, usar desc_nivel_j o nivel1/nivel2.
- **desc_nivel_j**: Desde 2012.
- **cod_modal_j**: 1: Jardín infantil; 2: Jardín familiar; 6, 12, 13, 91: Jardín comunicacional; 7: Jardín étnico; 8: Jardín laboral; 10: Jardín comunitario; 92: Conozca a su hijo (CASH); 93: PMI; 94: CECI.
- **cod_jor_j**: 1: Completa; 2: Mañana; 3: Tarde; 4: Extendida; 5: Semipresencial; 6: Vespertina. Desde 2012. Solo filas JUNJI.
- **nom_jor_j**: Desde 2012.
- **desc_niv_i**: Desde 2012.
- **cod_nivel_i**: Esquema 2011-2023: 1: Sala cuna menor; 2: Sala cuna mayor; 3: Medio menor; 4: Medio mayor; 5: Transición menor; 6: Transición mayor; 7: Sala cuna heterogéneo; 8: Medio heterogéneo; 9: Transición heterogéneo; 10: Medio y transición heterogénea. Las versiones anteriores del esquema usan otros códigos (p. ej. 3: Sala cuna, 6: Medios, 9: Transiciones). Para comparar años, usar desc_niv_i o nivel1/nivel2.
- **tipo_sostenedor**: 1: Municipal; 2: Servicio Local; 3: Privado (fundación, ONG, etc.); en blanco: No aplica. Desde 2021. Solo JUNJI VTF e INTEGRA convenio.
- **formal**: 0: No; 1: Sí. Desde 2020.
- **let_gru_j**: Desde 2012.

## Rows per year

| year | rows |
|---|---|
| 2011 | 691,145 |
| 2012 | 715,578 |
| 2013 | 738,656 |
| 2014 | 762,554 |
| 2015 | 776,090 |
| 2016 | 785,417 |
| 2017 | 792,994 |
| 2018 | 800,083 |
| 2019 | 816,814 |
| 2020 | 780,906 |
| 2021 | 745,860 |
| 2022 | 741,645 |
| 2023 | 731,880 |

## Columns published by MINEDUC

| published name | years | number of years | clean variable |
|---|---|---|---|
| AGNO | 2011-2023 | 13 | year |
| COD_COM_ESTAB | 2011-2023 | 13 | cod_com_estab |
| COD_DEPE1_M | 2011-2023 | 13 | cod_depe1_m |
| COD_ENSE1_M | 2011-2023 | 13 | cod_ense1_m |
| COD_GRADO_M | 2011-2023 | 13 | cod_grado_m |
| COD_GRUPO_I | 2011-2023 | 13 | cod_grupo_i |
| COD_MODAL_J | 2011-2023 | 13 | cod_modal_j |
| COD_NIVEL_I | 2011-2023 | 13 | cod_nivel_i |
| COD_NIVEL_J | 2011-2023 | 13 | cod_nivel_j |
| COD_PRO_ESTAB | 2011-2023 | 13 | cod_pro_estab |
| COD_REG_ESTAB | 2011-2023 | 13 | cod_reg_estab |
| DEPENDENCIA | 2011-2023 | 13 | dependencia |
| DESC_MODAL_J | 2011-2023 | 13 | desc_modal_j |
| DESC_MOD_I | 2011-2023 | 13 | desc_mod_i |
| DESC_PROG_J | 2011-2023 | 13 | desc_prog_j |
| FEC_NAC_ALU | 2011-2023 | 13 | birth_date_stud |
| ID_ESTAB | 2011-2023 | 13 | school_id |
| MRUN | 2011-2023 | 13 | mrun |
| NIVEL1 | 2011-2023 | 13 | nivel1 |
| NIVEL2 | 2011-2023 | 13 | nivel2 |
| NOM_COM_ESTAB | 2011-2023 | 13 | nom_com_estab |
| NOM_ESTAB | 2011-2023 | 13 | nom_estab |
| NOM_PRO_ESTAB | 2011-2023 | 13 | nom_pro_estab |
| NOM_REG_ESTAB | 2011-2023 | 13 | nom_reg_estab |
| ORIGEN | 2011-2023 | 13 | origen |
| RURAL_ESTAB | 2011-2023 | 13 | rural_establishment |
| ASIS_POTEN_J | 2011-2022 | 12 | asis_poten_j |
| ASIS_REAL_J | 2011-2022 | 12 | asis_real_j |
| COD_JOR_J | 2012-2023 | 12 | cod_jor_j |
| COD_PROG_J | 2012-2023 | 12 | cod_prog_j |
| CORR_GRU_J | 2012-2023 | 12 | corr_gru_j |
| DESC_NIVEL_J | 2012-2023 | 12 | desc_nivel_j |
| DESC_NIV_I | 2012-2023 | 12 | desc_niv_i |
| DIAS_TRAB_GRUPO_J | 2011-2022 | 12 | dias_trab_grupo_j |
| LET_CUR_M | 2012-2023 | 12 | let_cur_m |
| NOM_JOR_J | 2012-2023 | 12 | nom_jor_j |
| COD_ENSE2_M | 2013-2023 | 11 | cod_ense2_m |
| LET_GRU_J | 2012-2022 | 11 | let_gru_j |
| MES | 2013-2023 | 11 | month |
| ID_ESTAB_I | 2014-2023 | 10 | id_estab_i |
| ID_ESTAB_J | 2014-2023 | 10 | id_estab_j |
| RBD | 2014-2023 | 10 | rbd |
| COD_DEPROV_ESTAB | 2015-2023 | 9 | cod_deprov_estab |
| ESTADO_ESTAB_M | 2015-2023 | 9 | estado_estab_m |
| NOM_DEPROV_ESTAB | 2015-2023 | 9 | nom_deprov_estab |
| LATITUD | 2016-2023 | 8 | latitude |
| LONGITUD | 2016-2023 | 8 | longitude |
| SEXO_ALU | 2011-2017 | 7 | sex_stud |
| GEN_ALU | 2018-2023 | 6 | sex_stud |
| COD_TIP_CUR_M | 2019-2023 | 5 | cod_tip_cur_m |
| NOM_REG_A_ESTAB | 2019-2023 | 5 | nom_reg_a_estab |
| FORMAL | 2020-2023 | 4 | formal |
| TIPO_SOSTENEDOR | 2021-2023 | 3 | tipo_sostenedor |
| ASIS_POT_I | 2011 | 1 | not kept |
| ASIS_REAL_I | 2011 | 1 | not kept |
| COD_INT_ALU_M | 2018 | 1 | not kept |
| COD_NAC_ALU | 2018 | 1 | not kept |
| EDAD_30_06 | 2018 | 1 | not kept |
| INT_ALU_M | 2018 | 1 | not kept |
| POR_ASIS_I | 2011 | 1 | not kept |
| POR_ASIS_J | 2011 | 1 | not kept |
