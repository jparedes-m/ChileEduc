"""Column types of the clean datasets (names as they come out of the mapping file, lowercased).

Columns of the mapping that are not listed here are kept as text.
"""

import polars as pl

I32, I64, F64, STR = pl.Int32, pl.Int64, pl.Float64, pl.String

_GEO_SCHOOL = {
    "dgv_rbd": I32, "nom_rbd": STR, "cod_reg_rbd": I32, "nom_reg_rbd_a": STR,
    "cod_pro_rbd": I32, "cod_com_rbd": I32, "nom_com_rbd": STR, "cod_deprov_rbd": I32,
    "nom_deprov_rbd": STR, "rural_rbd": I32, "cod_depe": I32, "cod_depe2": I32,
}

SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "kinder": {
        "year": I32, "month": I32, "mrun": I64, "sex_stud": I32, "birth_date_stud": I32,
        "school_id": I64, "rbd": I32, "id_estab_j": I64, "id_estab_i": I64, "nom_estab": STR,
        "cod_reg_estab": I32, "cod_pro_estab": I32, "cod_com_estab": I32, "nom_reg_estab": STR,
        "nom_reg_a_estab": STR, "nom_pro_estab": STR, "nom_com_estab": STR,
        "cod_deprov_estab": I32, "nom_deprov_estab": STR, "latitude": F64, "longitude": F64,
        "rural_establishment": I32, "origen": I32, "dependencia": I32, "nivel1": I32,
        "nivel2": I32, "cod_ense1_m": I32, "cod_grado_m": I32, "let_cur_m": STR,
        "cod_tip_cur_m": I32, "cod_depe1_m": I32, "cod_ense2_m": I32, "estado_estab_m": I32,
        "corr_gru_j": I64, "cod_prog_j": I32, "desc_prog_j": STR, "cod_nivel_j": I32,
        "desc_nivel_j": STR, "cod_modal_j": I32, "desc_modal_j": STR, "cod_jor_j": I32,
        "nom_jor_j": STR, "desc_mod_i": STR, "desc_niv_i": STR, "cod_nivel_i": I32,
        "cod_grupo_i": I64, "tipo_sostenedor": I32, "formal": I32, "asis_real_j": F64,
        "asis_poten_j": F64, "dias_trab_grupo_j": F64,
    },
    "assistance": {
        "year": I32, "month": I32, "rbd": I32, **_GEO_SCHOOL, "cod_ense": I32,
        "cod_ense2": I32, "cod_grado": I32, "let_cur": STR, "mrun": I64, "gen_alu": I32,
        "fec_nac_alu": I32, "cod_com_alu": I32, "nom_com_alu": STR, "dias_asistidos": I32,
        "dias_trabajados": I32, "asis_promedio": F64,
    },
    "performance": {
        "year": I32, "rbd": I32, **_GEO_SCHOOL, "estado_estab": I32, "cod_ense": I32,
        "cod_ense2": I32, "cod_grado": I32, "let_cur": STR, "cod_jor": I32, "cod_tip_cur": I32,
        "cod_des_cur": I32, "mrun": I64, "gen_alu": I32, "fec_nac_alu": STR, "edad_alu": I32,
        "cod_reg_alu": I32, "cod_com_alu": I32, "nom_com_alu": STR, "cod_rama": I32,
        "cod_sec": I32, "cod_espe": I32, "prom_gral": F64, "asistencia": I32, "sit_fin": STR,
        "sit_fin_r": STR, "cod_men": I32,
    },
}
