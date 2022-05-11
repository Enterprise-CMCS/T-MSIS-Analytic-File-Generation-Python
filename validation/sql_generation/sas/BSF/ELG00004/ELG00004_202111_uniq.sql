create temp table ELG00004_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    coalesce(t1.msis_ident_num, t2.msis_ident_num) as msis_ident_num,
    coalesce(t1.submtg_state_cd, t2.submtg_state_cd) as submtg_state_cd,
    t1.ENROLLEES_COUNTY_CD_HOME,
    coalesce(t1.elgbl_adr_efctv_dt, t2.elgbl_adr_efctv_dt) as ELGBL_ADR_EFCTV_DT,
    coalesce(t1.elgbl_adr_end_dt, t2.elgbl_adr_end_dt) as ELGBL_ADR_END_DT,
    coalesce(t1.elgbl_adr_type_cd, t2.elgbl_adr_type_cd) as ELGBL_ADR_TYPE_CD,
    t1.elgbl_line_1_adr_home,
    t1.elgbl_line_2_adr_home,
    t1.elgbl_line_3_adr_home,
    t1.elgbl_city_name_home,
    t1.elgbl_zip_cd_home,
    t1.elgbl_cnty_cd_home,
    t1.elgbl_state_cd_home,
    t1.elgbl_phne_num_home,
    t2.elgbl_line_1_adr_mail,
    t2.elgbl_line_2_adr_mail,
    t2.elgbl_line_3_adr_mail,
    t2.elgbl_city_name_mail,
    t2.elgbl_zip_cd_mail,
    t2.elgbl_cnty_cd_mail,
    t2.elgbl_state_cd_mail,
    t2.elgbl_phne_num_mail
from
    ELG00004_uniq_step1 t1 full
    join ELG00004_uniq_step2 t2 on t1.msis_ident_num = t2.msis_ident_num
    and t1.submtg_state_cd = t2.submtg_state_cd