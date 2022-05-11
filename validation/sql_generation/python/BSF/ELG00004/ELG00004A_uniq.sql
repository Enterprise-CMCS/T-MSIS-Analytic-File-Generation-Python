create
or replace temporary view ELG00004A_uniq as
select
    t1.*,
    trim(ELGBL_STATE_CD) || trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
    elgbl_line_1_adr as elgbl_line_1_adr_mail,
    elgbl_line_2_adr as elgbl_line_2_adr_mail,
    elgbl_line_3_adr as elgbl_line_3_adr_mail,
    elgbl_city_name as elgbl_city_name_mail,
    elgbl_zip_cd as elgbl_zip_cd_mail,
    elgbl_cnty_cd as elgbl_cnty_cd_mail,
    elgbl_state_cd as elgbl_state_cd_mail,
    elgbl_phne_num as elgbl_phne_num_mail,
    1 as KEEP_FLAG
from
    ELG00004 t1
    inner join ELG00004A_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1
where
    ELGBL_ADR_TYPE_CD in ('06', '6')