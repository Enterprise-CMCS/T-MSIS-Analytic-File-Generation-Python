create temp table ELG00004_multi_all distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    trim(ELGBL_STATE_CD) || trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
    elgbl_line_1_adr as elgbl_line_1_adr_home,
    elgbl_line_2_adr as elgbl_line_2_adr_home,
    elgbl_line_3_adr as elgbl_line_3_adr_home,
    elgbl_city_name as elgbl_city_name_home,
    elgbl_zip_cd as elgbl_zip_cd_home,
    elgbl_cnty_cd as elgbl_cnty_cd_home,
    lpad(elgbl_state_cd, 2, '0') as elgbl_state_cd_home,
    elgbl_phne_num as elgbl_phne_num_home
from
    ELG00004 t1
    inner join ELG00004_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1
where
    ELGBL_ADR_TYPE_CD in ('01', '1')