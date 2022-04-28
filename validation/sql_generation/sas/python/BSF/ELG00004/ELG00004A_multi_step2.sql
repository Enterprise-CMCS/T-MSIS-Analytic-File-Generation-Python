create
or replace temporary view ELG00004A_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ELGBL_ADR_EFCTV_DT desc,
            ELGBL_ADR_END_DT desc,
            REC_NUM desc,
            coalesce(trim(elgbl_line_1_adr), 'xx') || coalesce(trim(elgbl_city_name), 'xx') || coalesce(trim(elgbl_cnty_cd), 'xx') || coalesce(trim(elgbl_phne_num), 'xx') || coalesce(trim(elgbl_state_cd), 'xx') || coalesce(trim(elgbl_zip_cd), 'xx')
    ) as KEEP_FLAG
from
    ELG00004A_multi_all
where
    ELGBL_ADR_TYPE_CD in ('06', '6')