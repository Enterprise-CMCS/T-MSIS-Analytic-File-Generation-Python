create temp table ELG00013_step1 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, ltss_deduper) as
select
    distinct submtg_state_cd,
    msis_ident_num,
    TMSIS_RPTG_PRD,
    LTSS_ELGBLTY_EFCTV_DT,
    LTSS_ELGBLTY_END_DT,
    REC_NUM,
    LTSS_PRVDR_NUM,
    LTSS_LVL_CARE_CD,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num,
        ltss_prvdr_num,
        ltss_lvl_care_cd
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            LTSS_ELGBLTY_EFCTV_DT desc,
            LTSS_ELGBLTY_END_DT desc,
            REC_NUM desc,
            ltss_prvdr_num,
            ltss_lvl_care_cd
    ) as ltss_deduper
from
    (
        select
            *
        from
            ELG00013
        where
            ltss_prvdr_num is not null
            or ltss_lvl_care_cd is not null
    ) t2