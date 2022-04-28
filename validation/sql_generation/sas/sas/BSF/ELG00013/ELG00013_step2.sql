create temp table ELG00013_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keeper) as
select
    distinct submtg_state_cd,
    msis_ident_num,
    LTSS_PRVDR_NUM,
    LTSS_LVL_CARE_CD,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            LTSS_ELGBLTY_EFCTV_DT desc,
            LTSS_ELGBLTY_END_DT desc,
            REC_NUM desc,
            ltss_prvdr_num,
            ltss_lvl_care_cd
    ) as keeper
from
    ELG00013_step1
where
    ltss_deduper = 1