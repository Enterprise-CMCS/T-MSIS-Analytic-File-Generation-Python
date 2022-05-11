create temp table ELG00012_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keeper) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            WVR_ENRLMT_EFCTV_DT desc,
            WVR_ENRLMT_END_DT desc,
            REC_NUM desc,
            wvr_id,
            wvr_type_code
    ) as keeper
from
    ELG00012_step1
where
    waiver_deduper = 1