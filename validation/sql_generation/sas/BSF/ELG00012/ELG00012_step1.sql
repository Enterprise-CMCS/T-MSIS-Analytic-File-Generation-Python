create temp table ELG00012_step1 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, waiver_deduper) as
select
    distinct submtg_state_cd,
    msis_ident_num,
    rec_num,
    WVR_ENRLMT_EFCTV_DT,
    WVR_ENRLMT_END_DT,
    tmsis_rptg_prd,
    wvr_id,
    lpad(trim(WVR_TYPE_CD), 2, '0') as WVR_TYPE_CODE,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num,
        wvr_id,
        wvr_type_code
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            WVR_ENRLMT_EFCTV_DT desc,
            WVR_ENRLMT_END_DT desc,
            REC_NUM desc,
            wvr_id,
            wvr_type_code
    ) as waiver_deduper
from
    (
        select
            *
        from
            ELG00012
        where
            wvr_id is not null
            or wvr_type_cd is not null
    ) t1