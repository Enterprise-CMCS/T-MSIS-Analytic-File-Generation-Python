create temp table ELG00006_multi_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keep_flag) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            HH_SNTRN_PRTCPTN_EFCTV_DT desc,
            HH_SNTRN_PRTCPTN_END_DT desc,
            REC_NUM desc,
            coalesce(trim(HH_ENT_NAME), 'xx')
    ) as KEEP_FLAG
from
    ELG00006_multi_all