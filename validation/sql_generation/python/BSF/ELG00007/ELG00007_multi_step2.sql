create
or replace temporary view ELG00007_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            HH_SNTRN_PRVDR_EFCTV_DT desc,
            HH_SNTRN_PRVDR_END_DT desc,
            REC_NUM desc,
            coalesce(trim(HH_PRVDR_NUM), 'xx')
    ) as KEEP_FLAG
from
    ELG00007_multi_all