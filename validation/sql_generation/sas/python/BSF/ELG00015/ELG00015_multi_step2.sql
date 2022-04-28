create
or replace temporary view ELG00015_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ETHNCTY_DCLRTN_EFCTV_DT desc,
            ETHNCTY_DCLRTN_END_DT desc,
            REC_NUM desc,
            coalesce(trim(ethncty_cd), 'xx')
    ) as KEEP_FLAG
from
    ELG00015_multi_all