create temp table ELG00018_multi_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keep_flag) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            SECT_1115A_DEMO_EFCTV_DT desc,
            SECT_1115A_DEMO_END_DT desc,
            REC_NUM desc,
            coalesce(SECT_1115A_DEMO_IND, 'xx')
    ) as KEEP_FLAG
from
    ELG00018_multi_all