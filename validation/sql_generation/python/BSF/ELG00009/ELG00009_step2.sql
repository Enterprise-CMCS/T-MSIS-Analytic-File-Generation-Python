create
or replace temporary view ELG00009_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            LCKIN_EFCTV_DT desc,
            LCKIN_END_DT desc,
            REC_NUM desc,
            lckin_prvdr_num,
            lckin_prvdr_type_code
    ) as keeper
from
    ELG00009_step1
where
    lckin_deduper = 1