create
or replace temporary view ELG00002_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            PRMRY_DMGRPHC_ELE_EFCTV_DT desc,
            PRMRY_DMGRPHC_ELE_END_DT desc,
            REC_NUM desc,
            coalesce(gndr_cd, 'xx') || coalesce(cast(birth_dt as char(10)), 'xx') || coalesce(cast(death_dt as char(10)), 'xx')
    ) as KEEP_FLAG
from
    ELG00002_multi_all