create
or replace temporary view ELG00002_death as
select
    submtg_state_cd,
    msis_ident_num,
    case
        when to_date(DEATH_DT, 'yyyyMM') > to_date('202111') then null
        else DEATH_DT
    end as DEATH_DATE,
    case
        when DEATH_DT is not null
        and DEATH_DT <= to_date('2021-11-30') then 1
        else 0
    end as DECEASED_FLG,
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
            DEATH_DT desc
    ) as best_record
from
    ELG00002A