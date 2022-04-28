create
or replace temporary view ELG00021_CHIP_step4 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            ENRLMT_EFCTV_DT,
            ENRLMT_END_DT
    ) as keeper,
    greatest(
        datediff(
            greatest(to_date('2021-11-01'), ENRLMT_EFCTV_DT),
            least(to_date('2021-11-30'), ENRLMT_END_DT)
        ) + 1,
        0
    ) as NUM_DAYS,
    case
        when (ENRLMT_END_DT >= to_date('2021-11-30')) then 1
        else 0
    end as ELIG_LAST_DAY
from
    ELG00021_CHIP_step3