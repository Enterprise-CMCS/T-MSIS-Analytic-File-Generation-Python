create temp table ELG00021_MDCD_step4 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
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
            day,
            greatest('01NOV2021', ENRLMT_EFCTV_DT),
            least('30NOV2021', ENRLMT_END_DT)
        ) + 1,
        0
    ) as NUM_DAYS,
case
        when date_cmp(ENRLMT_END_DT, '30NOV2021') in(0, 1) then 1
        else 0
    end as ELIG_LAST_DAY
from
    ELG00021_MDCD_step3