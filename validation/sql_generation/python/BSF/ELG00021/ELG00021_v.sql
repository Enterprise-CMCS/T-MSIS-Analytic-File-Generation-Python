create
or replace temporary view ELG00021_v as
select
    distinct e21.submtg_state_cd,
    e21.msis_ident_num,
    e21.tmsis_run_id,
    e21.enrlmt_type_cd,
    greatest(
        case
            when (e02.death_date <= to_date(ENRLMT_EFCTV_DT))
            or (
                e02.death_date is not null
                and to_date(ENRLMT_EFCTV_DT) is null
            ) then e02.death_date
            else to_date('ENRLMT_EFCTV_DT')
        end,
        to_date('2021-11-01')
    ) as ENRLMT_EFCTV_DT,
    least(
        case
            when (e02.death_date <= to_date(ENRLMT_END_DT))
            or (
                e02.death_date is not null
                and ENRLMT_END_DT is null
            ) then e02.death_date
            when ENRLMT_END_DT is null then to_date('9999-12-31')
            else ENRLMT_END_DT
        end,
        to_date('2021-11-30')
    ) as ENRLMT_END_DT
from
    ELG00021_step3 e21
    left join ELG00002_202111_uniq e02 on e21.submtg_state_cd = e02.submtg_state_cd
    and e21.msis_ident_num = e02.msis_ident_num
where
    e21.msis_ident_num is not null -- Filter out records where the death_date is before the start of the month
    -- This is separate from process enrollment because it includes UNK enrollment types
    and (
        least(
            to_date('2021-11-01'),
            nvl(
                death_date,
                nvl(to_date(ENRLMT_END_DT), to_date('9999-12-31'))
            )
        ) >= to_date('2021-11-01')
    ) -- Also remove any records where the effective date is after their death date
    and (
        ENRLMT_EFCTV_DT <= least(death_date, to_date('9999-12-31'))
    )