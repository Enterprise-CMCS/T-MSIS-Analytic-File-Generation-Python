create temp table ELG00021_v distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    distinct e21.submtg_state_cd,
    e21.msis_ident_num,
    e21.tmsis_run_id,
    e21.enrlmt_type_cd,
    greatest(
        case
            when date_cmp(e02.death_date, ENRLMT_EFCTV_DT) in(-1, 0)
            or (
                e02.death_date is not null
                and ENRLMT_EFCTV_DT is null
            ) then e02.death_date
            else ENRLMT_EFCTV_DT
        end,
        '01NOV2021' :: date
    ) as ENRLMT_EFCTV_DT,
    least(
        case
            when date_cmp(e02.death_date, ENRLMT_END_DT) in(-1, 0)
            or (
                e02.death_date is not null
                and ENRLMT_END_DT is null
            ) then e02.death_date
            when ENRLMT_END_DT is null then '31DEC9999'
            else ENRLMT_END_DT
        end,
        '30NOV2021' :: date
    ) as ENRLMT_END_DT
from
    ELG00021_step3 e21
    left join ELG00002_202111_uniq e02 on e21.submtg_state_cd = e02.submtg_state_cd
    and e21.msis_ident_num = e02.msis_ident_num
where
    e21.msis_ident_num is not null
    and date_cmp(
        least(
            '01NOV2021',
            nvl(death_date, ENRLMT_END_DT, '31DEC9999')
        ),
        '01NOV2021'
    ) in(0, 1)
    and date_cmp(ENRLMT_EFCTV_DT, least(death_date, '31DEC9999')) <> 1