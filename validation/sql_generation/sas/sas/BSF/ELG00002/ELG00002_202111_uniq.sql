create temp table ELG00002_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    comb.*,
    coalesce(d.DECEASED_FLG, 0) as DECEASED_FLG,
    d.DEATH_DATE,
    case
        when BIRTH_DT is null then null
        when coalesce(d.DECEASED_FLG, 0) = 1 then floor((d.DEATH_DATE - comb.BIRTH_DT) / 365.25)
        else floor(('30NOV2021' - comb.BIRTH_DT) / 365.25)
    end as AGE_CALC,
    case
        when AGE_CALC > 125 then 125
        when AGE_CALC < -1 then null
        else AGE_CALC
    end as AGE,
    case
        when BIRTH_DT is null
        or AGE < -1 then null
        when AGE between -1
        and 0 then 1
        when AGE between 1
        and 5 then 2
        when AGE between 6
        and 14 then 3
        when AGE between 15
        and 18 then 4
        when AGE between 19
        and 20 then 5
        when AGE between 21
        and 44 then 6
        when AGE between 45
        and 64 then 7
        when AGE between 65
        and 74 then 8
        when AGE between 75
        and 84 then 9
        when AGE between 85
        and 125 then 10
        else null
    end as AGE_GROUP_FLG
from
    (
        select
            *
        from
            ELG00002_uniq
        union
        all
        select
            *
        from
            ELG00002_multi
    ) comb
    left join ELG00002_death d on comb.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD
    and comb.msis_ident_num = d.msis_ident_num
    and d.best_record = 1