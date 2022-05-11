create temp table ELG00015_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    case
        when ETHNCTY_CD = '0' then 1
        else null
    end as NONHISPANIC_ETHNICITY_FLG,
    case
        when ETHNCTY_CD in('1', '2', '3', '4', '5') then 1
        when ETHNCTY_CD = '0' then 0
        else null
    end as HISPANIC_ETHNICITY_FLG,
    case
        when ETHNCTY_CD not in('0', '1', '2', '3', '4', '5') then 1
        else 0
    end as UNK_ETHNICITY_FLG,
    1 as KEEP_FLAG
from
    ELG00015 t1
    inner join ELG00015_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1