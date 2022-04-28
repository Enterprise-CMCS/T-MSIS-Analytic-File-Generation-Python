create temp table ELG00018_multi_all distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
case
        when SECT_1115A_DEMO_IND = '1' then 1
        else 0
    end as _1115A_PARTICIPANT_FLG
from
    ELG00018 t1
    inner join ELG00018_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1