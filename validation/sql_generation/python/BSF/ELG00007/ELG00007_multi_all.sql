create
or replace temporary view ELG00007_multi_all as
select
    t1.*
from
    ELG00007 as t1
    inner join ELG00007_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1