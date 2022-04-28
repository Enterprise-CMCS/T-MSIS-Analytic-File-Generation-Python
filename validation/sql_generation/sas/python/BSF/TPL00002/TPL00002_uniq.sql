create
or replace temporary view TPL00002_uniq as
select
    t1.*,
    1 as KEEP_FLAG
from
    TPL00002 t1
    inner join TPL00002_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1