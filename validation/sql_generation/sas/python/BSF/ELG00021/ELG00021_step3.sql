create
or replace temporary view ELG00021_step3 as
select
    t1.*
from
    ELG00021_step1 t1
    left join ELG00021_step2 t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t1.order_flag = t2.order_flag
where
    t2.msis_ident_num is null