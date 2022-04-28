create
or replace temporary view ELG00004A_multi as
select
    *
from
    ELG00004_multi_step2
where
    keep_flag = 1