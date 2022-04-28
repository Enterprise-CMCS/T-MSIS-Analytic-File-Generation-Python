create
or replace temporary view ELG00005_multi as
select
    *
from
    ELG00005_multi_step2
where
    keep_flag = 1