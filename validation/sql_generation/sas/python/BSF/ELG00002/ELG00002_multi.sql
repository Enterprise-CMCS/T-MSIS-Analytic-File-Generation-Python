create
or replace temporary view ELG00002_multi as
select
    *
from
    ELG00002_multi_step2
where
    keep_flag = 1