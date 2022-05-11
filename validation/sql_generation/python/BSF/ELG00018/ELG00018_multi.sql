create
or replace temporary view ELG00018_multi as
select
    *
from
    ELG00018_multi_step2
where
    keep_flag = 1