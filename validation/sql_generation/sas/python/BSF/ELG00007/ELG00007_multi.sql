create
or replace temporary view ELG00007_multi as
select
    *
from
    ELG00007_multi_step2
where
    keep_flag = 1