create
or replace temporary view ELG00015_multi as
select
    *
from
    ELG00015_multi_step2
where
    keep_flag = 1