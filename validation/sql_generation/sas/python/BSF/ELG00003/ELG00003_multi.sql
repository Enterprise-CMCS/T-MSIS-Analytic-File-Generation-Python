create
or replace temporary view ELG00003_multi as
select
    *
from
    ELG00003_multi_step2
where
    keep_flag = 1