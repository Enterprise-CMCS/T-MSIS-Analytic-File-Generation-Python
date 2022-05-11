create
or replace temporary view ELG00010_multi as
select
    *
from
    ELG00010_multi_step2
where
    keep_flag = 1