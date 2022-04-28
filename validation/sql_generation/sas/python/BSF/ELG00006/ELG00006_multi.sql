create
or replace temporary view ELG00006_multi as
select
    *
from
    ELG00006_multi_step2
where
    keep_flag = 1