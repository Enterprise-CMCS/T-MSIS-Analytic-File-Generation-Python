create
or replace temporary view ELG00022_MSIS_XWALK_multi as
select
    *
from
    ELG00022_multi_step2
where
    keep_flag = 1