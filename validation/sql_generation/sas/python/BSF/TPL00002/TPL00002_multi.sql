create
or replace temporary view TPL00002_multi as
select
    *
from
    TPL00002_multi_step2
where
    keep_flag = 1