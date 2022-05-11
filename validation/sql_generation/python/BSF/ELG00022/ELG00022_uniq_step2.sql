create
or replace temporary view ELG00022_uniq_step2 as
select
    *
from
    ELG00022_MSIS_XWALK_uniq
union
all
select
    *
from
    ELG00022_MSIS_XWALK_multi