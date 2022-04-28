create
or replace temporary view ELG00004_uniq_step2 as
select
    *
from
    ELG00004A_uniq
union
all
select
    *
from
    ELG00004A_multi