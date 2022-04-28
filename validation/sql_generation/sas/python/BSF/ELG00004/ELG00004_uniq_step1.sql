create
or replace temporary view ELG00004_uniq_step1 as
select
    *
from
    ELG00004_uniq
union
all
select
    *
from
    ELG00004_multi