create
or replace temporary view ELG00022_uniq_step1 as
select
    *
from
    ELG00022_ADDTNL_uniq
union
all
select
    *
from
    ELG00022_ADDTNL_multi