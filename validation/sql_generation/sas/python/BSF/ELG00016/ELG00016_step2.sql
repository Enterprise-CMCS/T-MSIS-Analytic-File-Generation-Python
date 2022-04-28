create
or replace temporary view ELG00016_step2 as
select
    *
from
    ELG00016_step1
where
    keeper = 1