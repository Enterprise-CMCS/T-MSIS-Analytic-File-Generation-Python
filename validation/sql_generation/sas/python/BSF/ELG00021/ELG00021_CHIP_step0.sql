create
or replace temporary view ELG00021_CHIP_step0 as
select
    *
from
    ELG00021_v
where
    enrlmt_type_cd = '2'