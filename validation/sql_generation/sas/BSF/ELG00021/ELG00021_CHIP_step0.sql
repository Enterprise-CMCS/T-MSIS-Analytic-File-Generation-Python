create temp table ELG00021_CHIP_step0 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00021_v
where
    enrlmt_type_cd = '2'