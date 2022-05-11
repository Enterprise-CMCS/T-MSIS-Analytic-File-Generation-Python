create temp table ELG00021_CHIP_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*
from
    ELG00021_CHIP_step1 t1
    left join ELG00021_CHIP_step1_overlaps t2 on t1.dateid = t2.dateid
where
    t2.dateid is null