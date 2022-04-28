create temp table ELG00021_CHIP_step1_overlaps distkey(dateid) sortkey(dateid) as
select
    t1.*
from
    ELG00021_CHIP_step1 t1
    inner join ELG00021_CHIP_step1 t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t1.dateId <> t2.dateId
where
    date_cmp(t1.ENRLMT_EFCTV_DT, t2.ENRLMT_EFCTV_DT) in(0, 1)
    and date_cmp(t1.ENRLMT_END_DT, t2.ENRLMT_END_DT) in (-1, 0)