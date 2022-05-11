create or replace temporary view ELG00021_CHIP_step1_overlaps as
select
    t1.*
from
    ELG00021_CHIP_step1 t1
    inner join ELG00021_CHIP_step1 t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t1.dateId <> t2.dateId
where
  (t1.ENRLMT_EFCTV_DT >= t2.ENRLMT_EFCTV_DT)
  and (t1.ENRLMT_END_DT <= t2.ENRLMT_END_DT)