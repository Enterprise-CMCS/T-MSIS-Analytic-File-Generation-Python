create
or replace temporary view ELG00021_MDCD_step1_overlaps as
select
  t1.*
from
  ELG00021_MDCD_step1 t1
  inner join ELG00021_MDCD_step1 t2 --  Join records for beneficiary to each other, but omit matches where it's the same record
  on t1.submtg_state_cd = t2.submtg_state_cd
  and t1.msis_ident_num = t2.msis_ident_num
  and t1.dateId <> t2.dateId --  Get every dateID where their effective date is greater than or equal to another record's effective date
  --  AND their end date is less than or equal to that other record's end date.
where
  (t1.ENRLMT_EFCTV_DT >= t2.ENRLMT_EFCTV_DT)
  and (t1.ENRLMT_END_DT <= t2.ENRLMT_END_DT)