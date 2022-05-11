create
or replace temporary view ELG00021_MDCD_step2 as
select
    t1.*
from
    ELG00021_MDCD_step1 t1 --  Join initial date to overlapping dateIDs and remove
    left join ELG00021_MDCD_step1_overlaps t2 on t1.dateid = t2.dateid
where
    t2.dateid is null