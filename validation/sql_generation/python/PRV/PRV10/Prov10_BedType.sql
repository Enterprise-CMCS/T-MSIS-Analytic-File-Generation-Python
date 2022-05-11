create
or replace temporary view Prov10_BedType as
select
    *
from
    TblCopyGrouped
where
    _wanted = 1
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id,
    bed_type_code
