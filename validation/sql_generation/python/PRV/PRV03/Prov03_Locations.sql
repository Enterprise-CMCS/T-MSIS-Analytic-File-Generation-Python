create
or replace temporary view Prov03_Locations as
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
    prov_addr_type,
    prov_location_id
