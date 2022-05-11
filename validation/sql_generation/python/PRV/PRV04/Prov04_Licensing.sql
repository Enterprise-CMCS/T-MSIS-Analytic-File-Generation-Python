create
or replace temporary view Prov04_Licensing as
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
    license_type,
    license_or_accreditation_number,
    license_issuing_entity_id
