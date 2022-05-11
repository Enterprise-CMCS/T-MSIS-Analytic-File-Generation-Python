create
or replace temporary view Prov05_Identifiers as
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
    prov_identifier_type,
    prov_identifier,
    prov_identifier_issuing_entity_id
