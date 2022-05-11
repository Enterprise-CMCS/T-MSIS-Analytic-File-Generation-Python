create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_location_id,
        prov_identifier_type,
        prov_identifier,
        prov_identifier_issuing_entity_id
        order by
            tms_reporting_period desc,
            prov_identifier_eff_date desc,
            prov_identifier_end_date desc,
            record_number desc,
            prov_identifier_type asc
    ) as _wanted
from
    Prov05_Identifiers_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id,
    prov_identifier_type,
    prov_identifier,
    prov_identifier_issuing_entity_id
