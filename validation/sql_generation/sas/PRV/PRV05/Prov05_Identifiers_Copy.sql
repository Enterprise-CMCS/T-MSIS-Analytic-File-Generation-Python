create table Prov05_Identifiers_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    submitting_state as submtg_state_cd,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(trim(upper(prov_location_id)), '') as prov_location_id,
    nullif(trim(upper(prov_identifier)), '') as prov_identifier,
    prov_identifier_type,
    nullif(
        trim(upper(prov_identifier_issuing_entity_id)),
        ''
    ) as prov_identifier_issuing_entity_id,
    prov_identifier_eff_date,
    prov_identifier_end_date
from
    #Prov05_Identifiers_Latest1 where tms_is_active=1 and (prov_identifier_type is not null and 
    nullif(trim(upper(prov_identifier)), '') is not null
)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;

;