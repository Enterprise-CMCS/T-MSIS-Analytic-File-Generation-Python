create
or replace temporary view Prov04_Licensing_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(trim(upper(prov_location_id)), '') as prov_location_id,
    nullif(trim(upper(license_or_accreditation_number)), '') as license_or_accreditation_number,
    license_type,
    nullif(trim(upper(license_issuing_entity_id)), '') as license_issuing_entity_id,
    prov_license_eff_date,
    prov_license_end_date,
    SPCL
from
    Prov04_Licensing_Latest1
where
    tms_is_active = 1
    and (
        license_type is not null
        and license_or_accreditation_number is not null
    )
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
