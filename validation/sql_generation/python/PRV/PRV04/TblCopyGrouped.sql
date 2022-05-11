create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_location_id,
        license_type,
        license_or_accreditation_number,
        license_issuing_entity_id
        order by
            tms_reporting_period desc,
            prov_license_eff_date desc,
            prov_license_end_date desc,
            record_number desc,
            license_type asc
    ) as _wanted
from
    Prov04_Licensing_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id,
    license_type,
    license_or_accreditation_number,
    license_issuing_entity_id
