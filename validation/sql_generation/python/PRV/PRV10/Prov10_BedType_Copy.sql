create
or replace temporary view Prov10_BedType_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(trim(upper(prov_location_id)), '') as prov_location_id,
    bed_count,
    bed_type_code,
    bed_type_eff_date,
    bed_type_end_date,
    SPCL
from
    Prov10_BedType_Latest1
where
    tms_is_active = 1
    and (
        (
            bed_type_code is not null
            and bed_type_code <> '8'
        )
        or bed_count is not null
    )
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
