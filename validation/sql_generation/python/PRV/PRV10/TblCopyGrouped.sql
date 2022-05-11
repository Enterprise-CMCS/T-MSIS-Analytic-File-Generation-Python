create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_location_id,
        bed_type_code
        order by
            tms_reporting_period desc,
            bed_type_eff_date desc,
            bed_type_end_date desc,
            record_number desc,
            bed_type_code asc
    ) as _wanted
from
    Prov10_BedType_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id,
    bed_type_code
