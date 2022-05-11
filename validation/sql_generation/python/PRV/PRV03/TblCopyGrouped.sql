create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_addr_type,
        prov_location_id
        order by
            tms_reporting_period desc,
            prov_location_and_contact_info_eff_date desc,
            prov_location_and_contact_info_end_date desc,
            record_number desc,
            prov_location_id asc
    ) as _wanted
from
    Prov03_Locations_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_addr_type,
    prov_location_id
