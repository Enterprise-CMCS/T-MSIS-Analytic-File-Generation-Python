create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id
        order by
            tms_reporting_period desc,
            prov_attributes_eff_date desc,
            prov_attributes_end_date desc,
            record_number desc,
            facility_group_individual_code asc
    ) as _wanted
from
    Prov02_Main_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
