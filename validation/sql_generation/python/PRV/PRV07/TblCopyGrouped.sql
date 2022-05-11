create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_medicaid_enrollment_status_code
        order by
            tms_reporting_period desc,
            prov_medicaid_eff_date desc,
            prov_medicaid_end_date desc,
            record_number desc,
            prov_medicaid_enrollment_status_code asc
    ) as _wanted
from
    Prov07_Medicaid_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_medicaid_enrollment_status_code
