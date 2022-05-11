create
or replace temporary view Prov07_Medicaid as
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
    prov_medicaid_enrollment_status_code
