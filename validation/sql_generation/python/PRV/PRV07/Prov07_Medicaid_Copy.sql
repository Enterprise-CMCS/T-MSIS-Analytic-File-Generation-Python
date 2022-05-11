create
or replace temporary view Prov07_Medicaid_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    % zero_pad(prov_medicaid_enrollment_status_code, 2),
    state_plan_enrollment,
    prov_enrollment_method,
    case
        when (appl_date < to_date('1600-01-01')) then to_date('1599-12-31')
        else appl_date
    end as appl_date,
    case
        when (prov_medicaid_eff_date < to_date('1600-01-01')) then to_date('1599-12-31')
        else prov_medicaid_eff_date
    end as prov_medicaid_eff_date,
    % set_end_dt(prov_medicaid_end_date) as prov_medicaid_end_date,
    SPCL
from
    Prov07_Medicaid_Latest1
where
    tms_is_active = 1
    and (prov_medicaid_enrollment_status_code is not null)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
