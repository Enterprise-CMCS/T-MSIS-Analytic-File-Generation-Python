create table Prov07_Medicaid_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
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
    case
        when length(trim(prov_medicaid_enrollment_status_code)) < 2
        and length(trim(prov_medicaid_enrollment_status_code)) > 0
        and prov_medicaid_enrollment_status_code is not null then lpad(
            trim(upper(prov_medicaid_enrollment_status_code)),
            2,
            '0'
        )
        else nullif(
            trim(upper(prov_medicaid_enrollment_status_code)),
            ''
        )
    end as prov_medicaid_enrollment_status_code,
    state_plan_enrollment,
    prov_enrollment_method,
    case
        when date_cmp(appl_date, '1600-01-01') = -1 then '1599-12-31' :: date
        else appl_date
    end as appl_date,
    case
        when date_cmp(prov_medicaid_eff_date, '1600-01-01') = -1 then '1599-12-31' :: date
        else prov_medicaid_eff_date
    end as prov_medicaid_eff_date,
    case
        when prov_medicaid_end_date is null then '9999-12-31' :: date
        when date_cmp(prov_medicaid_end_date, '1600-01-01') = -1 then '1599-12-31' :: date
        else prov_medicaid_end_date
    end as prov_medicaid_end_date
from
    #Prov07_Medicaid_Latest1 where tms_is_active=1 and 
    (prov_medicaid_enrollment_status_code is not null)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;

;