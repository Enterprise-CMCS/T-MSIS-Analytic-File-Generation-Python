create
or replace temporary view Prov02_Main_Copy as
select
    tms_run_id,
    tms_reporting_period,
    submitting_state,
    record_number,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    prov_attributes_eff_date,
    prov_attributes_end_date,
    prov_doing_business_as_name,
    prov_legal_name,
    prov_organization_name,
    prov_tax_name,
    facility_group_individual_code,
    teaching_ind,
    prov_first_name,
    prov_middle_initial,
    prov_last_name,
    sex,
    ownership_code,
    prov_profit_status,
    case
        when (date_of_birth < to_date('1600-01-01')) then to_date('1599-12-31')
        else date_of_birth
    end as date_of_birth,
    case
        when (date_of_death < to_date('1600-01-01')) then to_date('1599-12-31')
        else date_of_death
    end as date_of_death,
    accepting_new_patients_ind,
    SPCL
from
    Prov02_Main_Latest1
where
    tms_is_active = 1
    and (submitting_state_prov_id is not null)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
