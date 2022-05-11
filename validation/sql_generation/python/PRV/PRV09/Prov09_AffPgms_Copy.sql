create
or replace temporary view Prov09_AffPgms_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(trim(upper(affiliated_program_id)), '') as affiliated_program_id,
    affiliated_program_type,
    prov_affiliated_program_eff_date,
    prov_affiliated_program_end_date,
    SPCL
from
    Prov09_AffPgms_Latest1
where
    tms_is_active = 1
    and (affiliated_program_id is not null)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
