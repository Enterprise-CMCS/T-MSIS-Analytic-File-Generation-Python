create table Prov09_AffPgms_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
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
    nullif(trim(upper(affiliated_program_id)), '') as affiliated_program_id,
    affiliated_program_type,
    prov_affiliated_program_eff_date,
    prov_affiliated_program_end_date
from
    #Prov09_AffPgms_Latest1 where tms_is_active=1 and (nullif(trim(upper(affiliated_program_id)),'') is not null) order by tms_run_id, submitting_state, submitting_state_prov_id;;