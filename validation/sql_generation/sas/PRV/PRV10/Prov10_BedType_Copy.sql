create table Prov10_BedType_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
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
    nullif(trim(upper(prov_location_id)), '') as prov_location_id,
    bed_count,
    case
        when trim(bed_type_code) in ('1', '2', '3', '4') then trim(bed_type_code)
        else null
    end as bed_type_code,
    bed_type_eff_date,
    bed_type_end_date
from
    #Prov10_BedType_Latest1 where tms_is_active=1 and ((trim(bed_type_code) in ('1','2','3','4')) or (bed_count is not null and bed_count<>0)) order by tms_run_id, submitting_state, 
    submitting_state_prov_id;

;