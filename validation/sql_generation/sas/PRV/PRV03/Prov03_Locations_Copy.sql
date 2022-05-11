create table Prov03_Locations_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
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
    prov_addr_type,
    prov_location_and_contact_info_eff_date,
    prov_location_and_contact_info_end_date,
    addr_ln1,
    addr_ln2,
    addr_ln3,
    addr_city,
    nullif(trim(upper(addr_state)), '') as addr_state,
    addr_zip_code,
    addr_county,
    addr_border_state_ind
from
    #Prov03_Locations_Latest1 where tms_is_active=1 and (prov_addr_type=1 or prov_addr_type=3 or 
    prov_addr_type = 4
)
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;

;