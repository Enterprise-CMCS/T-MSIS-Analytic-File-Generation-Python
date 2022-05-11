create table Prov03_Location_BSM diststyle key distkey(PRV_LOC_LINK_KEY) compound sortkey (PRV_LOC_LINK_KEY) as
select
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id,
    tms_reporting_period,
    record_number,
    prov_addr_type,
    prov_location_and_contact_info_eff_date,
    prov_location_and_contact_info_end_date,
    addr_ln1,
    addr_ln2,
    addr_ln3,
    addr_city,
    addr_state,
    addr_zip_code,
    addr_county,
    addr_border_state_ind,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_rcd,
    ADR_STATE_CD,
    ADR_BRDR_STATE_IND,
    PRV_LOC_LINK_KEY
from
    #Prov03_Locations_BS order by PRV_LOC_LINK_KEY;