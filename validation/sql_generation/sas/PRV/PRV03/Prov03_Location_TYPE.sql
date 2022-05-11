create table Prov03_Location_TYPE diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    *,
    case
        when prov_addr_type = '1' then 1
        when prov_addr_type = '3'
        or prov_addr_type = '4' then 0
        else null
    end :: smallint as PRVDR_ADR_BLG_IND,
    case
        when prov_addr_type = '3' then 1
        when prov_addr_type = '1'
        or prov_addr_type = '4' then 0
        else null
    end :: smallint as PRVDR_ADR_PRCTC_IND,
    case
        when prov_addr_type = '4' then 1
        when prov_addr_type = '3'
        or prov_addr_type = '1' then 0
        else null
    end :: smallint as PRVDR_ADR_SRVC_IND
from
    #Prov03_Locations_BS order by PRV_LOC_LINK_KEY;