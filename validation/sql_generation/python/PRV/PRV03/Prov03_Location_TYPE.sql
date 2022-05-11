create
or replace temporary view Prov03_Location_TYPE as
select
    *,
    case
        when prov_addr_type = '1' then 1
        when prov_addr_type = '3'
        or prov_addr_type = '4' then 0
        else null
    end as PRVDR_ADR_BLG_IND,
    case
        when prov_addr_type = '3' then 1
        when prov_addr_type = '1'
        or prov_addr_type = '4' then 0
        else null
    end as PRVDR_ADR_PRCTC_IND,
    case
        when prov_addr_type = '4' then 1
        when prov_addr_type = '3'
        or prov_addr_type = '1' then 0
        else null
    end as PRVDR_ADR_SRVC_IND
from
    Prov03_Locations_BS
order by
    PRV_LOC_LINK_KEY
