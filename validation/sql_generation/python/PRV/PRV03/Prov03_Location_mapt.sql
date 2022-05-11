create
or replace temporary view Prov03_Location_mapt as
select
    [ 'tms_run_id',
    'submitting_state',
    'submitting_state_prov_id',
    'prov_location_id' ],
    PRV_LOC_LINK_KEY,
    max(PRVDR_ADR_BLG_IND) as PRVDR_ADR_BLG_IND,
    max(PRVDR_ADR_PRCTC_IND) as PRVDR_ADR_PRCTC_IND,
    max(PRVDR_ADR_SRVC_IND) as PRVDR_ADR_SRVC_IND
from
    Prov03_Location_TYPE
group by
    [ 'tms_run_id',
    'submitting_state',
    'submitting_state_prov_id',
    'prov_location_id' ],
    PRV_LOC_LINK_KEY
