create table Prov04_Licensing_CNST diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
select
    6539 :: integer as DA_RUN_ID,
    cast (
        (
            '46' || '-' || 201712 || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')
        ) as varchar(74)
    ) as PRV_LOC_LINK_KEY,
    201712 :: varchar(10) as PRV_FIL_DT,
    '46' :: varchar(2) as PRV_VRSN,
    tms_run_id as TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    prov_location_id as PRVDR_LCTN_ID,
    LCNS_TYPE_CD,
    license_or_accreditation_number as LCNS_OR_ACRDTN_NUM,
    license_issuing_entity_id as LCNS_ISSG_ENT_ID_TXT
from
    #Prov04_Licensing_TYP where LCNS_TYPE_CD is not null order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, 
    PRVDR_LCTN_ID;