create
or replace temporary view Prov05_Identifiers_CNST as
select
    6194 as DA_RUN_ID,
    case
        when SPCL is not null then cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*') | | '-' | | coalesce(prov_location_id, '**') | | '-' | | SPCL
            ) as varchar(74)
        )
        else cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*') | | '-' | | coalesce(prov_location_id, '**')
            ) as varchar(74)
        )
    end as PRV_LOC_LINK_KEY,
    202110 as PRV_FIL_DT,
    '0A' as PRV_VRSN,
    tms_run_id as TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    prov_location_id as PRVDR_LCTN_ID,
    PRVDR_ID_TYPE_CD,
    prov_identifier as PRVDR_ID,
    prov_identifier_issuing_entity_id as PRVDR_ID_ISSG_ENT_ID_TXT
from
    Prov05_Identifiers_TYP
where
    PRVDR_ID_TYPE_CD is not null
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID,
    PRVDR_LCTN_ID
