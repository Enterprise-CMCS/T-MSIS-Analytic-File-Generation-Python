create
or replace temporary view Prov06_Taxonomies_seg as
select
    6194 as DA_RUN_ID,
    case
        when SPCL is not null then cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*') | | '-' | | SPCL
            ) as varchar(50)
        )
        else cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*')
            ) as varchar(50)
        )
    end as PRV_LINK_KEY,
    202110 as PRV_FIL_DT,
    '0A' as PRV_VRSN,
    tms_run_id as TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    PRVDR_CLSFCTN_TYPE_CD,
    PRVDR_CLSFCTN_CD
from
    Prov06_Taxonomies_All
where
    PRVDR_CLSFCTN_TYPE_CD is not null
    and PRVDR_CLSFCTN_CD is not null
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID
