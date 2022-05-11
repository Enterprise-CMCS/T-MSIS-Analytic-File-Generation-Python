create table Prov06_Taxonomies_seg diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
select
    6539 :: integer as DA_RUN_ID,
    cast (
        (
            '46' || '-' || 201712 || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')
        ) as varchar(50)
    ) as PRV_LINK_KEY,
    201712 :: varchar(10) as PRV_FIL_DT,
    '46' :: varchar(2) as PRV_VRSN,
    tms_run_id as TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    PRVDR_CLSFCTN_TYPE_CD,
    PRVDR_CLSFCTN_CD
from
    #Prov06_Taxonomies_ALL where 
    PRVDR_CLSFCTN_TYPE_CD is not null
    and PRVDR_CLSFCTN_CD is not null
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID;