create table Prov07_Medicaid_ENRPOP diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
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
    PRVDR_MDCD_EFCTV_DT,
    PRVDR_MDCD_END_DT,
    PRVDR_MDCD_ENRLMT_STUS_CD,
    STATE_PLAN_ENRLMT_CD,
    PRVDR_MDCD_ENRLMT_MTHD_CD,
    APLCTN_DT,
    PRVDR_MDCD_ENRLMT_STUS_CTGRY
from
    Prov07_Medicaid_CNST
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID;