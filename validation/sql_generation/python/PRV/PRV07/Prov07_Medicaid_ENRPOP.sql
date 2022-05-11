create
or replace temporary view Prov07_Medicaid_ENRPOP as
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
    SUBMTG_STATE_PRVDR_ID
