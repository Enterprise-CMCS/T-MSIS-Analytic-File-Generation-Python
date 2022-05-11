create table Prov07_Medicaid_CNST diststyle key distkey(submitting_state_prov_id) as
select
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    SUBMTG_STATE_CD,
    PRVDR_MDCD_ENRLMT_STUS_CD,
    STATE_PLAN_ENRLMT_CD,
    PRVDR_MDCD_ENRLMT_MTHD_CD,
    appl_date as APLCTN_DT,
    PRVDR_MDCD_ENRLMT_STUS_CTGRY,
    prov_medicaid_eff_date as PRVDR_MDCD_EFCTV_DT,
    prov_medicaid_end_date as PRVDR_MDCD_END_DT,
    case
        when STATE_PLAN_ENRLMT_CD = 1
        or STATE_PLAN_ENRLMT_CD = 3 then 1
        when STATE_PLAN_ENRLMT_CD is null then null
        else 0
    end :: smallint as MDCD_ENRLMT_IND,
    case
        when STATE_PLAN_ENRLMT_CD = 2
        or STATE_PLAN_ENRLMT_CD = 3 then 1
        when STATE_PLAN_ENRLMT_CD is null then null
        else 0
    end :: smallint as CHIP_ENRLMT_IND,
    null :: smallint as MDCD_CHIP_ENRLMT_IND,
    case
        when STATE_PLAN_ENRLMT_CD = 4 then 1
        when STATE_PLAN_ENRLMT_CD is null then null
        else 0
    end :: smallint as NOT_SP_AFLTD_IND,
    case
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY = 1 then 1
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
        else 0
    end :: smallint as PRVDR_ENRLMT_STUS_ACTV_IND,
    case
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY = 2 then 1
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
        else 0
    end :: smallint as PRVDR_ENRLMT_STUS_DND_IND,
    case
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY = 3 then 1
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
        else 0
    end :: smallint as PRVDR_ENRLMT_STUS_PENDG_IND,
    case
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY = 4 then 1
        when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
        else 0
    end :: smallint as PRVDR_ENRLMT_STUS_TRMNTD_IND
from
    Prov07_Medicaid_MTD
where
    PRVDR_MDCD_ENRLMT_STUS_CD is not null
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;