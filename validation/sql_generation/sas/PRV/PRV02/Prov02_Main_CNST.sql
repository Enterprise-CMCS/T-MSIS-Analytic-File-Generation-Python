create table Prov02_Main_CNST diststyle key distkey(submitting_state_prov_id) as
select
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    tms_run_id as TMSIS_RUN_ID,
    201712 :: varchar(10) as PRV_FIL_DT,
    '46' :: varchar(2) as PRV_VRSN,
    6539 :: integer as DA_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    REG_FLAG,
    case
        when nullif(trim(prov_doing_business_as_name), '') = '888888888888888888888888888888888888888888888888888888888888888' then ''
        when nullif(trim(prov_doing_business_as_name), '') = '99999999999999999999999999999999999999999999999999' then ''
        else nullif(trim(upper(prov_doing_business_as_name)), '')
    end as PRVDR_DBA_NAME,
    nullif(trim(upper(prov_legal_name)), '') as PRVDR_LGL_NAME,
    nullif(trim(upper(prov_organization_name)), '') as PRVDR_ORG_NAME,
    nullif(trim(upper(prov_tax_name)), '') as PRVDR_TAX_NAME,
    FAC_GRP_INDVDL_CD,
    TCHNG_IND,
    nullif(trim(upper(prov_first_name)), '') as PRVDR_1ST_NAME,
    nullif(trim(upper(prov_middle_initial)), '') as PRVDR_MDL_INITL_NAME,
    nullif(trim(upper(prov_last_name)), '') as PRVDR_LAST_NAME,
    GNDR_CD,
    OWNRSHP_CD,
    OWNRSHP_CAT,
    PRVDR_PRFT_STUS_CD,
    date_of_birth as BIRTH_DT,
    DEATH_DT,
    ACPT_NEW_PTNTS_IND,
    case
        when AGE_NUM < 15 then null
        when AGE_NUM > 125 then 125
        else AGE_NUM
    end as AGE_NUM,
    cast (
        (
            '46' || '-' || 201712 || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')
        ) as varchar(50)
    ) as PRV_LINK_KEY
from
    #Prov02_Main_All order by tms_run_id, submitting_state, submitting_state_prov_id;