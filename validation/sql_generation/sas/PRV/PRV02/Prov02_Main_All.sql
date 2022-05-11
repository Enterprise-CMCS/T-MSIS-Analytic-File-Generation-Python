create table Prov02_Main_All diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    R.tms_run_id,
    R.tms_reporting_period,
    R.submitting_state,
    R.record_number,
    R.submitting_state_prov_id,
    R.prov_attributes_eff_date,
    R.prov_attributes_end_date,
    R.prov_doing_business_as_name,
    R.prov_legal_name,
    R.prov_organization_name,
    R.prov_tax_name,
    R.SUBMTG_STATE_CD,
    R.State,
    R.REG_FLAG,
    R.FAC_GRP_INDVDL_CD,
    R.TCHNG_IND,
    R.OWNRSHP_CD,
    R.OWNRSHP_CAT,
    R.PRVDR_PRFT_STUS_CD,
    R.ACPT_NEW_PTNTS_IND,
    T.prov_first_name,
    T.prov_middle_initial,
    T.prov_last_name,
    T.GNDR_CD,
    T.AGE_NUM,
    T.date_of_birth,
    T.DEATH_DT
from
    #Prov02_Main_NP R left join #Prov02_Main_GC 
    T on R.tms_run_id = T.tms_run_id
    and R.submitting_state = T.submitting_state
    and R.submitting_state_prov_id = T.submitting_state_prov_id
order by
    R.tms_run_id,
    R.submitting_state,
    R.submitting_state_prov_id;