create table Prov06_Taxonomies_ALL as (
    select
        tms_run_id,
        submtg_state_cd,
        submitting_state,
        submitting_state_prov_id,
        prvdr_clsfctn_type_cd,
        prvdr_clsfctn_cd
    from
        #Prov06_Taxonomies_CCD2) union (select tms_run_id, submtg_state_cd, submitting_state, 
        submitting_state_prov_id,
        prvdr_clsfctn_type_cd,
        prvdr_clsfctn_cd
    from
        #nppes_tax_final) order by tms_run_id, submitting_state, submitting_state_prov_id;