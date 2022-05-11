create
or replace temporary view Prov02_Base as
select
    M.DA_RUN_ID,
    M.prv_link_key,
    M.PRV_FIL_DT,
    M.PRV_VRSN,
    M.tmsis_run_id,
    M.submtg_state_cd,
    M.submtg_state_prvdr_id,
    M.REG_FLAG,
    M.prvdr_dba_name,
    M.prvdr_lgl_name,
    M.prvdr_org_name,
    M.prvdr_tax_name,
    M.fac_grp_indvdl_cd,
    M.tchng_ind,
    M.prvdr_1st_name,
    M.prvdr_mdl_initl_name,
    M.prvdr_last_name,
    M.gndr_cd,
    M.ownrshp_cd,
    M.ownrshp_cat,
    M.prvdr_prft_stus_cd,
    M.birth_dt,
    M.death_dt,
    M.acpt_new_ptnts_ind,
    M.age_num,
    coalesce(E.PRVDR_MDCD_ENRLMT_IND, 0) as PRVDR_MDCD_ENRLMT_IND,
    case
        when E.MDCD_CHIP_ENRLMT_IND = 1 then 0
        else E.MDCD_ENRLMT_IND
    end as MDCD_ENRLMT_IND,
    case
        when E.MDCD_CHIP_ENRLMT_IND = 1 then 0
        else E.CHIP_ENRLMT_IND
    end as CHIP_ENRLMT_IND,
    E.MDCD_CHIP_ENRLMT_IND,
    E.NOT_SP_AFLTD_IND,
    E.PRVDR_ENRLMT_STUS_ACTV_IND,
    E.PRVDR_ENRLMT_STUS_DND_IND,
    E.PRVDR_ENRLMT_STUS_TRMNTD_IND,
    E.PRVDR_ENRLMT_STUS_PENDG_IND,
    T.MLT_SNGL_SPCLTY_GRP_IND,
    T.ALPTHC_OSTPTHC_PHYSN_IND,
    T.BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
    T.CHRPRCTIC_PRVDR_IND,
    T.DNTL_PRVDR_IND,
    T.DTRY_NTRTNL_SRVC_PRVDR_IND,
    T.emer_MDCL_srvc_prvdr_ind,
    T.eye_VSN_srvc_prvdr_ind,
    T.nrsng_srvc_prvdr_ind,
    T.nrsng_srvc_rltd_ind,
    T.othr_INDVDL_srvc_prvdr_ind,
    T.PHRMCY_SRVC_PRVDR_IND,
    T.PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
    T.POD_MDCN_SRGRY_SRVCS_IND,
    T.resp_dev_reh_restor_prvdr_ind,
    T.SPCH_LANG_HEARG_SRVC_PRVDR_IND,
    T.STDNT_HLTH_CARE_PRVDR_IND,
    T.TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
    T.agncy_prvdr_ind,
    T.amb_hlth_CARE_fac_prvdr_ind,
    T.hosp_unit_prvdr_ind,
    T.hosp_prvdr_ind,
    T.lab_prvdr_ind,
    T.mco_prvdr_ind,
    T.NRSNG_CSTDL_CARE_FAC_IND,
    T.OTHR_NONINDVDL_SRVC_PRVDRS_IND,
    T.RSDNTL_TRTMT_FAC_PRVDR_IND,
    T.RESP_CARE_FAC_PRVDR_IND,
    T.SUPLR_PRVDR_IND,
    T.TRNSPRTN_SRVCS_PRVDR_IND,
    T.sud_srvc_prvdr_ind,
    T.mh_srvc_prvdr_ind,
    T.emer_srvcs_prvdr_ind
from
    Prov02_Main_CNST M
    left join Prov07_Medicaid_Mapped E on M.tms_run_id = E.tms_run_id
    and M.submitting_state = E.submitting_state
    and M.submitting_state_prov_id = E.submitting_state_prov_id
    left join Prov06_Taxonomies_Mapped T on M.tms_run_id = T.tms_run_id
    and M.submitting_state = T.submitting_state
    and M.submitting_state_prov_id = T.submitting_state_prov_id
order by
    M.tmsis_run_id,
    M.submtg_state_cd,
    M.submtg_state_prvdr_id
