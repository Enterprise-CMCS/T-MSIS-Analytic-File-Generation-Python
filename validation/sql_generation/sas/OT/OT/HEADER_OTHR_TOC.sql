create temp table HEADER_OTHR_TOC distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    a.TMSIS_RUN_ID,
    a.TMSIS_ACTV_IND,
    upper(a.SECT_1115A_DEMO_IND) as sect_1115a_demo_ind,
    coalesce(a.ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT,
    coalesce(upper(a.ADJSTMT_IND), 'X') as ADJSTMT_IND,
    upper(a.ADJSTMT_RSN_CD) as adjstmt_rsn_cd,
    coalesce(a.SRVC_BGNNG_DT, '01JAN1960') as SRVC_BGNNG_DT_HEADER,
    a.BENE_COINSRNC_AMT,
    a.BENE_COINSRNC_PD_DT,
    a.BENE_COPMT_AMT,
    a.BENE_COPMT_PD_DT,
    a.BENE_DDCTBL_AMT,
    a.BENE_DDCTBL_PD_DT,
    upper(a.BLG_PRVDR_NPI_NUM) as blg_prvdr_npi_num,
    upper(a.BLG_PRVDR_NUM) as blg_prvdr_num,
    upper(a.BLG_PRVDR_SPCLTY_CD) as blg_prvdr_spclty_cd,
    upper(a.BLG_PRVDR_TXNMY_CD) as blg_prvdr_txnmy_cd,
    upper(a.BLG_PRVDR_TYPE_CD) as blg_prvdr_type_cd,
    upper(a.BRDR_STATE_IND) as brdr_state_ind,
    a.CPTATD_PYMT_RQSTD_AMT,
    upper(a.CLM_DND_IND) as clm_dnd_ind,
    a.CLL_CNT,
    upper(a.CLM_STUS_CD) as clm_stus_cd,
    upper(a.COPAY_WVD_IND) as copay_wvd_ind,
    upper(a.XOVR_IND) as xovr_ind,
    a.DAILY_RATE,
    a.CPTATD_AMT_RQSTD_DT,
    a.BIRTH_DT,
    trim(translate(upper(a.DGNS_1_CD), '.', '')) as dgns_1_cd,
    trim(translate(upper(a.DGNS_2_CD), '.', '')) as dgns_2_cd,
    upper(a.DGNS_1_CD_IND) as dgns_1_cd_ind,
    upper(a.DGNS_2_CD_IND) as dgns_2_cd_ind,
    upper(a.DGNS_POA_1_CD_IND) as dgns_poa_1_cd_ind,
    upper(a.DGNS_POA_2_CD_IND) as dgns_poa_2_cd_ind,
    upper(a.ELGBL_1ST_NAME) as elgbl_1st_name,
    upper(a.ELGBL_LAST_NAME) as elgbl_last_name,
    upper(a.ELGBL_MDL_INITL_NAME) as elgbl_mdl_initl_name,
    coalesce(a.SRVC_ENDG_DT, '01JAN1960') as SRVC_ENDG_DT_HEADER,
    upper(a.FIXD_PYMT_IND) as fixd_pymt_ind,
    upper(a.FUNDNG_CD) as fundng_cd,
    upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as fundng_src_non_fed_shr_cd,
    upper(a.HLTH_CARE_ACQRD_COND_CD) as hlth_care_acqrd_cond_cd,
    upper(a.HH_ENT_NAME) as hh_ent_name,
    upper(a.HH_PRVDR_IND) as hh_prvdr_ind,
    upper(a.HH_PRVDR_NPI_NUM) as hh_prvdr_npi_num,
    coalesce(upper(a.ADJSTMT_CLM_NUM), '~') AS ADJSTMT_CLM_NUM,
    coalesce(upper(a.ORGNL_CLM_NUM), '~') AS ORGNL_CLM_NUM,
    a.MDCD_PD_DT,
    upper(a.MDCR_BENE_ID) as mdcr_bene_id,
    upper(a.MDCR_CMBND_DDCTBL_IND) as mdcr_cmbnd_ddctbl_ind,
    upper(a.MDCR_HICN_NUM) as mdcr_hicn_num,
    upper(a.MDCR_REIMBRSMT_TYPE_CD) as mdcr_reimbrsmt_type_cd,
    upper(a.MSIS_IDENT_NUM) as msis_ident_num,
    upper(a.OCRNC_01_CD) as ocrnc_01_cd,
    upper(a.OCRNC_02_CD) as ocrnc_02_cd,
    upper(a.OCRNC_03_CD) as ocrnc_03_cd,
    upper(a.OCRNC_04_CD) as ocrnc_04_cd,
    upper(a.OCRNC_05_CD) as ocrnc_05_cd,
    upper(a.OCRNC_06_CD) as ocrnc_06_cd,
    upper(a.OCRNC_07_CD) as ocrnc_07_cd,
    upper(a.OCRNC_08_CD) as ocrnc_08_cd,
    upper(a.OCRNC_09_CD) as ocrnc_09_cd,
    upper(a.OCRNC_10_CD) as ocrnc_10_cd,
    a.OCRNC_01_CD_EFCTV_DT,
    a.OCRNC_02_CD_EFCTV_DT,
    a.OCRNC_03_CD_EFCTV_DT,
    a.OCRNC_04_CD_EFCTV_DT,
    a.OCRNC_05_CD_EFCTV_DT,
    a.OCRNC_06_CD_EFCTV_DT,
    a.OCRNC_07_CD_EFCTV_DT,
    a.OCRNC_08_CD_EFCTV_DT,
    a.OCRNC_09_CD_EFCTV_DT,
    a.OCRNC_10_CD_EFCTV_DT,
    a.OCRNC_01_CD_END_DT,
    a.OCRNC_02_CD_END_DT,
    a.OCRNC_03_CD_END_DT,
    a.OCRNC_04_CD_END_DT,
    a.OCRNC_05_CD_END_DT,
    a.OCRNC_06_CD_END_DT,
    a.OCRNC_07_CD_END_DT,
    a.OCRNC_08_CD_END_DT,
    a.OCRNC_09_CD_END_DT,
    a.OCRNC_10_CD_END_DT,
    upper(a.OTHR_INSRNC_IND) as othr_insrnc_ind,
    upper(a.OTHR_TPL_CLCTN_CD) as othr_tpl_clctn_cd,
    upper(a.PYMT_LVL_IND) as pymt_lvl_ind,
    upper(a.SRVC_PLC_CD) as srvc_plc_cd,
    upper(a.PLAN_ID_NUM) as mc_plan_id,
    upper(a.PGM_TYPE_CD) as pgm_type_cd,
    upper(a.PRVDR_LCTN_ID) as prvdr_lctn_id,
    upper(a.RFRG_PRVDR_NPI_NUM) as rfrg_prvdr_npi_num,
    upper(a.RFRG_PRVDR_NUM) as rfrg_prvdr_num,
    upper(a.RFRG_PRVDR_SPCLTY_CD) as rfrg_prvdr_spclty_cd,
    upper(a.RFRG_PRVDR_TXNMY_CD) as rfrg_prvdr_txnmy_cd,
    upper(a.RFRG_PRVDR_TYPE_CD) as rfrg_prvdr_type_cd,
    upper(a.RMTNC_NUM) as rmtnc_num,
    a.SRVC_TRKNG_PYMT_AMT,
    upper(a.SRVC_TRKNG_TYPE_CD) as srvc_trkng_type_cd,
    a.SUBMTG_STATE_CD,
    a.TOT_MDCR_COINSRNC_AMT,
    a.TOT_MDCR_DDCTBL_AMT,
    upper(a.BILL_TYPE_CD) as bill_type_cd,
    upper(a.CLM_TYPE_CD) as clm_type_cd,
    upper(a.WVR_ID) as wvr_id,
    upper(a.WVR_TYPE_CD) as wvr_type_cd,
    upper(a.PRVDR_UNDER_DRCTN_NPI_NUM) as prvdr_under_drctn_npi_num,
    upper(a.PRVDR_UNDER_DRCTN_TXNMY_CD) as prvdr_under_drctn_txnmy_cd,
    upper(a.PRVDR_UNDER_SPRVSN_NPI_NUM) as prvdr_under_sprvsn_npi_num,
    upper(a.PRVDR_UNDER_SPRVSN_TXNMY_CD) as prvdr_under_sprvsn_txnmy_cd,
    a.TP_COINSRNC_PD_AMT,
    a.TP_COINSRNC_PD_DT,
    a.TP_COPMT_PD_AMT,
    a.TP_COPMT_PD_DT,
    a.TOT_ALOWD_AMT,
    a.TOT_BILL_AMT,
    a.TOT_COPAY_AMT,
    a.TOT_MDCD_PD_AMT,
    a.TOT_OTHR_INSRNC_AMT,
    a.TOT_TPL_AMT,
    coalesce(a.SRVC_ENDG_DT, a.SRVC_BGNNG_DT) as SRVC_ENDG_DT_DRVD_H,
    case
        when a.SRVC_ENDG_DT is not null then '2'
        when a.SRVC_ENDG_DT is null
        and a.SRVC_BGNNG_DT is not null then '3'
        else null
    end as SRVC_ENDG_DT_CD_H,
    case
        when a.SRVC_ENDG_DT is NULL
        and a.SRVC_BGNNG_DT is null then 1
        else 0
    end as NO_SRVC_DT
from
    cms_prod.TMSIS_CLH_REC_OTHR_TOC A
where
    (
        (
            date_part_year (nvl(a.SRVC_ENDG_DT, a.SRVC_BGNNG_DT)) = 2017
            and date_part (month, nvl(a.SRVC_ENDG_DT, a.SRVC_BGNNG_DT)) = 10
        )
        or (
            a.SRVC_ENDG_DT is null
            AND a.SRVC_BGNNG_DT is null
        )
    )
    and A.TMSIS_ACTV_IND = 1
    and (
        upper(A.CLM_STUS_CTGRY_CD) <> 'F2'
        or A.CLM_STUS_CTGRY_CD is null
    )
    and (
        upper(A.CLM_TYPE_CD) <> 'Z'
        or A.CLM_TYPE_CD is null
    )
    and (
        A.CLM_DND_IND <> '0'
        or A.CLM_DND_IND is null
    )
    and (
        A.CLM_STUS_CD NOT IN('26', '87', '026', '087', '542', '585', '654')
        or A.CLM_STUS_CD is null
    )
    and (
        A.ADJSTMT_IND <> '1'
        or A.ADJSTMT_IND IS NULL
    )
    and a.submtg_state_cd = '01'
    and a.tmsis_run_id = 4684