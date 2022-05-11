create temp table HEADER_RX distkey(ORGNL_CLM_NUM) sortkey(
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
    a.TMSIS_RPTG_PRD,
    upper(a.SECT_1115A_DEMO_IND) as SECT_1115A_DEMO_IND,
    coalesce(a.ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT,
    upper(COALESCE(a.ADJSTMT_IND, 'X')) AS ADJSTMT_IND,
    upper(a.ADJSTMT_RSN_CD) as ADJSTMT_RSN_CD,
    a.BENE_COINSRNC_AMT,
    a.BENE_COPMT_AMT,
    a.BENE_DDCTBL_AMT,
    upper(a.BLG_PRVDR_NPI_NUM) as BLG_PRVDR_NPI_NUM,
    upper(a.BLG_PRVDR_NUM) as BLG_PRVDR_NUM,
    upper(a.BLG_PRVDR_SPCLTY_CD) as BLG_PRVDR_SPCLTY_CD,
    upper(a.BLG_PRVDR_TXNMY_CD) as BLG_PRVDR_TXNMY_CD,
    upper(a.BRDR_STATE_IND) as BRDR_STATE_IND,
    a.CLL_CNT,
    upper(a.CLM_STUS_CTGRY_CD) as CLM_STUS_CTGRY_CD,
    upper(a.CMPND_DRUG_IND) as CMPND_DRUG_IND,
    upper(a.COPAY_WVD_IND) as COPAY_WVD_IND,
    upper(a.XOVR_IND) as XOVR_IND,
    a.PRSCRBD_DT,
    upper(a.DSPNSNG_PD_PRVDR_NPI_NUM) as DSPNSNG_PD_PRVDR_NPI_NUM,
    upper(a.DSPNSNG_PD_PRVDR_NUM) as DSPNSNG_PD_PRVDR_NUM,
    upper(a.FIXD_PYMT_IND) as FIXD_PYMT_IND,
    upper(a.FUNDNG_CD) as FUNDNG_CD,
    upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as FUNDNG_SRC_NON_FED_SHR_CD,
    coalesce(upper(a.ADJSTMT_CLM_NUM), '~') AS ADJSTMT_CLM_NUM,
    coalesce(upper(a.ORGNL_CLM_NUM), '~') AS ORGNL_CLM_NUM,
    upper(a.MDCR_BENE_ID) as MDCR_BENE_ID,
    upper(a.MDCR_HICN_NUM) as MDCR_HICN_NUM,
    upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM,
    upper(a.OTHR_INSRNC_IND) as OTHR_INSRNC_IND,
    upper(a.OTHR_TPL_CLCTN_CD) as OTHR_TPL_CLCTN_CD,
    upper(a.PYMT_LVL_IND) as PYMT_LVL_IND,
    upper(a.PLAN_ID_NUM) as MC_PLAN_ID,
    upper(a.SRVCNG_PRVDR_NPI_NUM) as SRVCNG_PRVDR_NPI_NUM,
    upper(a.PRSCRBNG_PRVDR_NUM) as PRSCRBNG_PRVDR_NUM,
    a.RX_FILL_DT,
    upper(a.PGM_TYPE_CD) as PGM_TYPE_CD,
    upper(a.PRVDR_LCTN_ID) as PRVDR_LCTN_ID,
    a.SRVC_TRKNG_PYMT_AMT,
    upper(a.SRVC_TRKNG_TYPE_CD) as SRVC_TRKNG_TYPE_CD,
    a.SUBMTG_STATE_CD,
    a.TOT_ALOWD_AMT,
    a.TOT_BILL_AMT,
    a.TOT_COPAY_AMT,
    a.TOT_MDCD_PD_AMT,
    a.TOT_MDCR_COINSRNC_AMT,
    a.TOT_MDCR_DDCTBL_AMT,
    a.TOT_OTHR_INSRNC_AMT,
    a.TOT_TPL_AMT,
    upper(a.CLM_TYPE_CD) as CLM_TYPE_CD,
    upper(a.WVR_ID) as WVR_ID,
    upper(a.WVR_TYPE_CD) as WVR_TYPE_CD,
    a.MDCD_PD_DT,
    upper(a.ELGBL_1ST_NAME) as ELGBL_1ST_NAME,
    upper(a.ELGBL_LAST_NAME) as ELGBL_LAST_NAME,
    upper(a.ELGBL_MDL_INITL_NAME) as ELGBL_MDL_INITL_NAME,
    a.BIRTH_DT,
    a.TP_COINSRNC_PD_AMT,
    a.TP_COPMT_PD_AMT
from
    cms_prod.TMSIS_CLH_REC_RX A
where
    (
        (
            date_part_year (a.RX_FILL_DT) = 2017
            and date_part (month, a.RX_FILL_DT) = 12
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
    and (a.submtg_state_cd, a.tmsis_run_id) in (
        ('01', 4617),
        ('02', 4685),
        ('04', 4663),
        ('05', 4661),
        ('06', 4605),
        ('08', 4629),
        ('09', 4664),
        ('10', 4618),
        ('11', 4662),
        ('12', 4679),
        ('13', 4656),
        ('15', 4667),
        ('16', 4643),
        ('17', 4640),
        ('18', 4681),
        ('19', 4600),
        ('20', 4651),
        ('21', 4666),
        ('22', 4670),
        ('23', 4654),
        ('24', 4682),
        ('25', 4672),
        ('26', 4645),
        ('27', 4652),
        ('28', 4649),
        ('29', 4665),
        ('30', 4632),
        ('31', 4657),
        ('32', 4627),
        ('33', 4620),
        ('34', 4650),
        ('35', 4668),
        ('36', 4635),
        ('37', 4648),
        ('38', 4639),
        ('39', 4624),
        ('40', 4659),
        ('41', 4609),
        ('42', 4671),
        ('44', 4647),
        ('45', 4673),
        ('46', 4603),
        ('47', 4625),
        ('48', 4653),
        ('49', 4655),
        ('50', 4638),
        ('51', 4674),
        ('53', 4642),
        ('54', 4628),
        ('55', 4612),
        ('56', 4423),
        ('72', 4660),
        ('78', 4687),
        ('93', 4447),
        ('97', 4658)
    )