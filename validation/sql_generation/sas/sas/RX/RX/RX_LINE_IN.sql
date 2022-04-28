create temp table RX_LINE_IN distkey (ORGNL_CLM_NUM_LINE) sortkey (
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
) as
select
    SUBMTG_STATE_CD,
    upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM_LINE,
    a.ALOWD_AMT,
    a.TMSIS_RUN_ID as TMSIS_RUN_ID_LINE,
    a.TMSIS_ACTV_IND as TMSIS_ACTV_IND3,
    upper(a.BNFT_TYPE_CD) as BNFT_TYPE_CD,
    upper(a.CLL_STUS_CD) as CLL_STUS_CD,
    a.BILL_AMT,
    upper(a.BRND_GNRC_IND) as BRND_GNRC_IND,
    upper(a.CMS_64_FED_REIMBRSMT_CTGRY_CD) as CMS_64_FED_REIMBRSMT_CTGRY_CD,
    upper(a.CMPND_DSG_FORM_CD) as CMPND_DSG_FORM_CD,
    a.COPAY_AMT,
    coalesce(a.ADJDCTN_DT, '01JAN1960') AS ADJDCTN_DT_LINE,
    coalesce(upper(a.ADJSTMT_CLM_NUM), '~') AS ADJSTMT_CLM_NUM_LINE,
    coalesce(upper(a.ORGNL_CLM_NUM), '~') AS ORGNL_CLM_NUM_LINE,
    upper(a.ORGNL_LINE_NUM) as ORGNL_LINE_NUM,
    upper(a.ADJSTMT_LINE_NUM) as ADJSTMT_LINE_NUM,
    a.SUPLY_DAYS_CNT,
    a.DSPNS_FEE_AMT,
    upper(a.DRUG_UTLZTN_CD) as DRUG_UTLZTN_CD,
    a.DTL_MTRC_DCML_QTY,
    upper(a.IMNZTN_TYPE_CD) as IMNZTN_TYPE_CD,
    a.MDCD_FFS_EQUIV_AMT,
    a.MDCD_PD_AMT,
    upper(a.NDC_CD) as NDC_CD,
    upper(a.NEW_REFL_IND) as NEW_REFL_IND,
    a.OTHR_TOC_RX_CLM_ACTL_QTY,
    a.OTHR_TOC_RX_CLM_ALOWD_QTY,
    a.MDCR_COINSRNC_PD_AMT,
    a.MDCR_DDCTBL_AMT,
    a.OTHR_INSRNC_AMT,
    upper(a.REBT_ELGBL_IND) as REBT_ELGBL_IND,
    upper(a.SUBMTG_STATE_CD) as SUBMTG_STATE_CD_LINE,
    a.TPL_AMT,
    upper(a.stc_cd) as TOS_CD,
    upper(a.UOM_CD) as UOM_CD,
    upper(lpad(trim(a.XIX_SRVC_CTGRY_CD), 4, '0')) as XIX_SRVC_CTGRY_CD,
    upper(lpad(trim(a.XXI_SRVC_CTGRY_CD), 3, '0')) as XXI_SRVC_CTGRY_CD,
    a.TMSIS_FIL_NAME,
    a.REC_NUM,
    upper(COALESCE(a.LINE_ADJSTMT_IND, 'X')) AS LINE_ADJSTMT_IND,
    a.MDCR_PD_AMT
from
    cms_prod.TMSIS_CLL_REC_RX A
where
    A.TMSIS_ACTV_IND = 1
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