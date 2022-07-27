# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.TAF_Closure import TAF_Closure


class IP_Metadata:

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def selectDataElements(segment_id: str, alias: str):

        new_line_comma = "\n\t\t\t,"

        columns = IP_Metadata.columns.get(segment_id).copy()

        for i, item in enumerate(columns):
            if item in IP_Metadata.cleanser.keys():
                columns[i] = IP_Metadata.cleanser.copy().get(item)(item, alias)
            elif item in IP_Metadata.validator.keys():
                columns[i] = IP_Metadata.maskInvalidValues(item, alias)
            elif item in IP_Metadata.upper:
                columns[i] = f"upper({alias}.{item}) as {str(item).lower()}"
            else:
                columns[i] = f"{alias}.{columns[i]}"

            # qualify header columns
            if segment_id == "CIP00002":
                if item in IP_Metadata.header_renames.keys():
                    columns[i] = (
                        columns[i].lower().split(" as ")[0]
                        + f" as {IP_Metadata.header_renames.get(item).lower()}"
                    )

            # qualify line columns
            if segment_id == "CIP00003":
                if item in IP_Metadata.line_renames.keys():
                    columns[i] = (
                        columns[i].lower().split(" as ")[0]
                        + f" as {IP_Metadata.line_renames.get(item).lower()}"
                    )

        return new_line_comma.join(columns)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def finalFormatter(output_columns):

        new_line_comma = "\n\t\t\t,"

        columns = output_columns.copy()

        return new_line_comma.join(columns)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def dates_of_service(colname: str, alias: str):
        return f"""
            case
                when {alias}.{colname} is not null then {alias}.{colname}
                else null
            end as SRVC_ENDG_DT_DRVD_H,
            case
                when {alias}.{colname} is not null then '1'
                else null
            end as SRVC_ENDG_DT_CD_H,
            coalesce({alias}.{colname}, '01JAN1960') as {colname}
        """

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def plan_id_num(colname: str, alias: str):
        return f"upper({alias}.{colname}) as mc_plan_id"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    cleanser = {
        "ADJDCTN_DT": TAF_Closure.coalesce_date,
        "DSCHRG_DT": dates_of_service,
        "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
        "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
        "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
        "ADMTG_DGNS_CD": TAF_Closure.compress_dots,
        "DGNS_1_CD": TAF_Closure.compress_dots,
        "DGNS_10_CD": TAF_Closure.compress_dots,
        "DGNS_11_CD": TAF_Closure.compress_dots,
        "DGNS_12_CD": TAF_Closure.compress_dots,
        "DGNS_2_CD": TAF_Closure.compress_dots,
        "DGNS_3_CD": TAF_Closure.compress_dots,
        "DGNS_4_CD": TAF_Closure.compress_dots,
        "DGNS_5_CD": TAF_Closure.compress_dots,
        "DGNS_6_CD": TAF_Closure.compress_dots,
        "DGNS_7_CD": TAF_Closure.compress_dots,
        "DGNS_8_CD": TAF_Closure.compress_dots,
        "DGNS_9_CD": TAF_Closure.compress_dots,
        "NCVRD_CHRGS_AMT": TAF_Closure.cast_as_dollar,
        "PLAN_ID_NUM": plan_id_num,
    }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    validator = {}

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    columns = {
        "CIP00001": [
            "TMSIS_RUN_ID",
            "TMSIS_FIL_NAME",
            "TMSIS_OFST_BYTE_NUM",
            "TMSIS_COMT_ID",
            "TMSIS_DLTD_IND",
            "TMSIS_OBSLT_IND",
            "TMSIS_ACTV_IND",
            "TMSIS_SQNC_NUM",
            "TMSIS_RUN_TS",
            "TMSIS_RPTG_PRD",
            "TMSIS_REC_MD5_B64_TXT",
            "REC_TYPE_CD",
            "DATA_DCTNRY_VRSN_NUM",
            "DATA_MPNG_DOC_VRSN_NUM",
            "FIL_CREATD_DT",
            "PRD_END_TIME",
            "FIL_ENCRPTN_SPEC_CD",
            "FIL_NAME",
            "FIL_STUS_CD",
            "SQNC_NUM",
            "PRD_EFCTV_TIME",
            "STATE_NOTN_TXT",
            "SUBMSN_TRANS_TYPE_CD",
            "SUBMTG_STATE_CD",
            "TOT_REC_CNT",
        ],
        "CIP00002": [
            "TMSIS_RUN_ID",
            "TMSIS_ACTV_IND",
            "SECT_1115A_DEMO_IND",
            "ADJDCTN_DT",
            "ADJSTMT_IND",
            "ADJSTMT_RSN_CD",
            "ADMSN_DT",
            "ADMSN_HR_NUM",
            "ADMSN_TYPE_CD",
            "ADMTG_DGNS_CD",
            "ADMTG_DGNS_CD_IND",
            "ADMTG_PRVDR_NPI_NUM",
            "ADMTG_PRVDR_NUM",
            "ADMTG_PRVDR_SPCLTY_CD",
            "ADMTG_PRVDR_TXNMY_CD",
            "ADMTG_PRVDR_TYPE_CD",
            "TOT_BENE_COINSRNC_PD_AMT",
            "TOT_BENE_COPMT_PD_AMT",
            "TOT_BENE_DDCTBL_PD_AMT",
            "BLG_PRVDR_NPI_NUM",
            "BLG_PRVDR_NUM",
            "BLG_PRVDR_SPCLTY_CD",
            "BLG_PRVDR_TXNMY_CD",
            "BLG_PRVDR_TYPE_CD",
            "BIRTH_WT_GRMS_QTY",
            "BRDR_STATE_IND",
            "CLL_CNT",
            "CLM_STUS_CD",
            "COPAY_WVD_IND",
            "XOVR_IND",
            "BIRTH_DT",
            "DGNS_1_CD",
            "DGNS_10_CD",
            "DGNS_11_CD",
            "DGNS_12_CD",
            "DGNS_2_CD",
            "DGNS_3_CD",
            "DGNS_4_CD",
            "DGNS_5_CD",
            "DGNS_6_CD",
            "DGNS_7_CD",
            "DGNS_8_CD",
            "DGNS_9_CD",
            "DGNS_1_CD_IND",
            "DGNS_10_CD_IND",
            "DGNS_11_CD_IND",
            "DGNS_12_CD_IND",
            "DGNS_2_CD_IND",
            "DGNS_3_CD_IND",
            "DGNS_4_CD_IND",
            "DGNS_5_CD_IND",
            "DGNS_6_CD_IND",
            "DGNS_7_CD_IND",
            "DGNS_8_CD_IND",
            "DGNS_9_CD_IND",
            "DGNS_POA_1_CD_IND",
            "DGNS_POA_10_CD_IND",
            "DGNS_POA_11_CD_IND",
            "DGNS_POA_12_CD_IND",
            "DGNS_POA_2_CD_IND",
            "DGNS_POA_3_CD_IND",
            "DGNS_POA_4_CD_IND",
            "DGNS_POA_5_CD_IND",
            "DGNS_POA_6_CD_IND",
            "DGNS_POA_7_CD_IND",
            "DGNS_POA_8_CD_IND",
            "DGNS_POA_9_CD_IND",
            "DRG_CD",
            "DRG_CD_IND",
            "DSCHRG_DT",
            "DSCHRG_HR_NUM",
            "DRG_DESC",
            "DRG_OUTLIER_AMT",
            "DRG_RLTV_WT_NUM",
            "ELGBL_1ST_NAME",
            "ELGBL_LAST_NAME",
            "ELGBL_MDL_INITL_NAME",
            "FIXD_PYMT_IND",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "HLTH_CARE_ACQRD_COND_CD",
            "ADJSTMT_CLM_NUM",
            "ORGNL_CLM_NUM",
            "MDCD_DSH_PD_AMT",
            "MDCD_CVRD_IP_DAYS_CNT",
            "MDCD_PD_DT",
            "MDCR_BENE_ID",
            "MDCR_CMBND_DDCTBL_IND",
            "MDCR_HICN_NUM",
            "MDCR_PD_AMT",
            "MDCR_REIMBRSMT_TYPE_CD",
            "MSIS_IDENT_NUM",
            "NCVRD_CHRGS_AMT",
            "NCVRD_DAYS_CNT",
            "OCRNC_01_CD",
            "OCRNC_02_CD",
            "OCRNC_03_CD",
            "OCRNC_04_CD",
            "OCRNC_05_CD",
            "OCRNC_06_CD",
            "OCRNC_07_CD",
            "OCRNC_08_CD",
            "OCRNC_09_CD",
            "OCRNC_10_CD",
            "OCRNC_01_CD_EFCTV_DT",
            "OCRNC_02_CD_EFCTV_DT",
            "OCRNC_03_CD_EFCTV_DT",
            "OCRNC_04_CD_EFCTV_DT",
            "OCRNC_05_CD_EFCTV_DT",
            "OCRNC_06_CD_EFCTV_DT",
            "OCRNC_07_CD_EFCTV_DT",
            "OCRNC_08_CD_EFCTV_DT",
            "OCRNC_09_CD_EFCTV_DT",
            "OCRNC_10_CD_EFCTV_DT",
            "OCRNC_01_CD_END_DT",
            "OCRNC_02_CD_END_DT",
            "OCRNC_03_CD_END_DT",
            "OCRNC_04_CD_END_DT",
            "OCRNC_05_CD_END_DT",
            "OCRNC_06_CD_END_DT",
            "OCRNC_07_CD_END_DT",
            "OCRNC_08_CD_END_DT",
            "OCRNC_09_CD_END_DT",
            "OCRNC_10_CD_END_DT",
            "OTHR_INSRNC_IND",
            "OTHR_TPL_CLCTN_CD",
            "OUTLIER_CD",
            "OUTLIER_DAYS_CNT",
            "PTNT_CNTL_NUM",
            "PTNT_STUS_CD",
            "PYMT_LVL_IND",
            "PLAN_ID_NUM",
            "PRCDR_1_CD",
            "PRCDR_2_CD",
            "PRCDR_3_CD",
            "PRCDR_4_CD",
            "PRCDR_5_CD",
            "PRCDR_6_CD",
            "PRCDR_1_CD_DT",
            "PRCDR_2_CD_DT",
            "PRCDR_3_CD_DT",
            "PRCDR_4_CD_DT",
            "PRCDR_5_CD_DT",
            "PRCDR_6_CD_DT",
            "PRCDR_1_CD_IND",
            "PRCDR_2_CD_IND",
            "PRCDR_3_CD_IND",
            "PRCDR_4_CD_IND",
            "PRCDR_5_CD_IND",
            "PRCDR_6_CD_IND",
            "PGM_TYPE_CD",
            "PRVDR_LCTN_ID",
            "RFRG_PRVDR_NPI_NUM",
            "RFRG_PRVDR_NUM",
            "RFRG_PRVDR_SPCLTY_CD",
            "RFRG_PRVDR_TXNMY_CD",
            "RFRG_PRVDR_TYPE_CD",
            "SRVC_TRKNG_PYMT_AMT",
            "SRVC_TRKNG_TYPE_CD",
            "SPLIT_CLM_IND",
            "SUBMTG_STATE_CD",
            "TOT_ALOWD_AMT",
            "TOT_BILL_AMT",
            "TOT_COPAY_AMT",
            "TOT_MDCD_PD_AMT",
            "TOT_MDCR_COINSRNC_AMT",
            "TOT_MDCR_DDCTBL_AMT",
            "TOT_OTHR_INSRNC_AMT",
            "TOT_TPL_AMT",
            "BILL_TYPE_CD",
            "CLM_TYPE_CD",
            "HOSP_TYPE_CD",
            "WVR_ID",
            "WVR_TYPE_CD",
            "TP_COINSRNC_PD_AMT",
            "TP_COPMT_PD_AMT",
        ],
        "CIP00003": [
            "TMSIS_FIL_NAME",
            "REC_NUM",
            "TMSIS_RUN_ID",
            "TMSIS_ACTV_IND",
            "ADJDCTN_DT",
            "SRVC_BGNNG_DT",
            "BNFT_TYPE_CD",
            "CLL_STUS_CD",
            "CMS_64_FED_REIMBRSMT_CTGRY_CD",
            "SRVC_ENDG_DT",
            "HCPCS_RATE",
            "ADJSTMT_CLM_NUM",
            "ORGNL_CLM_NUM",
            "IMNZTN_TYPE_CD",
            "RC_QTY_ACTL",
            "RC_QTY_ALOWD",
            "LINE_ADJSTMT_IND",
            "ADJSTMT_LINE_NUM",
            "ORGNL_LINE_NUM",
            "ALOWD_AMT",
            "MDCD_FFS_EQUIV_AMT",
            "MSIS_IDENT_NUM",
            "NDC_CD",
            "NDC_QTY",
            "NDC_UOM_CD",
            "OPRTG_PRVDR_NPI_NUM",
            "PRVDR_FAC_TYPE_CD",
            "REV_CHRG_AMT",
            "REV_CD",
            "PRSCRBNG_PRVDR_NPI_NUM",
            "SRVCNG_PRVDR_NUM",
            "SRVCNG_PRVDR_SPCLTY_CD",
            "SRVCNG_PRVDR_TXNMY_CD",
            "SRVCNG_PRVDR_TYPE_CD",
            "SUBMTG_STATE_CD",
            "STC_CD",
            "XIX_SRVC_CTGRY_CD",
            "XXI_SRVC_CTGRY_CD",
            "MDCD_PD_AMT",
            "OTHR_INSRNC_AMT",
        ],
    }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    coalesce = {
        "ADJDCTN_DT": "01JAN1960",
        "ADJDCTN_DT": "01JAN1960",
        "DSCHRG_DT": "01JAN1960",
        "SRVC_BGNNG_DT": "01JAN1960",
        "SRVC_ENDG_DT": "01JAN1960",
        "ADJSTMT_CLM_NUM": "~",
        "ADJSTMT_CLM_NUM": "~",
        "ADJSTMT_IND": "X",
        "LINE_ADJSTMT_IND": "X",
        "ORGNL_CLM_NUM": "~",
        "ORGNL_CLM_NUM": "~",
    }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    compress_dot = {
        "ADMTG_DGNS_CD",
        "DGNS_1_CD",
        "DGNS_10_CD",
        "DGNS_11_CD",
        "DGNS_12_CD",
        "DGNS_2_CD",
        "DGNS_3_CD",
        "DGNS_4_CD",
        "DGNS_5_CD",
        "DGNS_6_CD",
        "DGNS_7_CD",
        "DGNS_8_CD",
        "DGNS_9_CD",
    }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    header_renames = {}

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    line_renames = {
        "SUBMTG_STATE_CD": "SUBMTG_STATE_CD_LINE",
        "MSIS_IDENT_NUM": "MSIS_IDENT_NUM_LINE",
        "TMSIS_RUN_ID": "TMSIS_RUN_ID_LINE",
        "TMSIS_ACTV_IND": "TMSIS_ACTV_IND_LINE",
        "ADJDCTN_DT": "ADJDCTN_DT_LINE",
        "ADJSTMT_CLM_NUM": "ADJSTMT_CLM_NUM_LINE",
        "ORGNL_CLM_NUM": "ORGNL_CLM_NUM_LINE",
        "STC_CD": "TOS_CD",
        "NDC_UOM_CD": "UOM_CD",
    }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    upper = [
        "ADJSTMT_LINE_NUM",
        "ADJSTMT_RSN_CD",
        "ADMSN_HR_NUM",
        "ADMSN_TYPE_CD",
        "ADMTG_DGNS_CD_IND",
        "ADMTG_PRVDR_NPI_NUM",
        "ADMTG_PRVDR_NUM",
        "ADMTG_PRVDR_SPCLTY_CD",
        "ADMTG_PRVDR_TXNMY_CD",
        "ADMTG_PRVDR_TYPE_CD",
        "BILL_TYPE_CD",
        "BLG_PRVDR_NPI_NUM",
        "BLG_PRVDR_NUM",
        "BLG_PRVDR_SPCLTY_CD",
        "BLG_PRVDR_TXNMY_CD",
        "BLG_PRVDR_TYPE_CD",
        "BNFT_TYPE_CD",
        "BRDR_STATE_IND",
        "CLL_STUS_CD",
        "CLM_STUS_CD",
        "CLM_TYPE_CD",
        "CMS_64_FED_REIMBRSMT_CTGRY_CD",
        "COPAY_WVD_IND",
        "DGNS_1_CD_IND",
        "DGNS_10_CD_IND",
        "DGNS_11_CD_IND",
        "DGNS_12_CD_IND",
        "DGNS_2_CD_IND",
        "DGNS_3_CD_IND",
        "DGNS_4_CD_IND",
        "DGNS_5_CD_IND",
        "DGNS_6_CD_IND",
        "DGNS_7_CD_IND",
        "DGNS_8_CD_IND",
        "DGNS_9_CD_IND",
        "DGNS_POA_1_CD_IND",
        "DGNS_POA_10_CD_IND",
        "DGNS_POA_11_CD_IND",
        "DGNS_POA_12_CD_IND",
        "DGNS_POA_2_CD_IND",
        "DGNS_POA_3_CD_IND",
        "DGNS_POA_4_CD_IND",
        "DGNS_POA_5_CD_IND",
        "DGNS_POA_6_CD_IND",
        "DGNS_POA_7_CD_IND",
        "DGNS_POA_8_CD_IND",
        "DGNS_POA_9_CD_IND",
        "DRG_CD_IND",
        "DRG_CD",
        "DRG_DESC",
        "DSCHRG_HR_NUM",
        "ELGBL_1ST_NAME",
        "ELGBL_LAST_NAME",
        "ELGBL_MDL_INITL_NAME",
        "FIXD_PYMT_IND",
        "FUNDNG_CD",
        "FUNDNG_SRC_NON_FED_SHR_CD",
        "HCPCS_RATE",
        "HLTH_CARE_ACQRD_COND_CD",
        "HOSP_TYPE_CD",
        "IMNZTN_TYPE_CD",
        "XIX_SRVC_CTGRY_CD",
        "XXI_SRVC_CTGRY_CD",
        "MDCR_BENE_ID",
        "MDCR_CMBND_DDCTBL_IND",
        "MDCR_HICN_NUM",
        "MDCR_REIMBRSMT_TYPE_CD",
        "MSIS_IDENT_NUM",
        "MSIS_IDENT_NUM",
        "NDC_CD",
        "NDC_UOM_CD uom_cd",
        "OCRNC_01_CD",
        "OCRNC_02_CD",
        "OCRNC_03_CD",
        "OCRNC_04_CD",
        "OCRNC_05_CD",
        "OCRNC_06_CD",
        "OCRNC_07_CD",
        "OCRNC_08_CD",
        "OCRNC_09_CD",
        "OCRNC_10_CD",
        "OPRTG_PRVDR_NPI_NUM",
        "ORGNL_LINE_NUM",
        "OTHR_INSRNC_IND",
        "OTHR_TPL_CLCTN_CD",
        "OUTLIER_CD",
        "PGM_TYPE_CD",
        "PLAN_ID_NUM",
        "PRCDR_1_CD_IND",
        "PRCDR_1_CD",
        "PRCDR_2_CD_IND",
        "PRCDR_2_CD",
        "PRCDR_3_CD_IND",
        "PRCDR_3_CD",
        "PRCDR_4_CD_IND",
        "PRCDR_4_CD",
        "PRCDR_5_CD_IND",
        "PRCDR_5_CD",
        "PRCDR_6_CD_IND",
        "PRCDR_6_CD",
        "PRSCRBNG_PRVDR_NPI_NUM",
        "PRVDR_FAC_TYPE_CD",
        "PRVDR_LCTN_ID",
        "PTNT_CNTL_NUM",
        "PTNT_STUS_CD",
        "PYMT_LVL_IND",
        "REV_CD",
        "RFRG_PRVDR_NPI_NUM",
        "RFRG_PRVDR_NUM",
        "RFRG_PRVDR_SPCLTY_CD",
        "RFRG_PRVDR_TXNMY_CD",
        "RFRG_PRVDR_TYPE_CD",
        "SECT_1115A_DEMO_IND",
        "SPLIT_CLM_IND",
        "SRVC_TRKNG_TYPE_CD",
        "SRVCNG_PRVDR_NUM",
        "SRVCNG_PRVDR_SPCLTY_CD",
        "SRVCNG_PRVDR_TXNMY_CD",
        "SRVCNG_PRVDR_TYPE_CD",
        "STC_CD tos_cd",
        "TMSIS_FIL_NAME",
        "WVR_ID",
        "WVR_TYPE_CD",
        "XOVR_IND",
    ]

    # ---------------------------------------------------------------------------------
    #
    #   CREATE TEMP TABLE IPH
    #
    #
    # ---------------------------------------------------------------------------------
    header_columns = []

    # ---------------------------------------------------------------------------------
    #
    #   CREATE TEMP TABLE IPL
    #
    #
    # ---------------------------------------------------------------------------------
    line_columns = []

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    @staticmethod
    def finalTableOutputHeader():

        # INSERT INTO {self.runner.DA_SCHEMA}.TAF_IPL
        z = """
                insert into taf_python.taf_iph
                select
                    DA_RUN_ID
                   ,IP_LINK_KEY
                   ,IP_VRSN
                   ,IP_FIL_DT
                   ,TMSIS_RUN_ID
                   ,MSIS_IDENT_NUM
                   ,SUBMTG_STATE_CD
                   ,ORGNL_CLM_NUM
                   ,ADJSTMT_CLM_NUM
                   ,ADJSTMT_IND
                   ,ADJSTMT_RSN_CD
                   ,ADMSN_DT
                   ,ADMSN_HR_NUM
                   ,DSCHRG_DT
                   ,DSCHRG_HR_NUM
                   ,ADJDCTN_DT
                   ,MDCD_PD_DT
                   ,ADMSN_TYPE_CD
                   ,HOSP_TYPE_CD
                   ,SECT_1115A_DEMO_IND
                   ,CLM_TYPE_CD
                   ,BILL_TYPE_CD
                   ,PGM_TYPE_CD
                   ,MC_PLAN_ID
                   ,ELGBL_LAST_NAME
                   ,ELGBL_1ST_NAME
                   ,ELGBL_MDL_INITL_NAME
                   ,BIRTH_DT
                   ,WVR_TYPE_CD
                   ,WVR_ID
                   ,SRVC_TRKNG_TYPE_CD
                   ,SRVC_TRKNG_PYMT_AMT
                   ,OTHR_INSRNC_IND
                   ,OTHR_TPL_CLCTN_CD
                   ,FIXD_PYMT_IND
                   ,FUNDNG_CD
                   ,FUNDNG_SRC_NON_FED_SHR_CD
                   ,BRDR_STATE_IND
                   ,XOVR_IND
                   ,MDCR_HICN_NUM
                   ,MDCR_BENE_ID
                   ,PTNT_CNTL_NUM
                   ,HLTH_CARE_ACQRD_COND_CD
                   ,PTNT_STUS_CD
                   ,BIRTH_WT_GRMS_QTY
                   ,ADMTG_DGNS_CD
                   ,ADMTG_DGNS_CD_IND
                   ,DGNS_1_CD
                   ,DGNS_1_CD_IND
                   ,DGNS_POA_1_CD_IND
                   ,DGNS_2_CD
                   ,DGNS_2_CD_IND
                   ,DGNS_POA_2_CD_IND
                   ,DGNS_3_CD
                   ,DGNS_3_CD_IND
                   ,DGNS_POA_3_CD_IND
                   ,DGNS_4_CD
                   ,DGNS_4_CD_IND
                   ,DGNS_POA_4_CD_IND
                   ,DGNS_5_CD
                   ,DGNS_5_CD_IND
                   ,DGNS_POA_5_CD_IND
                   ,DGNS_6_CD
                   ,DGNS_6_CD_IND
                   ,DGNS_POA_6_CD_IND
                   ,DGNS_7_CD
                   ,DGNS_7_CD_IND
                   ,DGNS_POA_7_CD_IND
                   ,DGNS_8_CD
                   ,DGNS_8_CD_IND
                   ,DGNS_POA_8_CD_IND
                   ,DGNS_9_CD
                   ,DGNS_9_CD_IND
                   ,DGNS_POA_9_CD_IND
                   ,DGNS_10_CD
                   ,DGNS_10_CD_IND
                   ,DGNS_POA_10_CD_IND
                   ,DGNS_11_CD
                   ,DGNS_11_CD_IND
                   ,DGNS_POA_11_CD_IND
                   ,DGNS_12_CD
                   ,DGNS_12_CD_IND
                   ,DGNS_POA_12_CD_IND
                   ,DRG_CD
                   ,DRG_CD_IND
                   ,DRG_DESC
                   ,PRCDR_1_CD_DT
                   ,PRCDR_1_CD
                   ,PRCDR_1_CD_IND
                   ,PRCDR_2_CD_DT
                   ,PRCDR_2_CD
                   ,PRCDR_2_CD_IND
                   ,PRCDR_3_CD_DT
                   ,PRCDR_3_CD
                   ,PRCDR_3_CD_IND
                   ,PRCDR_4_CD_DT
                   ,PRCDR_4_CD
                   ,PRCDR_4_CD_IND
                   ,PRCDR_5_CD_DT
                   ,PRCDR_5_CD
                   ,PRCDR_5_CD_IND
                   ,PRCDR_6_CD_DT
                   ,PRCDR_6_CD
                   ,PRCDR_6_CD_IND
                   ,NCVRD_DAYS_CNT
                   ,NCVRD_CHRGS_AMT
                   ,MDCD_CVRD_IP_DAYS_CNT
                   ,OUTLIER_DAYS_CNT
                   ,OUTLIER_CD
                   ,ADMTG_PRVDR_NPI_NUM
                   ,ADMTG_PRVDR_NUM
                   ,ADMTG_PRVDR_SPCLTY_CD
                   ,ADMTG_PRVDR_TXNMY_CD
                   ,ADMTG_PRVDR_TYPE_CD
                   ,BLG_PRVDR_NUM
                   ,BLG_PRVDR_NPI_NUM
                   ,BLG_PRVDR_TXNMY_CD
                   ,BLG_PRVDR_TYPE_CD
                   ,BLG_PRVDR_SPCLTY_CD
                   ,RFRG_PRVDR_NUM
                   ,RFRG_PRVDR_NPI_NUM
                   ,RFRG_PRVDR_TYPE_CD
                   ,RFRG_PRVDR_SPCLTY_CD
                   ,PRVDR_LCTN_ID
                   ,PYMT_LVL_IND
                   ,TOT_BILL_AMT
                   ,TOT_ALOWD_AMT
                   ,TOT_MDCD_PD_AMT
                   ,TOT_COPAY_AMT
                   ,TOT_TPL_AMT
                   ,TOT_OTHR_INSRNC_AMT
                   ,TP_COINSRNC_PD_AMT
                   ,TP_COPMT_PD_AMT
                   ,MDCD_DSH_PD_AMT
                   ,DRG_OUTLIER_AMT
                   ,DRG_RLTV_WT_NUM
                   ,MDCR_PD_AMT
                   ,TOT_MDCR_DDCTBL_AMT
                   ,TOT_MDCR_COINSRNC_AMT
                   ,MDCR_CMBND_DDCTBL_IND
                   ,MDCR_REIMBRSMT_TYPE_CD
                   ,BENE_COINSRNC_AMT
                   ,BENE_COPMT_AMT
                   ,BENE_DDCTBL_AMT
                   ,COPAY_WVD_IND
                   ,OCRNC_01_CD_EFCTV_DT
                   ,OCRNC_01_CD_END_DT
                   ,OCRNC_01_CD
                   ,OCRNC_02_CD_EFCTV_DT
                   ,OCRNC_02_CD_END_DT
                   ,OCRNC_02_CD
                   ,OCRNC_03_CD_EFCTV_DT
                   ,OCRNC_03_CD_END_DT
                   ,OCRNC_03_CD
                   ,OCRNC_04_CD_EFCTV_DT
                   ,OCRNC_04_CD_END_DT
                   ,OCRNC_04_CD
                   ,OCRNC_05_CD_EFCTV_DT
                   ,OCRNC_05_CD_END_DT
                   ,OCRNC_05_CD
                   ,OCRNC_06_CD_EFCTV_DT
                   ,OCRNC_06_CD_END_DT
                   ,OCRNC_06_CD
                   ,OCRNC_07_CD_EFCTV_DT
                   ,OCRNC_07_CD_END_DT
                   ,OCRNC_07_CD
                   ,OCRNC_08_CD_EFCTV_DT
                   ,OCRNC_08_CD_END_DT
                   ,OCRNC_08_CD
                   ,OCRNC_09_CD_EFCTV_DT
                   ,OCRNC_09_CD_END_DT
                   ,OCRNC_09_CD
                   ,OCRNC_10_CD_EFCTV_DT
                   ,OCRNC_10_CD_END_DT
                   ,OCRNC_10_CD
                   ,SPLIT_CLM_IND
                   ,CLL_CNT
                   ,NUM_CLL
                   ,IP_MH_DX_IND
                   ,IP_SUD_DX_IND
                   ,IP_MH_TXNMY_IND
                   ,IP_SUD_TXNMY_IND
                   ,MAJ_DGNSTC_CTGRY
                   ,IAP_COND_IND
                   ,PRMRY_HIRCHCL_COND
                   ,REC_ADD_TS
                   ,REC_UPDT_TS
                   ,SRVC_ENDG_DT_DRVD
                   ,SRVC_ENDG_DT_CD
                from IPH
        """

        return z

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    @staticmethod
    def finalTableOutputLine():

        # INSERT INTO {self.runner.DA_SCHEMA}.TAF_IPL
        z = """
                insert into taf_python.taf_ipl
                select
                      DA_RUN_ID
                    , IP_LINK_KEY
                    , IP_VRSN
                    , IP_FIL_DT
                    , TMSIS_RUN_ID
                    , MSIS_IDENT_NUM
                    , SUBMTG_STATE_CD
                    , ORGNL_CLM_NUM
                    , ADJSTMT_CLM_NUM
                    , ORGNL_LINE_NUM
                    , ADJSTMT_LINE_NUM
                    , ADJDCTN_DT
                    , LINE_ADJSTMT_IND
                    , TOS_CD
                    , IMNZTN_TYPE_CD
                    , CMS_64_FED_REIMBRSMT_CTGRY_CD
                    , XIX_SRVC_CTGRY_CD
                    , XXI_SRVC_CTGRY_CD
                    , CLL_STUS_CD
                    , SRVC_BGNNG_DT
                    , SRVC_ENDG_DT
                    , BNFT_TYPE_CD
                    , REV_CD
                    , ACTL_SRVC_QTY
                    , ALOWD_SRVC_QTY
                    , REV_CHRG_AMT
                    , SRVCNG_PRVDR_NUM
                    , SRVCNG_PRVDR_NPI_NUM
                    , SRVCNG_PRVDR_TXNMY_CD
                    , SRVCNG_PRVDR_TYPE_CD
                    , SRVCNG_PRVDR_SPCLTY_CD
                    , OPRTG_PRVDR_NPI_NUM
                    , PRVDR_FAC_TYPE_CD
                    , NDC_QTY
                    , HCPCS_RATE
                    , NDC_CD
                    , UOM_CD
                    , ALOWD_AMT
                    , MDCD_PD_AMT
                    , OTHR_INSRNC_AMT
                    , MDCD_FFS_EQUIV_AMT
                    , LINE_NUM
                from ipl
        """

        return z

    # ---------------------------------------------------------------------------------
    #
    #   INSERT INTO &DA_SCHEMA..TAF_&FL.L
    #
    #
    # ---------------------------------------------------------------------------------
    output_columns = [
        "da_run_id",
        "ip_link_key",
        "ip_vrsn",
        "ip_fil_dt",
        "tmsis_run_id",
        "msis_ident_num",
        "submtg_state_cd",
        "orgnl_clm_num",
        "adjstmt_clm_num",
        "orgnl_line_num",
        "adjstmt_line_num",
        "adjdctn_dt",
        "line_adjstmt_ind",
        "tos_cd",
        "imnztn_type_cd",
        "cms_64_fed_reimbrsmt_ctgry_cd",
        "xix_srvc_ctgry_cd",
        "xxi_srvc_ctgry_cd",
        "cll_stus_cd",
        "srvc_bgnng_dt",
        "srvc_endg_dt",
        "bnft_type_cd",
        "rev_cd",
        "actl_srvc_qty",
        "alowd_srvc_qty",
        "rev_chrg_amt",
        "srvcng_prvdr_num",
        "srvcng_prvdr_npi_num",
        "srvcng_prvdr_txnmy_cd",
        "srvcng_prvdr_type_cd",
        "srvcng_prvdr_spclty_cd",
        "oprtg_prvdr_npi_num",
        "prvdr_fac_type_cd",
        "ndc_qty",
        "hcpcs_rate",
        "ndc_cd",
        "uom_cd",
        "alowd_amt",
        "mdcd_pd_amt",
        "othr_insrnc_amt",
        "mdcd_ffs_equiv_amt",
        "line_num",
    ]


# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
