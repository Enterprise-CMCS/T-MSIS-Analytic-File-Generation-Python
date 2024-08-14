from taf.TAF_Closure import TAF_Closure


class OT_Metadata:
    """
    Create OT metadata.
    """

    def selectDataElements(segment_id: str, alias: str):
        """
        Function to select data elements.  Selected data elements will be cleansed, checked against a validator,
        and masked if there are invalid values.
        """

        lower_segment_id = segment_id.casefold()

        cleanser_to_use = OT_Metadata.cleanser.copy()
        if lower_segment_id == 'cot00003':
          cleanser_to_use['SRVC_ENDG_DT'] = OT_Metadata.line_dates_of_service

        new_line_comma = "\n\t\t\t,"

        columns = OT_Metadata.columns.get(segment_id).copy()

        for i, item in enumerate(columns):
            if item in cleanser_to_use.keys():
                columns[i] = cleanser_to_use[item](item, alias)
            elif item in OT_Metadata.validator.keys(): # This is empty so it will not currently run. Leaving in for placeholder
                columns[i] = OT_Metadata.maskInvalidValues(item, alias)
            elif item in OT_Metadata.upper:
                columns[i] = f"upper({alias}.{item}) as {str(item).lower()}"
            else:
                columns[i] = f"{alias}.{columns[i]}"

            # qualify header columns
            if segment_id.casefold() == "cot00002":
                if item in OT_Metadata.header_renames.keys():
                    columns[i] = (
                        columns[i].lower().split(" as ")[0]
                        + f" as {OT_Metadata.header_renames.get(item).lower()}"
                    )

            # qualify line columns
            if segment_id.casefold() == "cot00003":
                if item in OT_Metadata.line_renames.keys():
                    if item in OT_Metadata.upper:
                        columns[i] = (
                            "upper(" + columns[i].lower().split(" as ")[0]
                            + f") as {OT_Metadata.line_renames.get(item).lower()}"
                        )
                    else:
                        columns[i] = (
                            columns[i].lower().split(" as ")[0]
                            + f" as {OT_Metadata.line_renames.get(item).lower()}"
                        )

        return new_line_comma.join(columns)

    def finalFormatter(output_columns):
        """
        Function for final formatting.
        """

        new_line_comma = "\n\t\t\t,"

        columns: list = output_columns.copy()
        rpl_columns: list = []

        # need to check for whether these fields are part of the upper list
        # which means they are required to be uppercase for safety
        for fld in columns:
            if fld in OT_Metadata.upper:
                rpl_columns.append(f"upper({fld})")
            else:
                rpl_columns.append(fld)

        return new_line_comma.join(rpl_columns)

    def dates_of_service(colname: str, alias: str):
        """
        Return dates of service.  If column name is null, then set date to 1960-01-01.
        """

        return f"""
            coalesce({alias}.{colname}, {alias}.SRVC_BGNNG_DT) as SRVC_ENDG_DT_DRVD_H,
            case
                when {alias}.{colname} is not null then '2'
                when {alias}.{colname} is null and {alias}.SRVC_BGNNG_DT is not null then '3'
                else null
            end as SRVC_ENDG_DT_CD_H,
            coalesce({alias}.{colname}, '1960-01-01') as {colname}
        """

    def line_dates_of_service(colname: str, alias: str):
        """
        Return dates of service to use with segment COT00003
        During the line_rename part, the second part after the " as "
        will be stripped
        """

        return f"""
            coalesce({alias}.{colname}, to_date('1960-01-01')) as {colname}
        """

    cleanser = {
        "ADJDCTN_DT": TAF_Closure.coalesce_date,
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
        "LINE_ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
        "NCVRD_CHRGS_AMT": TAF_Closure.cast_as_dollar,
        "SRVC_ENDG_DT": dates_of_service,
        "XIX_SRVC_CTGRY_CD": TAF_Closure.cleanXIX_SRVC_CTGRY_CD,
        "XXI_SRVC_CTGRY_CD": TAF_Closure.cleanXXI_SRVC_CTGRY_CD,
        "COPAY_WVD_IND":TAF_Closure.set_as_null,
        "RFRG_PRVDR_TXNMY_CD":TAF_Closure.set_as_null,
        "RFRG_PRVDR_SPCLTY_CD":TAF_Closure.set_as_null,
        "RFRG_PRVDR_TYPE_CD":TAF_Closure.set_as_null,
        "PRVDR_UNDER_SPRVSN_TXNMY_CD":TAF_Closure.set_as_null,
        "PRVDR_UNDER_DRCTN_TXNMY_CD":TAF_Closure.set_as_null,
        "PRVDR_UNDER_DRCTN_NPI_NUM":TAF_Closure.set_as_null,
        "CPTATD_AMT_RQSTD_DT":TAF_Closure.set_as_null,
        "HCPCS_RATE":TAF_Closure.set_as_null,
        "CPTATD_PYMT_RQSTD_AMT":TAF_Closure.set_as_null,
        "TOT_COPAY_AMT":TAF_Closure.set_as_null
    }

    validator = {}

    columns = {
        "COT00001": [
            "TMSIS_RUN_ID",
            "TMSIS_FIL_NAME",
            "TMSIS_ACTV_IND",
            "FIL_CREATD_DT",
            "PRD_END_TIME",
            "FIL_NAME",
            "FIL_STUS_CD",
            "SQNC_NUM",
            "PRD_EFCTV_TIME",
            "SUBMTG_STATE_CD",
            "TOT_REC_CNT",
        ],
        "COT00002": [
            "TMSIS_RUN_ID",
            "TMSIS_ACTV_IND",
            "SECT_1115A_DEMO_IND",
            "ADJDCTN_DT",
            "ADJSTMT_IND",
            "ADJSTMT_RSN_CD",
            "SRVC_BGNNG_DT",
            "SRVC_BGNNG_DT as SRVC_BGNNG_DT_HEADER",
            "TOT_BENE_COINSRNC_PD_AMT",
            "BENE_COINSRNC_PD_DT",
            "TOT_BENE_COPMT_PD_AMT",
            "BENE_COPMT_PD_DT",
            "TOT_BENE_DDCTBL_PD_AMT",
            "BENE_DDCTBL_PD_DT",
            "BLG_PRVDR_NPI_NUM",
            "BLG_PRVDR_NUM",
            "BLG_PRVDR_SPCLTY_CD",
            "BLG_PRVDR_TXNMY_CD",
            "BLG_PRVDR_TYPE_CD",
            "BRDR_STATE_IND",
            "CPTATD_PYMT_RQSTD_AMT",
            "CLM_DND_IND",
            "CLL_CNT",
            "CLM_STUS_CD",
            "COPAY_WVD_IND",
            "XOVR_IND",
            "DAILY_RATE",
            "CPTATD_AMT_RQSTD_DT",
            "BIRTH_DT",
            "DGNS_1_CD",
            "DGNS_2_CD",
            "DGNS_1_CD_IND",
            "DGNS_2_CD_IND",
            "DGNS_POA_1_CD_IND",
            "DGNS_POA_2_CD_IND",
            "ELGBL_1ST_NAME",
            "ELGBL_LAST_NAME",
            "ELGBL_MDL_INITL_NAME",
            "SRVC_ENDG_DT",
            "SRVC_ENDG_DT as SRVC_ENDG_DT_HEADER",
            "FIXD_PYMT_IND",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "HLTH_CARE_ACQRD_COND_CD",
            "HH_ENT_NAME",
            "HH_PRVDR_IND",
            "HH_PRVDR_NPI_NUM",
            "ADJSTMT_CLM_NUM",
            "ORGNL_CLM_NUM",
            "MDCD_PD_DT",
            "MDCR_BENE_ID",
            "MDCR_CMBND_DDCTBL_IND",
            "MDCR_HICN_NUM",
            "MDCR_REIMBRSMT_TYPE_CD",
            "MSIS_IDENT_NUM",
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
            "PYMT_LVL_IND",
            "SRVC_PLC_CD",
            "PLAN_ID_NUM",
            "PGM_TYPE_CD",
            "PRVDR_LCTN_ID",
            "RFRG_PRVDR_NPI_NUM",
            "RFRG_PRVDR_NUM",
            "RFRG_PRVDR_SPCLTY_CD",
            "RFRG_PRVDR_TXNMY_CD",
            "RFRG_PRVDR_TYPE_CD",
            "RMTNC_NUM",
            "SRVC_TRKNG_PYMT_AMT",
            "SRVC_TRKNG_TYPE_CD",
            "SUBMTG_STATE_CD",
            "TOT_MDCR_COINSRNC_AMT",
            "TOT_MDCR_DDCTBL_AMT",
            "BILL_TYPE_CD",
            "CLM_TYPE_CD",
            "WVR_ID",
            "WVR_TYPE_CD",
            "PRVDR_UNDER_DRCTN_NPI_NUM",
            "PRVDR_UNDER_DRCTN_TXNMY_CD",
            "PRVDR_UNDER_SPRVSN_NPI_NUM",
            "PRVDR_UNDER_SPRVSN_TXNMY_CD",
            "TP_COINSRNC_PD_AMT",
            "TP_COINSRNC_PD_DT",
            "TP_COPMT_PD_AMT",
            "TP_COPMT_PD_DT",
            "TOT_ALOWD_AMT",
            "TOT_BILL_AMT",
            "TOT_COPAY_AMT",
            "TOT_MDCD_PD_AMT",
            "TOT_OTHR_INSRNC_AMT",
            "TOT_TPL_AMT"
        ],
        "COT00003": [
            "TMSIS_FIL_NAME",
            "REC_NUM",
            "TMSIS_RUN_ID",
            "ADJDCTN_DT",
            "ALOWD_AMT",
            "SRVC_BGNNG_DT",
            "BNFT_TYPE_CD",
            "BILL_AMT",
            "CLL_STUS_CD",
            "CMS_64_FED_REIMBRSMT_CTGRY_CD",
            "BENE_COPMT_PD_AMT",
            "SRVC_ENDG_DT",
            "HCPCS_SRVC_CD",
            "HCPCS_TXNMY_CD",
            "HCPCS_RATE",
            "ADJSTMT_CLM_NUM",
            "ORGNL_CLM_NUM",
            "IMNZTN_TYPE_CD",
            "LINE_ADJSTMT_IND",
            "ADJSTMT_LINE_RSN_CD",
            "ADJSTMT_LINE_NUM",
            "ORGNL_LINE_NUM",
            "MDCD_FFS_EQUIV_AMT",
            "MDCD_PD_AMT",
            "MDCR_PD_AMT",
            "MSIS_IDENT_NUM",
            "NDC_CD",
            "NDC_QTY",
            "NDC_UOM_CD",
            "OTHR_TOC_RX_CLM_ACTL_QTY",
            "OTHR_TOC_RX_CLM_ALOWD_QTY",
            "OTHR_INSRNC_AMT",
            "PRE_AUTHRZTN_NUM",
            "PRCDR_CD",
            "PRCDR_CD_DT",
            "PRCDR_CD_IND",
            "PRCDR_1_MDFR_CD",
            "PRCDR_2_MDFR_CD",
            "PRCDR_3_MDFR_CD",
            "PRCDR_4_MDFR_CD",
            "REV_CD",
            "SELF_DRCTN_TYPE_CD",
            "PRSCRBNG_PRVDR_NPI_NUM",
            "SRVCNG_PRVDR_NUM",
            "SRVCNG_PRVDR_SPCLTY_CD",
            "SRVCNG_PRVDR_TXNMY_CD",
            "SRVCNG_PRVDR_TYPE_CD",
            "STATE_NOTN_TXT",
            "SUBMTG_STATE_CD",
            "TOOTH_DSGNTN_SYS_CD",
            "TOOTH_NUM",
            "TOOTH_ORAL_CVTY_AREA_DSGNTD_CD",
            "TOOTH_SRFC_CD",
            "TPL_AMT",
            "STC_CD",
            "XIX_SRVC_CTGRY_CD",
            "XXI_SRVC_CTGRY_CD",
        ],
    }

    class OTH:

        columns = []

        renames = {
            "NEW_SUBMTG_STATE_CD": "SUBMTG_STATE_CD",
        }

    coalesce = {
        "ADJDCTN_DT": "1960-01-01",
        "ADJDCTN_DT": "1960-01-01",
        "DSCHRG_DT": "1960-01-01",
        "SRVC_BGNNG_DT": "1960-01-01",
        "SRVC_ENDG_DT": "1960-01-01",
        "ADJSTMT_CLM_NUM": "~",
        "ADJSTMT_CLM_NUM": "~",
        "ADJSTMT_IND": "X",
        "LINE_ADJSTMT_IND": "X",
        "ORGNL_CLM_NUM": "~",
        "ORGNL_CLM_NUM": "~",
    }

    compress_dot = {}

    upper = [
        "ADJSTMT_LINE_NUM",
        "ADJSTMT_LINE_RSN_CD",
        "ADJSTMT_RSN_CD",
        "ADMSN_HR_NUM",
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
        "BLG_UNIT_CD",
        "BNFT_TYPE_CD",
        "BRDR_STATE_IND",
        "CHK_NUM",
        "CLL_STUS_CD",
        "CLM_DND_IND",
        "CLM_PYMT_REMIT_1_CD",
        "CLM_PYMT_REMIT_2_CD",
        "CLM_PYMT_REMIT_3_CD",
        "CLM_PYMT_REMIT_4_CD",
        "CLM_STUS_CD",
        "CLM_STUS_CTGRY_CD",
        "CLM_TYPE_CD",
        "CMS_64_FED_REIMBRSMT_CTGRY_CD",
        "DGNS_1_CD_IND",
        "DGNS_2_CD_IND",
        "DGNS_3_CD_IND",
        "DGNS_4_CD_IND",
        "DGNS_5_CD_IND",
        "DGNS_POA_1_CD_IND",
        "DGNS_POA_2_CD_IND",
        "DGNS_POA_3_CD_IND",
        "DGNS_POA_4_CD_IND",
        "DGNS_POA_5_CD_IND",
        "DSCHRG_HR_NUM",
        "ELGBL_1ST_NAME",
        "ELGBL_LAST_NAME",
        "ELGBL_MDL_INITL_NAME",
        "FIXD_PYMT_IND",
        "FRCD_CLM_CD",
        "FUNDNG_CD",
        "FUNDNG_SRC_NON_FED_SHR_CD",
        "HCBS_SRVC_CD",
        "HCBS_TXNMY",
        "HH_ENT_NAME",
        "HH_PRVDR_IND",
        "HH_PRVDR_NPI_NUM",
        "HLTH_CARE_ACQRD_COND_CD",
        "IMNZTN_TYPE_CD",
        "MDCR_BENE_ID",
        "MDCR_CMBND_DDCTBL_IND",
        "MDCR_HICN_NUM",
        "MDCR_REIMBRSMT_TYPE_CD",
        "MSIS_IDENT_NUM",
        "MSIS_IDENT_NUM",
        "NATL_HLTH_CARE_ENT_ID",
        "NDC_CD",
        "NDC_UOM_CD",
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
        "ORGNL_LINE_NUM",
        "OTHR_INSRNC_IND",
        "OTHR_TPL_CLCTN_CD",
        "OTHR_TPL_CLCTN_CD",
        "PGM_TYPE_CD",
        "PLAN_ID_NUM",
        "PRCDR_CD",
        "PRE_AUTHRZTN_NUM",
        "PRSCRBNG_PRVDR_NPI_NUM",
        "PRCDR_CD_IND",
        "PRVDR_FAC_TYPE_CD",
        "PRVDR_LCTN_ID",
        "PRVDR_UNDER_SPRVSN_NPI_NUM",
        "PTNT_CNTL_NUM",
        "PTNT_STUS_CD",
        "PYMT_LVL_IND",
        "REV_CD",
        "RFRG_PRVDR_NPI_NUM",
        "RFRG_PRVDR_NUM",
        "RMTNC_NUM",
        "SBMTR_ID",
        "SBMTR_ID",
        "SECT_1115A_DEMO_IND",
        "SELF_DRCTN_TYPE_CD",
        "SPLIT_CLM_IND",
        "SRC_LCTN_CD",
        "SRVC_TRKNG_TYPE_CD",
        "SRVCNG_PRVDR_NUM",
        "SRVCNG_PRVDR_SPCLTY_CD",
        "SRVCNG_PRVDR_TXNMY_CD",
        "SRVCNG_PRVDR_TYPE_CD",
        "STATE_NOTN_TXT",
        "STATE_NOTN_TXT",
        "STC_CD",
        "SUBMTG_STATE_CD",
        "TMSIS_FIL_NAME",
        "TOOTH_ORAL_CVTY_AREA_DSGNTD_CD",
        "WVR_ID",
        "WVR_TYPE_CD",
        "XOVR_IND",
        "XIX_SRVC_CTGRY_CD",
        "XXI_SRVC_CTGRY_CD",
    ]

    renames = {}

    header_renames = {"PLAN_ID_NUM": "MC_PLAN_ID"}

    line_renames = {
        "SUBMTG_STATE_CD": "SUBMTG_STATE_CD_LINE",
        "MSIS_IDENT_NUM": "MSIS_IDENT_NUM_LINE",
        "TMSIS_RUN_ID": "TMSIS_RUN_ID_LINE",
        "TMSIS_ACTV_IND": "TMSIS_ACTV_IND_LINE",
        "ADJDCTN_DT": "ADJDCTN_DT_LINE",
        "ADJSTMT_CLM_NUM": "ADJSTMT_CLM_NUM_LINE",
        "ORGNL_CLM_NUM": "ORGNL_CLM_NUM_LINE",
        "SRVC_BGNNG_DT": "SRVC_BGNNG_DT_LINE",
        "SRVC_ENDG_DT": "SRVC_ENDG_DT_LINE",
        "STC_CD": "TOS_CD",
        "HCPCS_SRVC_CD": "HCBS_SRVC_CD",
        "HCPCS_TXNMY_CD": "HCBS_TXNMY",
        "NDC_UOM_CD": "UOM_CD",
    }

    header_columns = [
        "DA_RUN_ID",
        "OT_LINK_KEY",
        "OT_VRSN",
        "OT_FIL_DT",
        "TMSIS_RUN_ID",
        "MSIS_IDENT_NUM",
        "SUBMTG_STATE_CD",
        "ORGNL_CLM_NUM",
        "ADJSTMT_CLM_NUM",
        "ADJSTMT_IND",
        "ADJSTMT_RSN_CD",
        "SRVC_BGNNG_DT",
        "SRVC_ENDG_DT",
        "ADJDCTN_DT",
        "MDCD_PD_DT",
        "SECT_1115A_DEMO_IND",
        "CLM_TYPE_CD",
        "BILL_TYPE_CD",
        "PGM_TYPE_CD",
        "MC_PLAN_ID",
        "ELGBL_LAST_NAME",
        "ELGBL_1ST_NAME",
        "ELGBL_MDL_INITL_NAME",
        "BIRTH_DT",
        "WVR_TYPE_CD",
        "WVR_ID",
        "SRVC_TRKNG_TYPE_CD",
        "SRVC_TRKNG_PYMT_AMT",
        "OTHR_INSRNC_IND",
        "OTHR_TPL_CLCTN_CD",
        "FIXD_PYMT_IND",
        "FUNDNG_CD",
        "FUNDNG_SRC_NON_FED_SHR_CD",
        "BRDR_STATE_IND",
        "XOVR_IND",
        "MDCR_HICN_NUM",
        "MDCR_BENE_ID",
        "HLTH_CARE_ACQRD_COND_CD",
        "DGNS_1_CD",
        "DGNS_1_CD_IND",
        "DGNS_POA_1_CD_IND",
        "DGNS_2_CD",
        "DGNS_2_CD_IND",
        "DGNS_POA_2_CD_IND",
        "SRVC_PLC_CD",
        "PRVDR_LCTN_ID",
        "BLG_PRVDR_NUM",
        "BLG_PRVDR_NPI_NUM",
        "BLG_PRVDR_TXNMY_CD",
        "BLG_PRVDR_TYPE_CD",
        "BLG_PRVDR_SPCLTY_CD",
        "RFRG_PRVDR_NUM",
        "RFRG_PRVDR_NPI_NUM",
        "RFRG_PRVDR_TXNMY_CD",
        "RFRG_PRVDR_TYPE_CD",
        "RFRG_PRVDR_SPCLTY_CD",
        "PRVDR_UNDER_DRCTN_NPI_NUM",
        "PRVDR_UNDER_DRCTN_TXNMY_CD",
        "PRVDR_UNDER_SPRVSN_NPI_NUM",
        "PRVDR_UNDER_SPRVSN_TXNMY_CD",
        "HH_PRVDR_IND",
        "HH_PRVDR_NPI_NUM",
        "HH_ENT_NAME",
        "RMTNC_NUM",
        "cast(DAILY_RATE as decimal(13,2)) as DAILY_RATE",
        "PYMT_LVL_IND",
        "TOT_BILL_AMT",
        "TOT_ALOWD_AMT",
        "TOT_MDCD_PD_AMT",
        "TOT_COPAY_AMT",
        "TOT_MDCR_DDCTBL_AMT",
        "TOT_MDCR_COINSRNC_AMT",
        "TOT_TPL_AMT",
        "TOT_OTHR_INSRNC_AMT",
        "TP_COINSRNC_PD_AMT",
        "TP_COPMT_PD_AMT",
        "MDCR_CMBND_DDCTBL_IND",
        "MDCR_REIMBRSMT_TYPE_CD",
        "BENE_COINSRNC_AMT",
        "BENE_COINSRNC_PD_DT",
        "BENE_COPMT_AMT",
        "BENE_COPMT_PD_DT",
        "BENE_DDCTBL_AMT",
        "BENE_DDCTBL_PD_DT",
        "COPAY_WVD_IND",
        "CPTATD_AMT_RQSTD_DT",
        "CPTATD_PYMT_RQSTD_AMT",
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
        "CLL_CNT",
        "cast(NUM_CLL as bigint) as NUM_CLL",
        "OT_MH_DX_IND",
        "OT_SUD_DX_IND",
        "OT_MH_TXNMY_IND",
        "OT_SUD_TXNMY_IND",
        "IAP_COND_IND",
        "PRMRY_HIRCHCL_COND",
        "REC_ADD_TS",
        "REC_UPDT_TS",
        "SRVC_ENDG_DT_DRVD",
        "SRVC_ENDG_DT_CD",
        "BLG_PRVDR_NPPES_TXNMY_CD",
        "DGNS_1_CCSR_DFLT_CTGRY_CD",
        "FED_SRVC_CTGRY_CD",
        "SRC_LCTN_CD"
    ]

    line_columns = [
        "DA_RUN_ID",
        "OT_LINK_KEY",
        "OT_VRSN",
        "OT_FIL_DT",
        "TMSIS_RUN_ID",
        "MSIS_IDENT_NUM",
        "SUBMTG_STATE_CD",
        "ORGNL_CLM_NUM",
        "ADJSTMT_CLM_NUM",
        "ORGNL_LINE_NUM",
        "ADJSTMT_LINE_NUM",
        "ADJDCTN_DT",
        "LINE_ADJSTMT_IND",
        "ADJSTMT_LINE_RSN_CD",
        "CLL_STUS_CD",
        "SRVC_BGNNG_DT",
        "SRVC_ENDG_DT",
        "REV_CD",
        "PRCDR_CD",
        "PRCDR_CD_DT",
        "PRCDR_CD_IND",
        "PRCDR_1_MDFR_CD",
        "IMNZTN_TYPE_CD",
        "BILL_AMT",
        "ALOWD_AMT",
        "COPAY_AMT",
        "TPL_AMT",
        "MDCD_PD_AMT",
        "MDCD_FFS_EQUIV_AMT",
        "MDCR_PD_AMT",
        "OTHR_INSRNC_AMT",
        "cast(ACTL_SRVC_QTY as numeric(12,3)) as ACTL_SRVC_QTY",
        "cast(ALOWD_SRVC_QTY as numeric(12,3)) as ALOWD_SRVC_QTY",
        "TOS_CD",
        "BNFT_TYPE_CD",
        "HCBS_SRVC_CD",
        "HCBS_TXNMY",
        "SRVCNG_PRVDR_NUM",
        "SRVCNG_PRVDR_NPI_NUM",
        "SRVCNG_PRVDR_TXNMY_CD",
        "SRVCNG_PRVDR_TYPE_CD",
        "SRVCNG_PRVDR_SPCLTY_CD",
        "TOOTH_DSGNTN_SYS_CD",
        "TOOTH_NUM",
        "TOOTH_ORAL_CVTY_AREA_DSGNTD_CD",
        "TOOTH_SRFC_CD",
        "CMS_64_FED_REIMBRSMT_CTGRY_CD",
        "XIX_SRVC_CTGRY_CD",
        "XXI_SRVC_CTGRY_CD",
        "STATE_NOTN_TXT",
        "NDC_CD",
        "PRCDR_2_MDFR_CD",
        "PRCDR_3_MDFR_CD",
        "PRCDR_4_MDFR_CD",
        "HCPCS_RATE",
        "SELF_DRCTN_TYPE_CD",
        "PRE_AUTHRZTN_NUM",
        "UOM_CD",
        "cast(NDC_QTY as numeric(12,3)) as NDC_QTY",
        "REC_ADD_TS",
        "REC_UPDT_TS",
        "LINE_NUM",
        "PRCDR_CCS_CTGRY_CD",
        "SRVCNG_PRVDR_NPPES_TXNMY_CD"
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
