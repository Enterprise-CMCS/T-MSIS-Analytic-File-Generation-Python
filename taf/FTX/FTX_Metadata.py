from taf.TAF_Closure import TAF_Closure

class FTX_Metadata:
    """
    Create the FTX metadata.
    """
    def selectDataElements(segment_id: str, alias: str):
        """
        Function to select data elements.  Selected data elements will be cleansed, checked against a validator,
        and masked if there are invalid values.
        """

        new_line_comma = "\n\t\t\t,"

        columns = FTX_Metadata.columns.get(segment_id).copy()
        cleanser = FTX_Metadata.cleanser.get(segment_id).copy()
        renames = FTX_Metadata.renames.get(segment_id).copy()

        for i, item in enumerate(columns):
            if item in cleanser.keys():
                columns[i] = cleanser.copy().get(item)(item, alias)
            # elif item in FTX_Metadata.validator.keys():
            #     columns[i] = FTX_Metadata.maskInvalidValues(item, alias)
            # elif item in FTX_Metadata.upper:
            #     columns[i] = f"upper({alias}.{item}) as {str(item).lower()}"
            else:
                columns[i] = f"{alias}.{columns[i]}"
            if item in renames.keys():
                columns[i] = (
                    columns[i].lower().split(" as ")[0]
                    + f" as {renames.get(item).lower()}"
                )


            # # qualify header columns
            # if segment_id == "CIP00002":
            #     if item in IP_Metadata.header_renames.keys():
            #         columns[i] = (
            #             columns[i].lower().split(" as ")[0]
            #             + f" as {IP_Metadata.header_renames.get(item).lower()}"
            #         )

            # # qualify line columns
            # if segment_id == "CIP00003":
            #     if item in IP_Metadata.line_renames.keys():
            #         columns[i] = (
            #             columns[i].lower().split(" as ")[0]
            #             + f" as {IP_Metadata.line_renames.get(item).lower()}"
            #         )

        return new_line_comma.join(columns)
    
    columns = {
        "FTX00002": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",
            "pymt_or_rcpmt_amt",
            "CHK_EFCTV_DT",
            "payerid",
            "payerid_type",
            "pyr_mcr_plan_type",
            "PYEE_ID",
            "pyee_id_type",
            "pyee_mcr_plan_type",
            "PYEE_TAX_ID",
            "pyee_tax_id_type",
            "SSN_NUM",
            "PLCY_MMBR_ID",
            "PLCY_GRP_NUM",
            "PLCY_OWNR_CD",
            "INSRNC_PLN_ID",
            "INSRNC_CARR_ID_NUM",
            "cptatn_prd_strt_dt",
            "cptatn_prd_end_dt",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "fed_reimbrsmt_ctgry_cd",
            "mbescbes_form_grp",
            "mbescbes_form",
            "mbescbes_srvc_ctgry_cd",
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD",
            "SDP_IND",
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND",
            "pymt_cat_xref", 
            "APM_MODEL_TYPE_CD",
            "expndtr_authrty_type"
        ],
        "FTX00003": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",
            "pymt_amt",
            "CHK_EFCTV_DT",
            "payerid",
            "payerid_type",
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "pyee_id_type",
            "PYEE_MC_PLN_TYPE_CD",
            "PYEE_TAX_ID",
            "pyee_tax_id_type",
            "SSN_NUM",
            "mmbr_id",
            "PLCY_GRP_NUM",
            "PLCY_OWNR_CD",
            "insrnc_plan_id",
            "INSRNC_CARR_ID_NUM",
            "prm_prd_strt_dt",
            "prm_prd_end_dt",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "fed_reimbrsmt_ctgry_cd",
            "mbescbes_form_grp",
            "mbescbes_form",
            "mbescbes_srvc_ctgry_cd",
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD",
            "SDP_IND",
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND",
            "PMT_CTGRY_XREF",
            "APM_MODEL_TYPE_CD",
            "expndtr_authrty_type"
        ],
        "FTX00004": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_dt",
            "pymt_amt",
            "CHK_EFCTV_DT",
            "payerid",
            "payerid_type",
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "pyee_id_type",
            "PYEE_MC_PLN_TYPE_CD",
            "PYEE_TAX_ID",
            "pyee_tax_id_type",
            "ssn",
            "mmbr_id",
            "grp_num",
            "PLCY_OWNR_CD",
            "insrnc_plan_id",
            "INSRNC_CARR_ID_NUM",
            "prm_prd_strt_dt",
            "prm_prd_end_dt",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "fed_reimbrsmt_ctgry_cd",
            "mbescbes_form_grp",
            "mbescbes_form",
            "mbescbes_srvc_ctgry_cd",
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD",
            "SDP_IND",
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND",
            "PMT_CTGRY_XREF",
            "APM_MODEL_TYPE_CD",
            "expndtr_authrty_type"
        ],
        "FTX00005": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",
            "pymt_or_rcpmt_amt",
            "CHK_EFCTV_DT",
            "payerid",
            "payerid_type",
            "PYR_MC_PLN_TYPE_CD", #does not exist
            "PYEE_ID",
            "pyee_id_type", #needs rename PYR_ID_TYPE_CD
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "insrnc_plan_id", 
            "INSRNC_CARR_ID_NUM", #does not exist
            "cvrg_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "cvrg_prd_end_dt", #needs rename PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #does not exist
            "TRNS_TYPE_CD", #needs rename to TRNS_TYPE_CD
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "ofst_trans_type", 
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "PMT_CTGRY_XREF", #does not exist
            "APM_MODEL_TYPE_CD", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00006": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",
            "pymt_or_rcpmt_amt",
            "CHK_EFCTV_DT",
            "payerid", #needs rename to PYR_ID
            "payerid_type", #needs rename PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "pyee_id_type", #needs rename PYR_ID_TYPE_CD
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "insrnc_plan_id", #does not exist
            "INSRNC_CARR_ID_NUM", #does not exist
            "prfmnc_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "prfmnc_prd_end_dt", #needs rename PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #does not exist
            "TRNS_TYPE_CD", #does not exist
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #does not exist
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "pymt_cat_xref", #does not exist
            "vb_pymt_model_type", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00007": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM", #does not exist
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",
            "pymt_or_rcpmt_amt",
            "CHK_EFCTV_DT",
            "payerid", #needs rename to PYR_ID
            "payerid_type", #needs rename PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "pyee_id_type", #needs rename PYR_ID_TYPE_CD
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "INSRNC_PLN_ID", #does not exist
            "INSRNC_CARR_ID_NUM", #does not exist
            "pymt_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "pymt_prd_end_dt", #needs rename PMT_PRD_END_DT
            "pymt_prd_type", #needs rename PMT_PRD_TYPE_CD
            "TRNS_TYPE_CD", #does not exist
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #does not exist
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "pymt_cat_xref", #does not exist
            "APM_MODEL_TYPE_CD", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00008": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #does not exist
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",   #does not exist
            "pymt_or_rcpmt_amt",  #does not exist
            "CHK_EFCTV_DT",
            "payerid", #needs rename to PYR_ID
            "payerid_type", #needs rename PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            
            
            "PYEE_ID",
            "pyee_id_type", #needs rename
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "insrnc_plan_id", #needs rename INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #does not exist
            "cst_stlmt_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "cst_stlmt_prd_end_dt", #needs rename PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #does not exist
            "TRNS_TYPE_CD", #does not exist
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #needs rename OFST_TYPE_CD
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "PMT_CTGRY_XREF", #does not exist
            "APM_MODEL_TYPE_CD", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00009": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #does not exist
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",   #does not exist
            "pymt_or_rcpmt_amt",  #does not exist
            "CHK_EFCTV_DT",
            "payerid", #needs rename to PYR_ID
            "payerid_type", #needs rename PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            
            
            "PYEE_ID",
            "pyee_id_type", #needs rename
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "insrnc_plan_id", #needs rename INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #does not exist
            "wrp_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "wrp_prd_end_dt", #needs rename PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #does not exist
            "TRNS_TYPE_CD", #does not exist
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #needs rename OFST_TYPE_CD
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "PMT_CTGRY_XREF", #does not exist
            "APM_MODEL_TYPE_CD", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX000095": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #does not exist
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "pymt_or_rcpmt_dt",   #does not exist
            "pymt_or_rcpmt_amt",  #does not exist
            "CHK_EFCTV_DT",
            "payerid", #needs rename to PYR_ID
            "payerid_type", #needs rename PYR_ID_TYPE_CD
            "pyr_mcr_plan_type",
            
            
            "PYEE_ID",
            "pyee_id_type", #needs rename
            "pyee_mcr_plan_type", #needs rename PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "pyee_tax_id_type", #needs rename PYR_ID_TYPE_CD
            
            "SSN_NUM", #does not exist
            "PLCY_MMBR_ID", #does not exist
            "PLCY_GRP_NUM", #does not exist
            "PLCY_OWNR_CD", #does not exist
            "insrnc_plan_id", #needs rename INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #does not exist
            "pymt_prd_strt_dt", #needs rename PMT_PRD_EFF_DT
            "pymt_prd_end_dt", #needs rename PMT_PRD_END_DT
            "pymt_prd_type", #does not exist
            "trans_type_cd", #does not exist
            "fed_reimbrsmt_ctgry_cd", #needs rename FED_RIMBRSMT_CTGRY
            "mbescbes_form_grp", #needs rename MBESCBES_FRM_GRP
            "mbescbes_form", #needs rename MBESCBES_FRM
            "mbescbes_srvc_ctgry_cd", #needs rename MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #needs rename OFST_TYPE_CD
            "SDP_IND", #does not exist
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #does not exist
            "pymt_cat_xref", #does not exist
            "APM_MODEL_TYPE_CD", #does not exist
            "expndtr_authrty_type" #needs rename EXPNDTR_AUTHRTY_TYPE_CD
        ]
        
    }
    
    cleanser = {
        "FTX00002":
        {
            "ADJDCTN_DT": TAF_Closure.coalesce_date,
            "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
            "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
            "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
            "SSN_NUM":TAF_Closure.set_as_null,
            "PLCY_MMBR_ID":TAF_Closure.set_as_null,
            "PLCY_GRP_NUM":TAF_Closure.set_as_null,
            "PLCY_OWNR_CD":TAF_Closure.set_as_null,
            "INSRNC_PLN_ID":TAF_Closure.set_as_null,
            "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
            "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
            "TRNS_TYPE_CD":TAF_Closure.set_as_null,
            "OFST_TYPE_CD":TAF_Closure.set_as_null,
            "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null
        },
        "FTX00003":
        {
            "ADJDCTN_DT": TAF_Closure.coalesce_date,
            "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
            "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
            "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
            "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
            "PYEE_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
            "SSN_NUM":TAF_Closure.set_as_null,
            "PLCY_GRP_NUM":TAF_Closure.set_as_null,
            "PLCY_OWNR_CD":TAF_Closure.set_as_null,
            "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
            "TRNS_TYPE_CD":TAF_Closure.set_as_null,
            "OFST_TYPE_CD":TAF_Closure.set_as_null,
            "SDP_IND":TAF_Closure.set_as_null,
            "SUBCPTATN_IND":TAF_Closure.set_as_null,
            "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
            "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00004":
        {
            "ADJDCTN_DT": TAF_Closure.coalesce_date,
            "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
            "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
            "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
            "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
            "PYEE_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
            "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
            "ofst_trans_type":TAF_Closure.set_as_null,
            "OFST_TYPE_CD":TAF_Closure.set_as_null,
            "SDP_IND":TAF_Closure.set_as_null,
            "SUBCPTATN_IND":TAF_Closure.set_as_null,
            "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
            "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
            "TRNS_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00005":
        {
            "ADJDCTN_DT": TAF_Closure.coalesce_date,
            "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
            "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
            "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
            "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
            "SSN_NUM":TAF_Closure.set_as_null,
            "PLCY_MMBR_ID":TAF_Closure.set_as_null,
            "PLCY_GRP_NUM":TAF_Closure.set_as_null,
            "PLCY_OWNR_CD":TAF_Closure.set_as_null,
            "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
            "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
            "TRNS_TYPE_CD":TAF_Closure.set_as_null,
            "SDP_IND":TAF_Closure.set_as_null,
            "SUBCPTATN_IND":TAF_Closure.set_as_null,
            "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
            "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00006":
        {
            "ADJDCTN_DT": TAF_Closure.coalesce_date,
            "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
            "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
            "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
            "SSN_NUM":TAF_Closure.set_as_null,
            "PLCY_MMBR_ID":TAF_Closure.set_as_null,
            "PLCY_GRP_NUM":TAF_Closure.set_as_null,
            "PLCY_OWNR_CD":TAF_Closure.set_as_null,
            "insrnc_plan_id":TAF_Closure.set_as_null,
            "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
            "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
            "TRNS_TYPE_CD":TAF_Closure.set_as_null,
            "OFST_TYPE_CD":TAF_Closure.set_as_null,
            "SUBCPTATN_IND":TAF_Closure.set_as_null,
            "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00007":
        {
                "ADJDCTN_DT": TAF_Closure.coalesce_date,
                "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
                "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
                "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
                "MSIS_IDENT_NUM":TAF_Closure.set_as_null,
                "SSN_NUM":TAF_Closure.set_as_null,
                "PLCY_MMBR_ID":TAF_Closure.set_as_null,
                "PLCY_GRP_NUM":TAF_Closure.set_as_null,
                "PLCY_OWNR_CD":TAF_Closure.set_as_null,
                "INSRNC_PLN_ID":TAF_Closure.set_as_null,
                "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
                "TRNS_TYPE_CD":TAF_Closure.set_as_null,
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SDP_IND":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
                "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00008":
        {
                "ADJDCTN_DT": TAF_Closure.coalesce_date,
                "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
                "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
                "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
                "MSIS_IDENT_NUM":TAF_Closure.set_as_null,
                "SSN_NUM":TAF_Closure.set_as_null,
                "PLCY_MMBR_ID":TAF_Closure.set_as_null,
                "PLCY_GRP_NUM":TAF_Closure.set_as_null,
                "PLCY_OWNR_CD":TAF_Closure.set_as_null,
                "insrnc_plan_id":TAF_Closure.set_as_null,
			    "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
                "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
                "TRNS_TYPE_CD":TAF_Closure.set_as_null,
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SDP_IND":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
                "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
        },
        "FTX00009":
        {
                "ADJDCTN_DT": TAF_Closure.coalesce_date,
                "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
                "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
                "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
                "MSIS_IDENT_NUM":TAF_Closure.set_as_null,
                "PYR_MC_PLN_TYPE_CD":TAF_Closure.set_as_null,
                "SSN_NUM":TAF_Closure.set_as_null,
                "PLCY_MMBR_ID":TAF_Closure.set_as_null,
                "PLCY_GRP_NUM":TAF_Closure.set_as_null,
                "PLCY_OWNR_CD":TAF_Closure.set_as_null,
                "insrnc_plan_id":TAF_Closure.set_as_null,
			    "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
                "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
                "TRNS_TYPE_CD":TAF_Closure.set_as_null,
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SDP_IND":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
                
        },
        "FTX000095":
        {
                "ADJDCTN_DT": TAF_Closure.coalesce_date,
                "ADJSTMT_IND": TAF_Closure.cleanADJSTMT_IND,
                "ADJSTMT_CLM_NUM": TAF_Closure.coalesce_tilda,
                "ORGNL_CLM_NUM": TAF_Closure.coalesce_tilda,
                "SSN_NUM":TAF_Closure.set_as_null,
                "PLCY_MMBR_ID":TAF_Closure.set_as_null,
                "PLCY_GRP_NUM":TAF_Closure.set_as_null,
                "PLCY_OWNR_CD":TAF_Closure.set_as_null,
                "insrnc_plan_id":TAF_Closure.set_as_null,
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
        }
    }
    
    renames = {
        "FTX00002":
            {
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "pymt_cat_xref":"PMT_CTGRY_XREF",
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "cptatn_prd_strt_dt":"PMT_PRD_EFF_DT",
                "cptatn_prd_end_dt":"PMT_PRD_END_DT",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                
            },
        "FTX00003":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_amt":"PMT_OR_RCPMT_AMT",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "mmbr_id":"PLCY_MMBR_ID",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "prm_prd_strt_dt":"PMT_PRD_EFF_DT",
                "prm_prd_end_dt":"PMT_PRD_END_DT"
            },
        "FTX00004":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pymt_dt":"PMT_OR_RCPMT_DT",
                "pymt_amt":"PMT_OR_RCPMT_AMT",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "ssn":"SSN_NUM",
                "mmbr_id":"PLCY_MMBR_ID",
                "grp_num":"PLCY_GRP_NUM",
                "prm_prd_strt_dt":"PMT_PRD_EFF_DT",
                "prm_prd_end_dt":"PMT_PRD_END_DT",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD"
            },
        "FTX00005":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "pymt_amt":"PMT_OR_RCPMT_AMT",
                "ofst_trans_type":"OFST_TYPE_CD",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "cvrg_prd_strt_dt":"PMT_PRD_EFF_DT",
                "cvrg_prd_end_dt":"PMT_PRD_END_DT",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
            },
        "FTX00006":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "pymt_cat_xref":"PMT_CTGRY_XREF",
                "vb_pymt_model_type":"APM_MODEL_TYPE_CD",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "prfmnc_prd_strt_dt":"PMT_PRD_EFF_DT",
                "prfmnc_prd_end_dt":"PMT_PRD_END_DT",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
            },
        "FTX00007":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "pymt_cat_xref":"PMT_CTGRY_XREF",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "pymt_prd_strt_dt":"PMT_PRD_EFF_DT",
                "pymt_prd_end_dt":"PMT_PRD_END_DT",
                "pymt_prd_type":"PMT_PRD_TYPE_CD",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX00008":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "cst_stlmt_prd_strt_dt":"PMT_PRD_EFF_DT",
                "cst_stlmt_prd_end_dt":"PMT_PRD_END_DT",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX00009":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "wrp_prd_strt_dt":"PMT_PRD_EFF_DT",
                "wrp_prd_end_dt":"PMT_PRD_END_DT",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX000095":
            {
                "payerid":"PYR_ID",
                "payerid_type":"PYR_ID_TYPE_CD",
                "pyr_mcr_plan_type":"PYR_MC_PLN_TYPE_CD",
                "pymt_prd_strt_dt":"PMT_PRD_EFF_DT",
                "pymt_prd_end_dt":"PMT_PRD_END_DT",
                "pymt_prd_type":"PMT_PRD_TYPE_CD",
                "trans_type_cd":"TRNS_TYPE_CD",
                "pymt_cat_xref":"PMT_CTGRY_XREF",
                "pymt_or_rcpmt_dt":"PMT_OR_RCPMT_DT",
                "pymt_or_rcpmt_amt":"PMT_OR_RCPMT_AMT",
                "pyee_tax_id_type":"PYEE_TAX_ID_TYPE_CD",
                "pyee_id_type":"PYEE_ID_TYPE_CD",
                "pyee_mcr_plan_type":"PYEE_MC_PLN_TYPE_CD",
                "insrnc_plan_id":"INSRNC_PLN_ID",
                "fed_reimbrsmt_ctgry_cd":"FED_RIMBRSMT_CTGRY",
                "mbescbes_form_grp":"MBESCBES_FRM_GRP",
                "mbescbes_form":"MBESCBES_FRM",
                "mbescbes_srvc_ctgry_cd":"MBESCBES_SRVC_CTGRY",
                "expndtr_authrty_type":"EXPNDTR_AUTHRTY_TYPE_CD",
            }
    }
