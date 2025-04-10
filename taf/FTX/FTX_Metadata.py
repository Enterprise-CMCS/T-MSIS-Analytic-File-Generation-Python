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

        columns =  FTX_Metadata.columns.get(segment_id).copy()
        cleanser = FTX_Metadata.cleanser.get(segment_id).copy()
        renames =  FTX_Metadata.renames.get(segment_id).copy()
        #upper = FTX_Metadata.upper.get(segment_id).copy()
        upper = FTX_Metadata.upper.copy()

        for i, item in enumerate(columns):
            if item in cleanser.keys():
                columns[i] = cleanser.copy().get(item)(item, alias)
            # elif item in FTX_Metadata.validator.keys():
            #     columns[i] = FTX_Metadata.maskInvalidValues(item, alias)
            elif item in upper:
                columns[i] = f"upper({alias}.{item}) as {str(item).lower()}"
            else:
                columns[i] = f"{alias}.{columns[i]}"
            if item in renames.keys():
                columns[i] = (
                    columns[i].lower().split(" as ")[0]
                    + f" as {renames.get(item).lower()}"
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
            if fld in FTX_Metadata.upper:
                rpl_columns.append(f"upper({fld})")
            else:
                rpl_columns.append(fld)

        return new_line_comma.join(rpl_columns)

    columns = {
        "FTX00002": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",
            "PYMT_OR_RCPMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID",
            "PAYERID_TYPE",
            "PYR_MCR_PLAN_TYPE",
            "PYEE_ID",
            "PYEE_ID_TYPE",
            "PYEE_MCR_PLAN_TYPE",
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE",
            "SSN_NUM",
            "PLCY_MMBR_ID",
            "PLCY_GRP_NUM",
            "PLCY_OWNR_CD",
            "INSRNC_PLN_ID",
            "INSRNC_CARR_ID_NUM",
            "CPTATN_PRD_STRT_DT",
            "CPTATN_PRD_END_DT",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "FED_REIMBRSMT_CTGRY_CD",
            "MBESCBES_FORM_GRP",
            "MBESCBES_FORM",
            "MBESCBES_SRVC_CTGRY_CD",
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD",
            "SDP_IND",
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND",
            "PYMT_CAT_XREF", 
            "APM_MODEL_TYPE_CD",
            "EXPNDTR_AUTHRTY_TYPE"
        ],
        "FTX00003": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",
            "PYMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID",
            "PAYERID_TYPE",
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "PYEE_ID_TYPE",
            "PYEE_MC_PLN_TYPE_CD",
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE",
            "SSN_NUM",
            "MMBR_ID",
            "PLCY_GRP_NUM",
            "PLCY_OWNR_CD",
            "INSRNC_PLAN_ID",
            "INSRNC_CARR_ID_NUM",
            "PRM_PRD_STRT_DT",
            "PRM_PRD_END_DT",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "FED_REIMBRSMT_CTGRY_CD",
            "MBESCBES_FORM_GRP",
            "MBESCBES_FORM",
            "MBESCBES_SRVC_CTGRY_CD",
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
            "EXPNDTR_AUTHRTY_TYPE"
        ],
        "FTX00004": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_DT",
            "PYMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID",
            "PAYERID_TYPE",
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "PYEE_ID_TYPE",
            "PYEE_MC_PLN_TYPE_CD",
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE",
            "SSN",
            "MMBR_ID",
            "GRP_NUM",
            "PLCY_OWNR_CD",
            "INSRNC_PLAN_ID",
            "INSRNC_CARR_ID_NUM",
            "PRM_PRD_STRT_DT",
            "PRM_PRD_END_DT",
            "PMT_PRD_TYPE_CD",
            "TRNS_TYPE_CD",
            "FED_REIMBRSMT_CTGRY_CD",
            "MBESCBES_FORM_GRP",
            "MBESCBES_FORM",
            "MBESCBES_SRVC_CTGRY_CD",
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
            "EXPNDTR_AUTHRTY_TYPE"
        ],
        "FTX00005": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",
            "PYMT_OR_RCPMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID",
            "PAYERID_TYPE",
            "PYR_MC_PLN_TYPE_CD", #DOES NOT EXIST
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLAN_ID", 
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "CVRG_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "CVRG_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #DOES NOT EXIST
            "TRNS_TYPE_CD", #NEEDS RENAME TO TRNS_TYPE_CD
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TRANS_TYPE", 
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PMT_CTGRY_XREF", #DOES NOT EXIST
            "APM_MODEL_TYPE_CD", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00006": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",
            "PYMT_OR_RCPMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID", #NEEDS RENAME TO PYR_ID
            "PAYERID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLN_ID", #DOES NOT EXIST
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "PRFMNC_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "PRFMNC_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #DOES NOT EXIST
            "TRNS_TYPE_CD", #DOES NOT EXIST
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #DOES NOT EXIST
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PYMT_CAT_XREF", #DOES NOT EXIST
            "VB_PYMT_MODEL_TYPE", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00007": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM", #DOES NOT EXIST
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",
            "PYMT_OR_RCPMT_AMT",
            "CHK_EFCTV_DT",
            "PAYERID", #NEEDS RENAME TO PYR_ID
            "PAYERID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLN_ID", #DOES NOT EXIST
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "PYMT_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "PYMT_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PYMT_PRD_TYPE", #NEEDS RENAME PMT_PRD_TYPE_CD
            "TRNS_TYPE_CD", #DOES NOT EXIST
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #DOES NOT EXIST
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PYMT_CAT_XREF", #DOES NOT EXIST
            "APM_MODEL_TYPE_CD", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00008": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #DOES NOT EXIST
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",   #DOES NOT EXIST
            "PYMT_OR_RCPMT_AMT",  #DOES NOT EXIST
            "CHK_EFCTV_DT",
            "PAYERID", #NEEDS RENAME TO PYR_ID
            "PAYERID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            
            
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLN_ID", #NEEDS RENAME INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "CST_STLMT_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "CST_STLMT_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #DOES NOT EXIST
            "TRNS_TYPE_CD", #DOES NOT EXIST
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #NEEDS RENAME OFST_TYPE_CD
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PMT_CTGRY_XREF", #DOES NOT EXIST
            "APM_MODEL_TYPE_CD", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00009": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #DOES NOT EXIST
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",   #DOES NOT EXIST
            "PYMT_OR_RCPMT_AMT",  #DOES NOT EXIST
            "CHK_EFCTV_DT",
            "PAYERID", #NEEDS RENAME TO PYR_ID
            "PAYERID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYR_MC_PLN_TYPE_CD",
            
            
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLN_ID", #NEEDS RENAME INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "WRP_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "WRP_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PMT_PRD_TYPE_CD", #DOES NOT EXIST
            "TRNS_TYPE_CD", #DOES NOT EXIST
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #NEEDS RENAME OFST_TYPE_CD
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PMT_CTGRY_XREF", #DOES NOT EXIST
            "APM_MODEL_TYPE_CD", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
        ],
        "FTX00095": [
            "TMSIS_RUN_ID",
            "MSIS_IDENT_NUM",  #DOES NOT EXIST
            "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM",
            "ADJSTMT_CLM_NUM",
            "ADJSTMT_IND",
            "PYMT_OR_RCPMT_DT",   #DOES NOT EXIST
            "PYMT_OR_RCPMT_AMT",  #DOES NOT EXIST
            "CHK_EFCTV_DT",
            "PAYERID", #NEEDS RENAME TO PYR_ID
            "PAYERID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            "PYR_MCR_PLAN_TYPE",
            
            
            "PYEE_ID",
            "PYEE_ID_TYPE", #NEEDS RENAME
            "PYEE_MCR_PLAN_TYPE", #NEEDS RENAME PYEE_MC_PLN_TYPE_CD
            "PYEE_TAX_ID",
            "PYEE_TAX_ID_TYPE", #NEEDS RENAME PYR_ID_TYPE_CD
            
            "SSN_NUM", #DOES NOT EXIST
            "PLCY_MMBR_ID", #DOES NOT EXIST
            "PLCY_GRP_NUM", #DOES NOT EXIST
            "PLCY_OWNR_CD", #DOES NOT EXIST
            "INSRNC_PLN_ID", #NEEDS RENAME INSRNC_PLN_ID
            "INSRNC_CARR_ID_NUM", #DOES NOT EXIST
            "PYMT_PRD_STRT_DT", #NEEDS RENAME PMT_PRD_EFF_DT
            "PYMT_PRD_END_DT", #NEEDS RENAME PMT_PRD_END_DT
            "PYMT_PRD_TYPE", #DOES NOT EXIST
            "TRANS_TYPE_CD", #DOES NOT EXIST
            "FED_REIMBRSMT_CTGRY_CD", #NEEDS RENAME FED_RIMBRSMT_CTGRY
            "MBESCBES_FORM_GRP", #NEEDS RENAME MBESCBES_FRM_GRP
            "MBESCBES_FORM", #NEEDS RENAME MBESCBES_FRM
            "MBESCBES_SRVC_CTGRY_CD", #NEEDS RENAME MBESCBES_SRVC_CTGRY
            "WVR_ID",
            "WVR_TYPE_CD",
            "FUNDNG_CD",
            "FUNDNG_SRC_NON_FED_SHR_CD",
            "OFST_TYPE_CD", #NEEDS RENAME OFST_TYPE_CD
            "SDP_IND", #DOES NOT EXIST
            "SRC_LCTN_CD",
            "SPA_NUM",
            "SUBCPTATN_IND", #DOES NOT EXIST
            "PYMT_CAT_XREF", #DOES NOT EXIST
            "APM_MODEL_TYPE_CD", #DOES NOT EXIST
            "EXPNDTR_AUTHRTY_TYPE" #NEEDS RENAME EXPNDTR_AUTHRTY_TYPE_CD
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
            "INSRNC_PLN_ID":TAF_Closure.set_as_null,
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
                "INSRNC_PLN_ID":TAF_Closure.set_as_null,
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
                "INSRNC_PLN_ID":TAF_Closure.set_as_null,
			    "INSRNC_CARR_ID_NUM":TAF_Closure.set_as_null,
                "PMT_PRD_TYPE_CD":TAF_Closure.set_as_null,
                "TRNS_TYPE_CD":TAF_Closure.set_as_null,
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SDP_IND":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "PMT_CTGRY_XREF":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
                
        },
        "FTX00095":
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
                "OFST_TYPE_CD":TAF_Closure.set_as_null,
                "SUBCPTATN_IND":TAF_Closure.set_as_null,
                "APM_MODEL_TYPE_CD":TAF_Closure.set_as_null,
        }
    }

    renames = {
        "FTX00002":
            {
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "PYMT_CAT_XREF":"PMT_CTGRY_XREF",
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "CPTATN_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "CPTATN_PRD_END_DT":"PMT_PRD_END_DT",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD"
            },
        "FTX00003":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "MMBR_ID":"PLCY_MMBR_ID",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "INSRNC_PLAN_ID":"INSRNC_PLN_ID",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PRM_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "PRM_PRD_END_DT":"PMT_PRD_END_DT"
            },
        "FTX00004":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_AMT":"PMT_OR_RCPMT_AMT",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "INSRNC_PLAN_ID":"INSRNC_PLN_ID",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "SSN":"SSN_NUM",
                "MMBR_ID":"PLCY_MMBR_ID",
                "GRP_NUM":"PLCY_GRP_NUM",
                "PRM_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "PRM_PRD_END_DT":"PMT_PRD_END_DT",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD"
            },
        "FTX00005":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYMT_AMT":"PMT_OR_RCPMT_AMT",
                "OFST_TRANS_TYPE":"OFST_TYPE_CD",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "INSRNC_PLAN_ID":"INSRNC_PLN_ID",
                "CVRG_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "CVRG_PRD_END_DT":"PMT_PRD_END_DT",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
            },
        "FTX00006":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYMT_CAT_XREF":"PMT_CTGRY_XREF",
                "VB_PYMT_MODEL_TYPE":"APM_MODEL_TYPE_CD",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "PRFMNC_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "PRFMNC_PRD_END_DT":"PMT_PRD_END_DT",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
            },
        "FTX00007":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYMT_CAT_XREF":"PMT_CTGRY_XREF",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "PYMT_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "PYMT_PRD_END_DT":"PMT_PRD_END_DT",
                "PYMT_PRD_TYPE":"PMT_PRD_TYPE_CD",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX00008":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "CST_STLMT_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "CST_STLMT_PRD_END_DT":"PMT_PRD_END_DT",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX00009":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "WRP_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "WRP_PRD_END_DT":"PMT_PRD_END_DT",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
            },
        "FTX00095":
            {
                "PAYERID":"PYR_ID",
                "PAYERID_TYPE":"PYR_ID_TYPE_CD",
                "PYR_MCR_PLAN_TYPE":"PYR_MC_PLN_TYPE_CD",
                "PYMT_PRD_STRT_DT":"PMT_PRD_EFF_DT",
                "PYMT_PRD_END_DT":"PMT_PRD_END_DT",
                "PYMT_PRD_TYPE":"PMT_PRD_TYPE_CD",
                "TRANS_TYPE_CD":"TRNS_TYPE_CD",
                "PYMT_CAT_XREF":"PMT_CTGRY_XREF",
                "PYMT_OR_RCPMT_DT":"PMT_OR_RCPMT_DT",
                "PYMT_OR_RCPMT_AMT":"PMT_OR_RCPMT_AMT",
                "PYEE_TAX_ID_TYPE":"PYEE_TAX_ID_TYPE_CD",
                "PYEE_ID_TYPE":"PYEE_ID_TYPE_CD",
                "PYEE_MCR_PLAN_TYPE":"PYEE_MC_PLN_TYPE_CD",
                "FED_REIMBRSMT_CTGRY_CD":"FED_RIMBRSMT_CTGRY",
                "MBESCBES_FORM_GRP":"MBESCBES_FRM_GRP",
                "MBESCBES_FORM":"MBESCBES_FRM",
                "MBESCBES_SRVC_CTGRY_CD":"MBESCBES_SRVC_CTGRY",
                "EXPNDTR_AUTHRTY_TYPE":"EXPNDTR_AUTHRTY_TYPE_CD",
            }
    }

    upper =  {
                "SUBMTG_STATE_CD",
                "MSIS_IDENT_NUM",
                "PAYERID",
                "PYR_MCR_PLAN_TYPE",
                "PYEE_ID",
                "PYEE_ID_TYPE",
                "PYEE_MCR_PLAN_TYPE",
                "PYEE_TAX_ID",
                "PYEE_TAX_ID_TYPE",
                "MMBR_ID",
                "GRP_NUM",
                "PLCY_OWNR_CD",
                "INSRNC_PALN_ID",
                "INSRNC_CARR_ID_NUM",
                "PMT_PRD_TYPE",
                "TRANS_TYPE_CD",
                "FED_REIMBRSMT_CTGRY_CD",
                "MBESCBES_FORM_GRP",
                "MBESCBES_FORM",
                "MBESCBES_SRVC_CTGRY_CD",
                "WVR_ID",
                "WVR_TYPE_CD",
                "FUNDNG_CD",
                "FUNDNG_SRC_NON_FED_SHR_CD",
                "OFST_TRANS_TYPE",
                "SDP_IND",
                "SRC_LCTN_CD",
                "SPA_NUM",
                "SUBCPTATN_IND",
                "PMT_CAT_XREF",
                "VB_PYMT_MODEL_TYPE",
                "EXPNDTR_AUTHRTY_TYPE_CD"
            }
    
    ftx_cols = [
        "DA_RUN_ID",
        "FTX_VRSN",
        "FTX_FIL_DT",
        "TMSIS_RUN_ID",
        "TMSIS_SGMT_NUM",
        "INDVDL_BENE_IND",
        "MSIS_IDENT_NUM",
        "SUBMTG_STATE_CD",
        "ORGNL_CLM_NUM",
        "ADJSTMT_CLM_NUM",
        "ADJSTMT_IND",
        "PMT_OR_RCPMT_DT",
        "PMT_OR_RCPMT_AMT",
        "CHK_EFCTV_DT",
        "PYR_ID",
        "PYR_ID_TYPE_CD",
        "PYR_MC_PLN_TYPE_CD",
        "PYEE_ID",
        "PYEE_ID_TYPE_CD",
        "PYEE_MC_PLN_TYPE_CD",
        "PYEE_TAX_ID",
        "PYEE_TAX_ID_TYPE_CD",
        "SSN_NUM",
        "PLCY_MMBR_ID",
        "PLCY_GRP_NUM",
        "PLCY_OWNR_CD",
        "INSRNC_PLN_ID",
        "INSRNC_CARR_ID_NUM",
        "PMT_PRD_EFF_DT",
        "PMT_PRD_END_DT",
        "PMT_PRD_TYPE_CD",
        "TRNS_TYPE_CD",
        "FED_RIMBRSMT_CTGRY",
        "MBESCBES_FRM_GRP",
        "MBESCBES_FRM",
        "MBESCBES_SRVC_CTGRY",
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
        "EXPNDTR_AUTHRTY_TYPE_CD",
        "REC_ADD_TS",
        "REC_UPDT_TS"
    ]