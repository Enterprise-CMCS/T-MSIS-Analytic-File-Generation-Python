create table Prov06_Taxonomies_CNST diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    PRVDR_CLSFCTN_IND,
    PRVDR_CLSFCTN_TYPE_CD,
    case
        when PRVDR_CLSFCTN_IND = 1 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as MLT_SNGL_SPCLTY_GRP_IND,
    case
        when PRVDR_CLSFCTN_IND = 2 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as ALPTHC_OSTPTHC_PHYSN_IND,
    case
        when PRVDR_CLSFCTN_IND = 3 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 4 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as CHRPRCTIC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 5 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as DNTL_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 6 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as DTRY_NTRTNL_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 7 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as EMER_MDCL_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 8 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as EYE_VSN_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 9 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as NRSNG_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 10 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as NRSNG_SRVC_RLTD_IND,
    case
        when PRVDR_CLSFCTN_IND = 11 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as OTHR_INDVDL_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 12 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as PHRMCY_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 13 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 14 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as POD_MDCN_SRGRY_SRVCS_IND,
    case
        when PRVDR_CLSFCTN_IND = 15 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as RESP_DEV_REH_RESTOR_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 16 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as SPCH_LANG_HEARG_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 17 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as STDNT_HLTH_CARE_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 18 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 19 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as AGNCY_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 20 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as AMB_HLTH_CARE_FAC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 21 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as HOSP_UNIT_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 22 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as HOSP_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 23 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as LAB_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 24 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as MCO_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 25 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as NRSNG_CSTDL_CARE_FAC_IND,
    case
        when PRVDR_CLSFCTN_IND = 26 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as OTHR_NONINDVDL_SRVC_PRVDRS_IND,
    case
        when PRVDR_CLSFCTN_IND = 27 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as RSDNTL_TRTMT_FAC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 28 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as RESP_CARE_FAC_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 29 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as SUPLR_PRVDR_IND,
    case
        when PRVDR_CLSFCTN_IND = 30 then 1
        when PRVDR_CLSFCTN_IND is null then null
        else 0
    end :: smallint as TRNSPRTN_SRVCS_PRVDR_IND,
    case
        when (
            PRVDR_CLSFCTN_TYPE_CD = '1'
            or PRVDR_CLSFCTN_TYPE_CD = 'N'
        )
        and PRVDR_CLSFCTN_SME = 1 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '2'
        and PRVDR_CLSFCTN_SME = 4 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '4'
        and PRVDR_CLSFCTN_SME = 7 then 1
        when PRVDR_CLSFCTN_CD = '.'
        or PRVDR_CLSFCTN_CD is null
        or PRVDR_CLSFCTN_TYPE_CD = '.'
        or PRVDR_CLSFCTN_TYPE_CD is null then null
        when PRVDR_CLSFCTN_TYPE_CD <> '1'
        and PRVDR_CLSFCTN_TYPE_CD <> 'N'
        and PRVDR_CLSFCTN_TYPE_CD <> '2'
        and PRVDR_CLSFCTN_TYPE_CD <> '4' then null
        else 0
    end :: smallint as SUD_SRVC_PRVDR_IND,
    case
        when (
            PRVDR_CLSFCTN_TYPE_CD = '1'
            or PRVDR_CLSFCTN_TYPE_CD = 'N'
        )
        and PRVDR_CLSFCTN_SME = 2 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '2'
        and PRVDR_CLSFCTN_SME = 5 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '3'
        and PRVDR_CLSFCTN_MHT = 1 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '4'
        and PRVDR_CLSFCTN_SME = 8 then 1
        when PRVDR_CLSFCTN_CD = '.'
        or PRVDR_CLSFCTN_CD is null
        or PRVDR_CLSFCTN_TYPE_CD = '.'
        or PRVDR_CLSFCTN_TYPE_CD is null then null
        when PRVDR_CLSFCTN_TYPE_CD <> '1'
        and PRVDR_CLSFCTN_TYPE_CD <> 'N'
        and PRVDR_CLSFCTN_TYPE_CD <> '2'
        and PRVDR_CLSFCTN_TYPE_CD <> '3'
        and PRVDR_CLSFCTN_TYPE_CD <> '4' then null
        else 0
    end :: smallint as MH_SRVC_PRVDR_IND,
    case
        when (
            PRVDR_CLSFCTN_TYPE_CD = '1'
            or PRVDR_CLSFCTN_TYPE_CD = 'N'
        )
        and PRVDR_CLSFCTN_SME = 3 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '2'
        and PRVDR_CLSFCTN_SME = 6 then 1
        when PRVDR_CLSFCTN_TYPE_CD = '4'
        and PRVDR_CLSFCTN_SME = 9 then 1
        when PRVDR_CLSFCTN_CD = '.'
        or PRVDR_CLSFCTN_CD is null
        or PRVDR_CLSFCTN_TYPE_CD = '.'
        or PRVDR_CLSFCTN_TYPE_CD is null then null
        when PRVDR_CLSFCTN_TYPE_CD <> '1'
        and PRVDR_CLSFCTN_TYPE_CD <> 'N'
        and PRVDR_CLSFCTN_TYPE_CD <> '2'
        and PRVDR_CLSFCTN_TYPE_CD <> '4' then null
        else 0
    end :: smallint as EMER_SRVCS_PRVDR_IND
from
    #Prov06_Taxonomies_MHT where PRVDR_CLSFCTN_TYPE_CD is not null and PRVDR_CLSFCTN_CD is not null order by tms_run_id, submitting_state, submitting_state_prov_id;