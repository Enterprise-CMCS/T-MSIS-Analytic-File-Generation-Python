create table Prov03_Location_CNST diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
select
    6539 :: integer as DA_RUN_ID,
    cast (
        (
            '46' || '-' || 201712 || '-' || T.SUBMTG_STATE_CD || '-' || coalesce(T.submitting_state_prov_id, '*')
        ) as varchar(50)
    ) as PRV_LINK_KEY,
    T.PRV_LOC_LINK_KEY,
    201712 :: varchar(10) as PRV_FIL_DT,
    '46' :: varchar(2) as PRV_VRSN,
    T.tms_run_id as TMSIS_RUN_ID,
    T.SUBMTG_STATE_CD,
    T.submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    T.prov_location_id as PRVDR_LCTN_ID,
    L.PRVDR_ADR_BLG_IND,
    L.PRVDR_ADR_PRCTC_IND,
    L.PRVDR_ADR_SRVC_IND,
    nullif(trim(upper(T.addr_ln1)), '') as ADR_LINE_1_TXT,
    nullif(trim(upper(T.addr_ln2)), '') as ADR_LINE_2_TXT,
    nullif(trim(upper(T.addr_ln3)), '') as ADR_LINE_3_TXT,
    nullif(trim(upper(T.addr_city)), '') as ADR_CITY_NAME,
    T.ADR_STATE_CD,
    T.addr_zip_code as ADR_ZIP_CD,
    T.addr_county as ADR_CNTY_CD,
    T.ADR_BRDR_STATE_IND,
    case
        when L.PRVDR_ADR_SRVC_IND = 1
        and T.SUBMTG_STATE_rcd = T.ADR_STATE_CD then 0
        when L.PRVDR_ADR_SRVC_IND = 1
        and T.SUBMTG_STATE_rcd <> T.ADR_STATE_CD then 1
        else null
    end :: smallint as PRVDR_SRVC_ST_DFRNT_SUBMTG_ST
from
    #Prov03_Location_BSM T left join #Prov03_Location_mapt L on T.PRV_LOC_LINK_KEY=L.PRV_LOC_LINK_KEY where 
    T.prov_addr_type = '4'
    or (
        T.prov_addr_type = '3'
        and (
            L.PRVDR_ADR_SRVC_IND is null
            or L.PRVDR_ADR_SRVC_IND = 0
        )
    )
    or (
        T.prov_addr_type = '1'
        and (
            L.PRVDR_ADR_SRVC_IND is null
            or L.PRVDR_ADR_SRVC_IND = 0
        )
        and (
            L.PRVDR_ADR_PRCTC_IND is null
            or L.PRVDR_ADR_PRCTC_IND = 0
        )
    )
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID,
    PRVDR_LCTN_ID;