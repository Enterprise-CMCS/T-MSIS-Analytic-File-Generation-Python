create temp TABLE COMBINED_HEADER distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as (
    select
        A.TMSIS_RUN_ID,
        A.ORGNL_CLM_NUM,
        A.ADJSTMT_CLM_NUM,
        A.SUBMTG_STATE_CD,
        A.ADJDCTN_DT,
        A.ADJSTMT_IND,
        null as SRVC_ENDG_DT_DRVD_L,
        null as SRVC_ENDG_DT_CD_L
    from
        HEADER2_OTHR_TOC A
    where
        A.NO_SRVC_DT = 0
    union
    all
    select
        B.TMSIS_RUN_ID,
        B.ORGNL_CLM_NUM,
        B.ADJSTMT_CLM_NUM,
        B.SUBMTG_STATE_CD,
        B.ADJDCTN_DT,
        B.ADJSTMT_IND,
        case
            when nullif(B.SRVC_ENDG_DT, '01JAN1960') is null
            or SRVC_ENDG_DT is null then SRVC_BGNNG_DT
            else SRVC_ENDG_DT
        end as SRVC_ENDG_DT_DRVD_L,
        case
            when nullif(B.SRVC_ENDG_DT, '01JAN1960') is not null
            and SRVC_ENDG_DT is not null then '4'
            when nullif(B.SRVC_BGNNG_DT, '01JAN1960') is not null
            and SRVC_BGNNG_DT is not null then '5'
            else null
        end as SRVC_ENDG_DT_CD_L
    from
        NO_DISCHARGE_DATES B
)