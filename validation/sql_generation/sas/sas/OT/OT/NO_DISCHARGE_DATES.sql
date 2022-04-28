create temp TABLE NO_DISCHARGE_DATES distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as with MAX_DATES as (
    select
        H.TMSIS_RUN_ID,
        H.ORGNL_CLM_NUM,
        H.ADJSTMT_CLM_NUM,
        H.SUBMTG_STATE_CD,
        H.ADJDCTN_DT,
        H.ADJSTMT_IND,
        MAX(L.SRVC_ENDG_DT) as SRVC_ENDG_DT,
        MAX(L.SRVC_BGNNG_DT) as SRVC_BGNNG_DT
    from
        HEADER2_OTHR_TOC H
        inner join cms_prod.TMSIS_CLL_REC_OTHR_TOC L on H.TMSIS_RUN_ID = L.TMSIS_RUN_ID
        and H.SUBMTG_STATE_CD = L.SUBMTG_STATE_CD
        and H.ORGNL_CLM_NUM = upper(coalesce(L.ORGNL_CLM_NUM, '~'))
        and H.ADJSTMT_CLM_NUM = upper(coalesce(L.ADJSTMT_CLM_NUM, '~'))
        and H.ADJDCTN_DT = coalesce(L.ADJDCTN_DT, '01JAN1960')
        and H.ADJSTMT_IND = upper(coalesce(L.LINE_ADJSTMT_IND, 'X'))
    where
        L.TMSIS_ACTV_IND = 1
        and L.SRVC_ENDG_DT is not null
        and H.NO_SRVC_DT = 1
        AND L.SUBMTG_STATE_CD = '01'
        AND L.TMSIS_RUN_ID = 4684
    group by
        H.TMSIS_RUN_ID,
        H.ORGNL_CLM_NUM,
        H.ADJSTMT_CLM_NUM,
        H.SUBMTG_STATE_CD,
        H.ADJDCTN_DT,
        H.ADJSTMT_IND
)
select
    *
from
    MAX_DATES
where
    (
        (
            date_part_year (SRVC_ENDG_DT) = 2017
            and date_part (month, SRVC_ENDG_DT) = 10
        )
    );