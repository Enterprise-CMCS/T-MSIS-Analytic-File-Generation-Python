create temp table HEADER2_OTHR_TOC distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    A.TMSIS_RUN_ID,
    A.ORGNL_CLM_NUM,
    A.ADJSTMT_CLM_NUM,
    A.SUBMTG_STATE_CD,
    A.ADJDCTN_DT,
    A.ADJSTMT_IND,
    A.NO_SRVC_DT
from
    HEADER_OTHR_TOC A
group by
    A.TMSIS_RUN_ID,
    A.SUBMTG_STATE_CD,
    A.ORGNL_CLM_NUM,
    A.ADJSTMT_CLM_NUM,
    A.ADJDCTN_DT,
    A.ADJSTMT_IND,
    NO_SRVC_DT
having
    count(A.TMSIS_RUN_ID) = 1