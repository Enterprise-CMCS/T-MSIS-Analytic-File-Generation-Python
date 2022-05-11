create temp TABLE FA_HDR_LT distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    A.*,
    h.submtg_state_cd as new_submtg_state_cd
from
    ALL_HEADER_LT H
    inner join HEADER_LT A on H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD
    and H.TMSIS_RUN_ID = A.TMSIS_RUN_ID
    and H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM
    and H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM
    and H.ADJDCTN_DT = A.ADJDCTN_DT
    and H.ADJSTMT_IND = A.ADJSTMT_IND