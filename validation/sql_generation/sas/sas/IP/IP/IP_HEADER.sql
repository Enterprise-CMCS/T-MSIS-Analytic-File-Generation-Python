create temp table IP_HEADER distkey (ORGNL_CLM_NUM) sortkey (
    NEW_SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    HEADER.*,
    coalesce(RN.NUM_CLL, 0) as NUM_CLL
from
    FA_HDR_IP HEADER
    left join RN_IP RN on HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE
    and HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE
    and HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE
    and HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE
    and HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND