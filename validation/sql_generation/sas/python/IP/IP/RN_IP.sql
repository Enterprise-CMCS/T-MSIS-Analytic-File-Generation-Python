create
or replace temporary view RN_IP as
select
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND,
    max(RN) as NUM_CLL
from
    IP_LINE
group by
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
