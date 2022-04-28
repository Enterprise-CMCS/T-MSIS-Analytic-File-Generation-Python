create
or replace temporary view ALL_HEADER_IP as
select
    H.*
from
    COMBINED_HEADER H
    inner join CLM_FMLY_IP F on H.ORGNL_CLM_NUM = F.ORGNL_CLM_NUM
    and H.ADJSTMT_CLM_NUM = F.ADJSTMT_CLM_NUM
    and H.ADJDCTN_DT = F.ADJDCTN_DT
    and H.ADJSTMT_IND = F.ADJSTMT_IND
    and H.SUBMTG_STATE_CD = F.SUBMTG_STATE_CD
    and H.TMSIS_RUN_ID = F.TMSIS_RUN_ID
