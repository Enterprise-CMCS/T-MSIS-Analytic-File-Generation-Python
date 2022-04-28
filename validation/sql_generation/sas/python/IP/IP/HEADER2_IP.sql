create
or replace temporary view HEADER2_IP as
select
    A.TMSIS_RUN_ID,
    A.ORGNL_CLM_NUM,
    A.ADJSTMT_CLM_NUM,
    A.SUBMTG_STATE_CD,
    A.ADJDCTN_DT,
    A.ADJSTMT_IND,
    A.NO_DISCH_DT
from
    HEADER_IP A
group by
    A.TMSIS_RUN_ID,
    A.SUBMTG_STATE_CD,
    A.ORGNL_CLM_NUM,
    A.ADJSTMT_CLM_NUM,
    A.ADJDCTN_DT,
    A.ADJSTMT_IND,
    A.NO_DISCH_DT
having
    count(A.TMSIS_RUN_ID) = 1
