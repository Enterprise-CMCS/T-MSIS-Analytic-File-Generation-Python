create
or replace temporary view IP_LINE as
select
    a.*,
    row_number() over (
        partition by a.SUBMTG_STATE_CD,
        a.ORGNL_CLM_NUM_LINE,
        a.ADJSTMT_CLM_NUM_LINE,
        a.ADJDCTN_DT_LINE,
        a.LINE_ADJSTMT_IND
        order by
            a.SUBMTG_STATE_CD,
            a.ORGNL_CLM_NUM_LINE,
            a.ADJSTMT_CLM_NUM_LINE,
            a.ADJDCTN_DT_LINE,
            a.LINE_ADJSTMT_IND,
            a.TMSIS_FIL_NAME,
            a.REC_NUM
    ) as RN,
    a.submtg_state_cd as new_submtg_state_cd_line
from
    IP_LINE_IN as A
    inner join FA_HDR_IP H on H.TMSIS_RUN_ID = a.TMSIS_RUN_ID_LINE
    and H.SUBMTG_STATE_CD = a.SUBMTG_STATE_CD
    and H.ORGNL_CLM_NUM = a.ORGNL_CLM_NUM_LINE
    and H.ADJSTMT_CLM_NUM = a.ADJSTMT_CLM_NUM_LINE
    and H.ADJDCTN_DT = a.ADJDCTN_DT_LINE
    and H.ADJSTMT_IND = a.LINE_ADJSTMT_IND
