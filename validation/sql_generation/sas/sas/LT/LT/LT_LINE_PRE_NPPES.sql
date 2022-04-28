create temp table LT_LINE_PRE_NPPES distkey (ORGNL_CLM_NUM_LINE) sortkey (SRVCNG_PRVDR_NPI_NUM) as
select
    A.*,
    row_number() over (
        partition by A.SUBMTG_STATE_CD,
        A.ORGNL_CLM_NUM_LINE,
        A.ADJSTMT_CLM_NUM_LINE,
        A.ADJDCTN_DT_LINE,
        A.LINE_ADJSTMT_IND
        order by
            A.SUBMTG_STATE_CD,
            A.ORGNL_CLM_NUM_LINE,
            A.ADJSTMT_CLM_NUM_LINE,
            A.ADJDCTN_DT_LINE,
            A.LINE_ADJSTMT_IND,
            A.TMSIS_FIL_NAME,
            A.REC_NUM
    ) as RN,
    a.submtg_state_cd as new_submtg_state_cd_line
from
    LT_LINE_IN as A
    inner join FA_HDR_LT H on H.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE
    and H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD
    and H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE
    and H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE
    and H.ADJDCTN_DT = A.ADJDCTN_DT_LINE
    and H.ADJSTMT_IND = A.LINE_ADJSTMT_IND