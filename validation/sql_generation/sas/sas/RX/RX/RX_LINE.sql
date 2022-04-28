create temp table RX_LINE distkey (ORGNL_CLM_NUM_LINE) sortkey (
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
) as
select
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
    a.*,
    a.submtg_state_cd as new_submtg_state_cd_line,
CASE
        WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
        ELSE SUBSTRING(A.DRUG_UTLZTN_CD, 1, 2)
    END AS RSN_SRVC_CD,
CASE
        WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
        ELSE SUBSTRING(A.DRUG_UTLZTN_CD, 3, 2)
    END AS PROF_SRVC_CD,
CASE
        WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
        ELSE SUBSTRING(A.DRUG_UTLZTN_CD, 5, 2)
    END AS RSLT_SRVC_CD
from
    RX_LINE_IN A
    inner join FA_HDR_RX as HEADER on HEADER.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE
    and HEADER.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE
    and HEADER.ADJDCTN_DT = A.ADJDCTN_DT_LINE
    and HEADER.ADJSTMT_IND = A.LINE_ADJSTMT_IND
    and HEADER.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD
    and HEADER.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE