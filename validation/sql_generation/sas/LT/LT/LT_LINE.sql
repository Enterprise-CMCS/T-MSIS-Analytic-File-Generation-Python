create temp table LT_LINE distkey (ORGNL_CLM_NUM_LINE) sortkey (
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
) as
select
    A.*,
    case
        when regexp_count(trim(SELECTED_TXNMY_CD), '[^0]+') = 0
        or SELECTED_TXNMY_CD in (
            '8888888888',
            '9999999999',
            '000000000X',
            '999999999X',
            'NONE',
            'XXXXXXXXXX',
            'NO TAXONOMY'
        )
        or nullif(trim('8888888888'), '') = NULL
        or nullif(trim('9999999999'), '') = NULL
        or nullif(trim('000000000X'), '') = NULL
        or nullif(trim('999999999X'), '') = NULL
        or nullif(trim('NONE'), '') = NULL
        or nullif(trim('XXXXXXXXXX'), '') = NULL
        or nullif(trim('NO TAXONOMY'), '') = NULL then NULL
        else SELECTED_TXNMY_CD
    end as SRVCNG_PRVDR_NPPES_TXNMY_CD
from
    LT_LINE_PRE_NPPES as A
    left join nppes_npi nppes on nppes.prvdr_npi = a.SRVCNG_PRVDR_NPI_NUM