create temp TABLE CLM_FMLY_OTHR_TOC distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    TMSIS_RUN_ID,
    coalesce(upper(ORGNL_CLM_NUM), '~') as ORGNL_CLM_NUM,
    coalesce(upper(ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM,
    SUBMTG_STATE_CD,
    coalesce(ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT,
    COALESCE(ADJSTMT_IND, 'X') as ADJSTMT_IND
from
    cms_prod.TMSIS_CLM_FMLY_OTHR_TOC F
where
    CLM_FMLY_FINL_ACTN_IND = 1
    and F.submtg_state_cd = '01'
    and F.tmsis_run_id = 4684
group by
    1,
    2,
    3,
    4,
    5,
    6
having
    count(TMSIS_RUN_ID) = 1