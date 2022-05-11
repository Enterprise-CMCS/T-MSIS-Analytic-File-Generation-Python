create table Prov09_AffPgms_CNST diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
select
    6539 :: integer as DA_RUN_ID,
    cast (
        (
            '46' || '-' || 201712 || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')
        ) as varchar(50)
    ) as PRV_LINK_KEY,
    201712 :: varchar(10) as PRV_FIL_DT,
    '46' :: varchar(2) as PRV_VRSN,
    tms_run_id as TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
    AFLTD_PGM_TYPE_CD,
    affiliated_program_id as AFLTD_PGM_ID
from
    Prov09_AffPgms_TYP
order by
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    SUBMTG_STATE_PRVDR_ID;