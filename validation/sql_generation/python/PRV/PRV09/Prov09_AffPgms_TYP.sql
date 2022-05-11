create
or replace temporary view Prov09_AffPgms_TYP as
select
    *
from
    TblCopyGrouped
where
    _wanted = 1
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    AFLTD_PGM_TYPE_CD,
    affiliated_program_id
