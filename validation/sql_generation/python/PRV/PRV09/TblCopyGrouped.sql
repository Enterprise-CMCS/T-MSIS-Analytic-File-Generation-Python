create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        AFLTD_PGM_TYPE_CD,
        affiliated_program_id
        order by
            tms_reporting_period desc,
            prov_affiliated_program_eff_date desc,
            prov_affiliated_program_end_date desc,
            record_number desc,
            affiliated_program_id asc
    ) as _wanted
from
    Prov09_AffPgms_TYP1
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    AFLTD_PGM_TYPE_CD,
    affiliated_program_id
