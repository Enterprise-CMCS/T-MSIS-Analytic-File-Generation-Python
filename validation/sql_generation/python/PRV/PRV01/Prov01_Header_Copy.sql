create
or replace temporary view Prov01_Header_Copy as
select
    tms_run_id,
    submitting_state
from
    (
        select
            *,
            submitting_state as submtg_state_cd
        from
            File_Header_Record_Provider
        where
            tms_is_active = 1
            and tms_reporting_period is not null
            and tot_rec_cnt > 0
            and trim(submitting_state) not in ('94', '96')
    )
order by
    tms_run_id,
    submitting_state
