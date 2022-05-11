create
or replace temporary view Prov02_Main as
select
    *
from
    TblCopyGrouped
where
    _wanted = 1
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
