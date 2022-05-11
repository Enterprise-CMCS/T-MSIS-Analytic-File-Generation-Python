create
or replace temporary view Prov01_Header as
select
    submitting_state,
    SPCL,
    max(tms_run_id) as tms_run_id
from
    Prov01_Header_Copy
    left join SPCLlst as S on submitting_state = S.start
where
    (submitting_state, tms_run_id) in (concat('56', '00123'))
group by
    submitting_state,
    SPCL
order by
    submitting_state
