create
or replace temporary view Prov02_Main_ST as
select
    T.*,
    cast(F.label as varchar(7)) as State
from
    Prov02_Main_STV T
    left join prv_formats_sm F on F.fmtname = STFIPN
    and (
        Trim(T.SUBMTG_STATE_CD) >= F.start
        and Trim(T.SUBMTG_STATE_CD) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
