create
or replace temporary view Prov08_Groups_STV as
select
    T.*,
    when F.label is null then T.submitting_state
    else F.label
end as SUBMTG_STATE_CD
from
    Prov08_Groups T
    left join prv_formats_sm F on F.fmtname = STFIPC
    and (
        Trim(T.submitting_state) >= F.start
        and Trim(T.submitting_state) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
