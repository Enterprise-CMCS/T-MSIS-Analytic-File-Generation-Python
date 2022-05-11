create
or replace temporary view Prov02_Main_TF as
select
    T.*,
    cast(F.label as varchar(1)) as TCHNG_IND
from
    Prov02_Main_PC T
    left join prv_formats_sm F on F.fmtname = TEACHV
    and (
        Trim(T.teaching_ind) >= F.start
        and Trim(T.teaching_ind) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
