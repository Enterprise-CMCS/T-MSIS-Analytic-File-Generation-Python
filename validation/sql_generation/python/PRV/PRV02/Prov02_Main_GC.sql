create
or replace temporary view Prov02_Main_GC as
select
    T.*,
    cast(F.label as varchar(1)) as GNDR_CD
from
    Prov02_Main_INDV T
    left join prv_formats_sm F on F.fmtname = GENDERV
    and (
        Trim(T.sex) >= F.start
        and Trim(T.sex) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
