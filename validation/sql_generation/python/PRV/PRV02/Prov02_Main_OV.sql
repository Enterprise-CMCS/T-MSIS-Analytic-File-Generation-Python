create
or replace temporary view Prov02_Main_OV as
select
    T.*,
    cast(F.label as varchar(2)) as OWNRSHP_CD
from
    Prov02_Main_TF T
    left join prv_formats_sm F on F.fmtname = OWNERV
    and (
        Trim(T.ownership_code) >= F.start
        and Trim(T.ownership_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
