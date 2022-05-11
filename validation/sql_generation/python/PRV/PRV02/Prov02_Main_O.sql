create
or replace temporary view Prov02_Main_O as
select
    T.*,
    cast(F.label as smallint) as OWNRSHP_CAT
from
    Prov02_Main_OV T
    left join prv_formats_sm F on F.fmtname = OWNER
    and (
        Trim(T.ownership_code) >= F.start
        and Trim(T.ownership_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
