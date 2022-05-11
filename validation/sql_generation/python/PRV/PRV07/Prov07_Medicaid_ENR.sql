create
or replace temporary view Prov07_Medicaid_ENR as
select
    T.*,
    cast(F.label as varchar(1)) as STATE_PLAN_ENRLMT_CD
from
    Prov07_Medicaid_STC T
    left join prv_formats_sm F on F.fmtname = ENRCDV
    and (
        Trim(T.state_plan_enrollment) >= F.start
        and Trim(T.state_plan_enrollment) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
