create
or replace temporary view Prov07_Medicaid_STS as
select
    T.*,
    cast(F.label as varchar(2)) as PRVDR_MDCD_ENRLMT_STUS_CD
from
    Prov07_Medicaid_ST T
    left join prv_formats_sm F on F.fmtname = ENRSTCDV
    and (
        Trim(T.prov_medicaid_enrollment_status_code) >= F.start
        and Trim(T.prov_medicaid_enrollment_status_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
