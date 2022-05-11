create
or replace temporary view Prov07_Medicaid_MTD as
select
    T.*,
    cast(F.label as varchar(1)) as PRVDR_MDCD_ENRLMT_MTHD_CD
from
    Prov07_Medicaid_ENR T
    left join prv_formats_sm F on F.fmtname = ENRMDCDV
    and (
        Trim(T.prov_enrollment_method) >= F.start
        and Trim(T.prov_enrollment_method) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
