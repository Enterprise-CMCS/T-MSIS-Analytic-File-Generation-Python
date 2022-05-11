create table Prov07_Medicaid_STS diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as varchar(2)) as PRVDR_MDCD_ENRLMT_STUS_CD
from
    #Prov07_Medicaid T 
    left join prv_formats_sm F on F.fmtname = 'ENRSTCDV'
    and (
        Trim(T.prov_medicaid_enrollment_status_code) >= F.start
        and Trim(T.prov_medicaid_enrollment_status_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;