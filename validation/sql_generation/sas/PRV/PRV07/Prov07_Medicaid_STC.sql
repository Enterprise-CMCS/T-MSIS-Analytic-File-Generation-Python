create table Prov07_Medicaid_STC diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as smallint) as PRVDR_MDCD_ENRLMT_STUS_CTGRY
from
    #Prov07_Medicaid_STS 
    T
    left join prv_formats_sm F on F.fmtname = 'ENRSTCAT'
    and (
        Trim(T.PRVDR_MDCD_ENRLMT_STUS_CD) >= F.start
        and Trim(T.PRVDR_MDCD_ENRLMT_STUS_CD) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;