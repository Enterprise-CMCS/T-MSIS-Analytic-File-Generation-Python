create table Prov07_Medicaid_Latest1 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*
from
    Prov_Medicaid_Enrollment T
    inner join #Prov02_Main R on 
    T.tms_run_id = R.tms_run_id
    and T.submitting_state = R.submitting_state
    and nullif(trim(upper(T.submitting_state_prov_id)), '') = R.submitting_state_prov_id
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;