create table Prov05_Identifiers_Latest1 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id
) as
select
    T.*
from
    Prov_Identifiers T
    inner join #Prov03_Locations_g0 R on 
    T.tms_run_id = R.tms_run_id
    and T.submitting_state = R.submitting_state
    and nullif(trim(upper(T.submitting_state_prov_id)), '') = R.submitting_state_prov_id
    and nullif(trim(upper(T.prov_location_id)), '') = R.prov_location_id
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id;
