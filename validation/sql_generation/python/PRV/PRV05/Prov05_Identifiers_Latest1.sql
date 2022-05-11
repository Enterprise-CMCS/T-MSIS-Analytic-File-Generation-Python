create
or replace temporary view Prov05_Identifiers_Latest1 as
select
    T.*,
    R.SPCL
from
    Prov_Identifiers T
    inner join Prov03_Locations R on T.tms_run_id = R.tms_run_id
    and T.submitting_state = R.submitting_state
    and upper(T.submitting_state_prov_id) = R.submitting_state_prov_id
    and upper(T.prov_location_id) = R.prov_location_id
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id
