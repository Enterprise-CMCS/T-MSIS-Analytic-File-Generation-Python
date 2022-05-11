create table Prov03_Locations_link diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    *,
    cast (
        (
            '46' || '-' || 201712 || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')
        ) as varchar(74)
    ) as PRV_LOC_LINK_KEY
from
    #Prov03_Locations_STV order by tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id;