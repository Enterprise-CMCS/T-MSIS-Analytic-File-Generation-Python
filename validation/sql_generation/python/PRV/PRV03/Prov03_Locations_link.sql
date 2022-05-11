create
or replace temporary view Prov03_Locations_link as
select
    *,
    case
        when SPCL is not null then cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*') | | '-' | | coalesce(prov_location_id, '**') | | '-' | | SPCL
            ) as varchar(74)
        )
        else cast (
            (
                '0A' | | '-' | | 202110 | | '-' | | SUBMTG_STATE_CD | | '-' | | coalesce(submitting_state_prov_id, '*') | | '-' | | coalesce(prov_location_id, '**')
            ) as varchar(74)
        )
    end as PRV_LOC_LINK_KEY
from
    Prov03_Locations_STV
order by
    [ 'tms_run_id',
    'submitting_state',
    'submitting_state_prov_id',
    'prov_location_id' ]
