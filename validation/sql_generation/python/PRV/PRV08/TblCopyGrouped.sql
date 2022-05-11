create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        submitting_state_prov_id_of_affiliated_entity
        order by
            tms_reporting_period desc,
            prov_affiliated_group_eff_date desc,
            prov_affiliated_group_end_date desc,
            record_number desc,
            submitting_state_prov_id_of_affiliated_entity asc
    ) as _wanted
from
    Prov08_AffGrps_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    submitting_state_prov_id_of_affiliated_entity
