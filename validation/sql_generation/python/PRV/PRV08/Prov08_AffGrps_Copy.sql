create
or replace temporary view Prov08_AffGrps_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(
        trim(
            upper(submitting_state_prov_id_of_affiliated_entity)
        ),
        ''
    ) as submitting_state_prov_id_of_affiliated_entity,
    prov_affiliated_group_eff_date,
    prov_affiliated_group_end_date,
    SPCL
from
    Prov08_AffGrps_Latest1
where
    tms_is_active = 1
    and (
        submitting_state_prov_id_of_affiliated_entity is not null
    )
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
