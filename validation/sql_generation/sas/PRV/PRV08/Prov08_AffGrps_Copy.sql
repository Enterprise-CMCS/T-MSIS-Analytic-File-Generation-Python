create table Prov08_AffGrps_Copy diststyle key distkey(submitting_state_prov_id) compound sortkey(
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    submitting_state as submtg_state_cd,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    nullif(
        trim(
            upper(submitting_state_prov_id_of_affiliated_entity)
        ),
        ''
    ) as submitting_state_prov_id_of_affiliated_entity,
    prov_affiliated_group_eff_date,
    prov_affiliated_group_end_date
from
    Prov08_AffGrps_Latest1
where
    tms_is_active = 1
    and (
        nullif(
            trim(
                upper(submitting_state_prov_id_of_affiliated_entity)
            ),
            ''
        ) is not null
    )
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;

;