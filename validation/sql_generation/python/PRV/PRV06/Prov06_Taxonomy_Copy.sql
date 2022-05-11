create
or replace temporary view Prov06_Taxonomy_Copy as
select
    tms_run_id,
    tms_reporting_period,
    record_number,
    submitting_state,
    nullif(trim(upper(submitting_state_prov_id)), '') as submitting_state_prov_id,
    case
        when (
            prov_classification_type = '2'
            or prov_classification_type = '3'
        )
        and length(trim(prov_classification_code)) < 2
        and length(trim(prov_classification_code)) > 0
        and nullif(trim(upper(prov_classification_code)), '') is not null then lpad(trim(upper(prov_classification_code)), 2, '0')
        when prov_classification_type = '4'
        and length(trim(prov_classification_code)) < 3
        and length(trim(prov_classification_code)) > 0
        and nullif(trim(upper(prov_classification_code)), '') is not null then lpad(trim(upper(prov_classification_code)), 3, '0')
        else nullif(trim(upper(prov_classification_code)), '')
    end as prov_classification_code,
    prov_classification_type,
    prov_taxonomy_classification_eff_date,
    prov_taxonomy_classification_end_date,
    SPCL
from
    Prov06_Taxonomy_Latest1
where
    tms_is_active = 1
    and (
        prov_classification_type is not null
        and prov_classification_code is not null
    )
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
