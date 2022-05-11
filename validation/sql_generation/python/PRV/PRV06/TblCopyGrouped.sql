create
or replace temporary view TblCopyGrouped as
select
    *,
    row_number() over (
        partition by tms_run_id,
        submitting_state,
        submitting_state_prov_id,
        prov_classification_type,
        prov_classification_code
        order by
            tms_reporting_period desc,
            prov_taxonomy_classification_eff_date desc,
            prov_taxonomy_classification_end_date desc,
            record_number desc,
            prov_classification_type asc
    ) as _wanted
from
    Prov06_Taxonomy_Latest2
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_classification_type,
    prov_classification_code
