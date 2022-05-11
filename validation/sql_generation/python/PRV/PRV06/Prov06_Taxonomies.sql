create
or replace temporary view Prov06_Taxonomies as
select
    *
from
    TblCopyGrouped
where
    _wanted = 1
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_classification_type,
    prov_classification_code
