create
or replace temporary view Prov08_Groups as
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
    submitting_state_prov_id_of_affiliated_entity
