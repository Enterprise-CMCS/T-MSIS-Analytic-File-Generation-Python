create table nppes_tax_final as
select
    distinct i.tms_run_id,
    i.submtg_state_cd,
    i.submitting_state,
    i.submitting_state_prov_id,
    'N' as prvdr_clsfctn_type_cd,
    n.prvdr_clsfctn_cd
from
    #nppes_id1 i left join #NPPES_tax1 n on i.prvdr_id=n.prvdr_id_chr where