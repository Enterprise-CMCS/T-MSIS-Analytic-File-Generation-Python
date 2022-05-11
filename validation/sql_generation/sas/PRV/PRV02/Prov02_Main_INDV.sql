create table Prov02_Main_INDV diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    *,
    case
        when date_of_birth is null
        or date_cmp(date_of_birth, '31DEC2017') = 1 then null
        when date_of_death is not null
        and date_cmp(date_of_death, '31DEC2017') in (-1, 0) then floor((date_of_death - date_of_birth) / 365.25)
        else floor(('31DEC2017' - date_of_birth) / 365.25)
    end :: smallint as AGE_NUM,
    case
        when date_cmp(date_of_death, '31DEC2017') = 1 then null
        else date_of_death
    end as DEATH_DT
from
    #Prov02_Main_NP where FAC_GRP_INDVDL_CD='03' or FAC_GRP_INDVDL_CD is null order by tms_run_id, submitting_state, submitting_state_prov_id;