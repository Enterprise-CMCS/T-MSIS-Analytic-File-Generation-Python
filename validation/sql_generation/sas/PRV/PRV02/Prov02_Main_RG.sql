create table Prov02_Main_RG diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as smallint) as REG_FLAG
from
    #Prov02_Main_ST T left join prv_formats_sm F 
    on F.fmtname = 'REGION'
    and (
        Trim(T.State) >= F.start
        and Trim(T.State) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;