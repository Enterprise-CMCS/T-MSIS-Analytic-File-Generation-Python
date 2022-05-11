create table Prov02_Main_OV diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as varchar(2)) as OWNRSHP_CD
from
    #Prov02_Main_TF T left join prv_formats_sm 
    F on F.fmtname = 'OWNERV'
    and (
        Trim(T.ownership_code) >= F.start
        and Trim(T.ownership_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;