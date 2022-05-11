create table Prov02_Main_NP diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as varchar(1)) as ACPT_NEW_PTNTS_IND
from
    #Prov02_Main_PS T left join 
    prv_formats_sm F on F.fmtname = 'NEWPATV'
    and (
        Trim(T.accepting_new_patients_ind) >= F.start
        and Trim(T.accepting_new_patients_ind) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;