create table Prov02_Main_STV diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    case
        when F.label is null then T.submitting_state
        else F.label
    end :: varchar(2) as SUBMTG_STATE_rcd
from
    #Prov02_Main T left join prv_formats_sm F on F.fmtname='STFIPC' and (Trim(T.submitting_state)>=F.start and Trim(T.submitting_state)<=F.end) order by T.tms_run_id , T.submitting_state , T.submitting_state_prov_id;;