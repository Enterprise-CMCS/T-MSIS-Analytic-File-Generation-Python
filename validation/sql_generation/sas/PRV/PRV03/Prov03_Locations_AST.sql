create table Prov03_Locations_AST diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id
) as
select
    T.*,
    cast(F.label as varchar(2)) as ADR_STATE_CD
from
    #Prov03_Locations_AST1 T left join prv_formats_sm F on F.fmtname='STFIPV' and (Trim(T.addr_state2)>=F.start and Trim(T.addr_state2)<=F.end) order by T.tms_run_id , T.submitting_state , T.submitting_state_prov_id , T.prov_location_id;;