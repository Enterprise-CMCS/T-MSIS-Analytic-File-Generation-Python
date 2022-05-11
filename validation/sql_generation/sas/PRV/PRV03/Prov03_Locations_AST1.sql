create table Prov03_Locations_AST1 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id
) as
select
    T.*,
    case
        when F.label is null then T.addr_state
        else F.label
    end :: varchar(2) as addr_state2
from
    #Prov03_Locations_link T left join prv_formats_sm F on F.fmtname='STCDN' and (Trim(T.addr_state)>=F.start and Trim(T.addr_state)<=F.end) order by T.tms_run_id , T.submitting_state , T.submitting_state_prov_id , 
    T.prov_location_id;

;