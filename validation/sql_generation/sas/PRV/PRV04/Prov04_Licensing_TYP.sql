create table Prov04_Licensing_TYP diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_location_id
) as
select
    T.*,
    cast(F.label as varchar(1)) as LCNS_TYPE_CD
from
    #Prov04_Licensing T left join prv_formats_sm F on F.fmtname='LICCDV' and (Trim(T.license_type)>=F.start and Trim(T.license_type)<=F.end) order by T.tms_run_id , T.submitting_state , T.submitting_state_prov_id , T.prov_location_id;;