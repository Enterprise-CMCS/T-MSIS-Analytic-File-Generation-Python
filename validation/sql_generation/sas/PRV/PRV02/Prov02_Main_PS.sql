create table Prov02_Main_PS diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as varchar(2)) as PRVDR_PRFT_STUS_CD
from
    #Prov02_Main_O T left join 
    prv_formats_sm F on F.fmtname = 'PROFV'
    and (
        Trim(T.prov_profit_status) >= F.start
        and Trim(T.prov_profit_status) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;