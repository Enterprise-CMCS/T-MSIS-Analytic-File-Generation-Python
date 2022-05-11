create
or replace temporary view Prov03_Locations_AST as
select
    T.*,
    cast(F.label as varchar(2)) as ADR_STATE_CD
from
    Prov03_Locations_AST1 T
    left join prv_formats_sm F on F.fmtname = STFIPV
    and (
        Trim(T.addr_state2) >= F.start
        and Trim(T.addr_state2) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id
