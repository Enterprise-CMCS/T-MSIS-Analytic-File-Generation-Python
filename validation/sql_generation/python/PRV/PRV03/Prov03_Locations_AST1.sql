create
or replace temporary view Prov03_Locations_AST1 as
select
    T.*,
    when F.label is null then T.addr_state
    else F.label
end as addr_state2
from
    Prov03_Locations_link T
    left join prv_formats_sm F on F.fmtname = STCDN
    and (
        Trim(T.addr_state) >= F.start
        and Trim(T.addr_state) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id
