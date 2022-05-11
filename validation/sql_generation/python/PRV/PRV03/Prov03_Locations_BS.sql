create
or replace temporary view Prov03_Locations_BS as
select
    T.*,
    cast(F.label as varchar(1)) as ADR_BRDR_STATE_IND
from
    Prov03_Locations_AST T
    left join prv_formats_sm F on F.fmtname = BRDRSTV
    and (
        Trim(T.addr_border_state_ind) >= F.start
        and Trim(T.addr_border_state_ind) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id
