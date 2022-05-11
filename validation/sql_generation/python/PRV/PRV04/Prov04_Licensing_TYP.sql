create
or replace temporary view Prov04_Licensing_TYP as
select
    T.*,
    cast(F.label as varchar(1)) as LCNS_TYPE_CD
from
    Prov04_Licensing_STV T
    left join prv_formats_sm F on F.fmtname = LICCDV
    and (
        Trim(T.license_type) >= F.start
        and Trim(T.license_type) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_location_id
