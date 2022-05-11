create
or replace temporary view Prov02_Main_PC as
select
    T.*,
    cast(F.label as varchar(2)) as FAC_GRP_INDVDL_CD
from
    Prov02_Main_RG T
    left join prv_formats_sm F on F.fmtname = PROVCLSS
    and (
        Trim(T.facility_group_individual_code) >= F.start
        and Trim(T.facility_group_individual_code) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
