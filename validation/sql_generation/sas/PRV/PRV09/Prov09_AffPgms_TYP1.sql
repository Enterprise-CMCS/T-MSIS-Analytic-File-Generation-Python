create table Prov09_AffPgms_TYP1 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id
) as
select
    T.*,
    cast(F.label as varchar(1)) as AFLTD_PGM_TYPE_CD
from
    Prov09_AffPgms T
    left join prv_formats_sm F on F.fmtname = 'PRGCDV'
    and (
        Trim(T.affiliated_program_type) >= F.start
        and Trim(T.affiliated_program_type) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id;

;