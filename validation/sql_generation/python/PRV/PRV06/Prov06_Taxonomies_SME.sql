create
or replace temporary view Prov06_Taxonomies_SME as
select
    T.*,
    cast(F.label as smallint) as PRVDR_CLSFCTN_SME
from
    Prov06_Taxonomies_IND T
    left join prv_formats_sm F on F.fmtname = SMECLASS
    and (
        Trim(T.PRVDR_CLSFCTN_CD) >= F.start
        and Trim(T.PRVDR_CLSFCTN_CD) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
