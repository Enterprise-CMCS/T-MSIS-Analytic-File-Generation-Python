create
or replace temporary view Prov06_Taxonomies_TYP as
select
    T.*,
    cast(F.label as varchar(1)) as PRVDR_CLSFCTN_TYPE_CD
from
    Prov06_Taxonomies_STV T
    left join prv_formats_sm F on F.fmtname = CLSSCDV
    and (
        Trim(T.prov_classification_type) >= F.start
        and Trim(T.prov_classification_type) <= F.
    end
)
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id
