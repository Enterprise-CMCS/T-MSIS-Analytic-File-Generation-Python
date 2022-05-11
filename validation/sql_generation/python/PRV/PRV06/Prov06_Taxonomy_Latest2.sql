create
or replace temporary view Prov06_Taxonomy_Latest2 as
select
    T.*
from
    Prov06_Taxonomy_Copy T -- left join
    --     data_anltcs_dm_prod.state_submsn_type s
    --     on
    --         on T.submitting_state = s.submtg_state_cd
    --         and upper(s.fil_type) = 'PRV'
where
    (
        T.prov_taxonomy_classification_eff_date <= to_date('2021-10-31')
        and (
            T.prov_taxonomy_classification_end_date >= to_date('2021-10-01')
            or T.prov_taxonomy_classification_end_date is NULL
        )
    )
) -- and (
--     (
--         upper(coalesce(s.submsn_type, 'X')) <> 'CSO'
--         and a.TMSIS_RPTG_PRD >= to_date('2021-10-01')
--     )
--     or (upper(coalesce(s.submsn_type, 'X')) = 'CSO')
-- )
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_classification_type
