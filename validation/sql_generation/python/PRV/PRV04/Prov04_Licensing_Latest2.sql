create
or replace temporary view Prov04_Licensing_Latest2 as
select
    T.*
from
    Prov04_Licensing_Copy T -- left join
    --     data_anltcs_dm_prod.state_submsn_type s
    --     on
    --         on T.submitting_state = s.submtg_state_cd
    --         and upper(s.fil_type) = 'PRV'
where
    (
        T.prov_license_eff_date <= to_date('2021-10-31')
        and (
            T.prov_license_end_date >= to_date('2021-10-01')
            or T.prov_license_end_date is NULL
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
    T.prov_location_id,
    T.license_type,
    T.license_or_accreditation_number,
    T.license_issuing_entity_id
