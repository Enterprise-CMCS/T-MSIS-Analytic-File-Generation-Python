create table Prov06_Taxonomy_Latest2 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_classification_type
) as
select
    T.*
from
    #Prov06_Taxonomy_Copy T left join 
    data_anltcs_dm_prod.state_submsn_type s on T.submitting_state = s.submtg_state_cd
    and upper(s.fil_type) = 'PRV'
where
    (
        date_cmp(
            T.prov_taxonomy_classification_eff_date,
            '31DEC2017'
        ) in(-1, 0)
        and (
            date_cmp(
                T.prov_taxonomy_classification_end_date,
                '01DEC2017'
            ) in(0, 1)
            or T.prov_taxonomy_classification_end_date is NULL
        )
    )
    and (
        (
            upper(coalesce(s.submsn_type, 'X')) <> 'CSO'
            and date_cmp(T.tms_reporting_period, '01DEC2017') in (1, 0)
        )
        or (upper(coalesce(s.submsn_type, 'X')) = 'CSO')
    )
order by
    T.tms_run_id,
    T.submitting_state,
    T.submitting_state_prov_id,
    T.prov_classification_type;

;