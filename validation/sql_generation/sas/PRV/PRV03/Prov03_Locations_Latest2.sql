create table Prov03_Locations_Latest2 diststyle key distkey(submitting_state_prov_id) compound sortkey (
    tms_run_id,
    submitting_state,
    submitting_state_prov_id,
    prov_addr_type,
    prov_location_id
) as
select
    T.*
from
    #Prov03_Locations_Copy T left join 
    data_anltcs_dm_prod.state_submsn_type s on T.submitting_state = s.submtg_state_cd
    and upper(s.fil_type) = 'PRV'
where
    (
        date_cmp(
            T.prov_location_and_contact_info_eff_date,
            '31DEC2017'
        ) in(-1, 0)
        and (
            date_cmp(
                T.prov_location_and_contact_info_end_date,
                '01DEC2017'
            ) in(0, 1)
            or T.prov_location_and_contact_info_end_date is NULL
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
    T.prov_addr_type,
    T.prov_location_id;

;