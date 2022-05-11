create temp table ot_lne as
select
    submtg_state_cd,
    msis_ident_num,
    da_run_id,
    line_num,
    ot_fil_dt as fil_dt,
    ot_link_key,
    ot_vrsn as vrsn,
    mdcd_pd_amt,
    xix_srvc_ctgry_cd,
    tos_cd,
    xxi_srvc_ctgry_cd,
    bnft_type_cd,
    srvcng_prvdr_txnmy_cd,
    rev_cd,
    min(
        case
            when rev_cd is null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as all_null_rev_cd,
    max(
        case
            when rev_cd in (
                '0510',
                '0511',
                '0512',
                '0513',
                '0514',
                '0515',
                '0516',
                '0517',
                '0518',
                '0519',
                '0520',
                '0521',
                '0522',
                '0523',
                '0524',
                '0525',
                '0526',
                '0527',
                '0528',
                '0529'
            ) then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_clinic_rev,
    max(
        case
            when rev_cd in (
                '0650',
                '0651',
                '0652',
                '0653',
                '0654',
                '0655',
                '0656',
                '0657',
                '0658',
                '0659',
                '0115',
                '0125',
                '0135',
                '0145'
            ) then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_hospice_rev,
    min(
        case
            when rev_cd in (
                '0023',
                '056',
                '056 
',
                '0560',
                '0561',
                '0562',
                '0563',
                '0564',
                '0565',
                '0566',
                '0567',
                '0568',
                '0569',
                '057',
                '057 
',
                '0570',
                '0571',
                '0572',
                '0573',
                '0574',
                '0575',
                '0576',
                '0577',
                '0578',
                '0579',
                '058',
                '058 
',
                '0580',
                '0581',
                '0582',
                '0583',
                '0584',
                '0585',
                '0586',
                '0587',
                '0588',
                '0589',
                '059',
                '059 
',
                '0590',
                '0591',
                '0592',
                '0593',
                '0594',
                '0595',
                '0596',
                '0597',
                '0598',
                '0599'
            ) then 1
            when rev_cd is not null then 0
            else null
        end
    ) over (partition by submtg_state_cd, ot_link_key) as only_hh_rev,
    hcbs_txnmy,
    prcdr_cd,
    hcpcs_rate,
    srvcng_prvdr_num,
    srvcng_prvdr_npi_num,
    min(
        case
            when prcdr_cd is null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as all_null_prcdr_cd,
    min(
        case
            when hcpcs_rate is null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as all_null_hcpcs_cd,
    max(
        case
            when prcdr_cd is null
            and hcpcs_rate is null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_null_prcdr_hcpcs_cd,
    max(
        case
            when prcdr_cd is not null
            or hcpcs_rate is not null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_valid_prcdr_hcpcs_cd,
    min(
        case
            when prcdr_cd in (
                '99503',
                '99504',
                '99505',
                '99506',
                '99507',
                '99509',
                '99511',
                '99512',
                '99600',
                '99601',
                '99602',
                'G0068',
                'G0069',
                'G0070',
                'G0088',
                'G0089',
                'G0090',
                'G0151',
                'G0152',
                'G0153',
                'G0154',
                'G0155',
                'G0156',
                'G0157',
                'G0158',
                'G0159',
                'G0160',
                'G0161',
                'G0162',
                'G0163',
                'G0164',
                'G0299',
                'G0300',
                'G0490',
                'G0493',
                'G0494',
                'G0495',
                'G0496',
                'S5108',
                'S5109',
                'S5110',
                'S5111',
                'S5115',
                'S5116',
                'S5180',
                'S5181',
                'S5522',
                'S5523',
                'S9097',
                'S9098',
                'S9122',
                'S9123',
                'S9124',
                'S9127',
                'S9128',
                'S9129',
                'S9131',
                'S9474',
                'T1000',
                'T1001',
                'T1002',
                'T1003',
                'T1004',
                'T1021',
                'T1022',
                'T1030',
                'T1031',
                'T1502',
                'T1503'
            )
            or hcpcs_rate in (
                '99503',
                '99504',
                '99505',
                '99506',
                '99507',
                '99509',
                '99511',
                '99512',
                '99600',
                '99601',
                '99602',
                'G0068',
                'G0069',
                'G0070',
                'G0088',
                'G0089',
                'G0090',
                'G0151',
                'G0152',
                'G0153',
                'G0154',
                'G0155',
                'G0156',
                'G0157',
                'G0158',
                'G0159',
                'G0160',
                'G0161',
                'G0162',
                'G0163',
                'G0164',
                'G0299',
                'G0300',
                'G0490',
                'G0493',
                'G0494',
                'G0495',
                'G0496',
                'S5108',
                'S5109',
                'S5110',
                'S5111',
                'S5115',
                'S5116',
                'S5180',
                'S5181',
                'S5522',
                'S5523',
                'S9097',
                'S9098',
                'S9122',
                'S9123',
                'S9124',
                'S9127',
                'S9128',
                'S9129',
                'S9131',
                'S9474',
                'T1000',
                'T1001',
                'T1002',
                'T1003',
                'T1004',
                'T1021',
                'T1022',
                'T1030',
                'T1031',
                'T1502',
                'T1503'
            ) then 1
            when prcdr_cd is null
            and hcpcs_rate is null then null
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as only_hh_procs,
    max(
        case
            when bnft_type_cd in ('039') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_icf_bnft_typ,
    max(
        case
            when tos_cd in ('123', '131', '135') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_dsh_drg_ehr_tos,
    max(
        case
            when tos_cd in ('119', '120', '121', '122') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_cap_pymt_tos,
    max(
        case
            when tos_cd in (
                '119',
                '120',
                '121',
                '122',
                '138',
                '139',
                '140',
                '141',
                '142',
                '143',
                '144'
            ) then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_cap_tos,
    max(
        case
            when tos_cd in ('119', '122') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_php_tos,
    max(
        case
            when tos_cd in ('123') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_dsh_tos,
    max(
        case
            when tos_cd in ('131') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_drg_rbt_tos,
    max(
        case
            when tos_cd in ('036', '018') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_dme_hhs_tos,
    max(
        case
            when xix_srvc_ctgry_cd in ('001B', '002B') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_dsh_xix_srvc_ctgry,
    max(
        case
            when xix_srvc_ctgry_cd in ('07A1', '07A2', '07A3', '07A4', '07A5', '07A6') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_othr_fin_xix_srvc_ctgry,
    max(
        case
            when CLL_STUS_CD in ('542', '585', '654') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as ever_denied_line
from
    otL