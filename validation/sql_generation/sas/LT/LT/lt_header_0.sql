create temp table lt_header_0 as
select
    submtg_state_cd,
    msis_ident_num,
    da_run_id,
    lt_fil_dt as fil_dt,
    lt_link_key,
    lt_vrsn as vrsn,
    'lt' as file_type,
    clm_type_cd,
    srvc_trkng_type_cd,
    srvc_trkng_pymt_amt,
    blg_prvdr_txnmy_cd,
    tot_mdcd_pd_amt,
    blg_prvdr_npi_num,
    dgns_1_cd,
    dgns_2_cd,
    bill_type_cd,
    case
        when length(bill_type_cd) = 3
        and substring(bill_type_cd, 1, 1) != '0' then '0' || substring(bill_type_cd, 1, 3)
        when length(bill_type_cd) = 4
        and bill_type_cd not in ('0000') then bill_type_cd
        else null
    end as bill_type_cd_upd,
    case
        when dgns_1_cd is null then 1
        else 0
    end as dgns_1_cd_null,
    num_cll,
    case
        when clm_type_cd in ('1', 'A', 'U') then '1_FFS'
        when clm_type_cd in ('2', 'B', 'V') then '2_CAP'
        when clm_type_cd in ('3', 'C', 'W') then '3_ENC'
        when clm_type_cd in ('4', 'D', 'X') then '4_SRVC_TRKG'
        when clm_type_cd in ('5', 'E', 'Y') then '5_SUPP'
        else null
    end as clm_type_grp_ctgry
from
    ltH