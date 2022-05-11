create temp table rx_header_0 as
select
    submtg_state_cd,
    msis_ident_num,
    da_run_id,
    rx_fil_dt as fil_dt,
    rx_link_key,
    rx_vrsn as vrsn,
    'rx' as file_type,
    clm_type_cd,
    srvc_trkng_type_cd,
    srvc_trkng_pymt_amt,
    blg_prvdr_txnmy_cd,
    tot_mdcd_pd_amt,
    blg_prvdr_npi_num,
    cmpnd_drug_ind,
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
    rxH