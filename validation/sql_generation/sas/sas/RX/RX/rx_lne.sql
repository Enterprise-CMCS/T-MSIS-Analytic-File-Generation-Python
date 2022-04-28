create temp table rx_lne as
select
    submtg_state_cd,
    msis_ident_num,
    da_run_id,
    line_num,
    rx_fil_dt as fil_dt,
    rx_link_key,
    rx_vrsn as vrsn,
    mdcd_pd_amt,
    xix_srvc_ctgry_cd,
    tos_cd,
    xxi_srvc_ctgry_cd,
    bnft_type_cd,
    ndc_cd,
    min(
        case
            when ndc_cd is null then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as all_null_ndc_cd,
    max(
        case
            when (
                (
                    length(ndc_cd) = 10
                    or length(ndc_cd) = 11
                )
                and ndc_cd !~ '([^0-9])'
            ) then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_valid_ndc,
    max(
        case
            when bnft_type_cd in ('039') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_icf_bnft_typ,
    max(
        case
            when tos_cd in ('123', '131', '135') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_dsh_drg_ehr_tos,
    max(
        case
            when tos_cd in ('119', '120', '121', '122') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_cap_pymt_tos,
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
    ) over (partition by submtg_state_cd, rx_link_key) as ever_cap_tos,
    max(
        case
            when tos_cd in ('119', '122') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_php_tos,
    max(
        case
            when tos_cd in ('123') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_dsh_tos,
    max(
        case
            when tos_cd in ('131') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_drg_rbt_tos,
    max(
        case
            when tos_cd in ('036', '018') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_dme_hhs_tos,
    max(
        case
            when xix_srvc_ctgry_cd in ('001B', '002B') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_dsh_xix_srvc_ctgry,
    max(
        case
            when xix_srvc_ctgry_cd in ('07A1', '07A2', '07A3', '07A4', '07A5', '07A6') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_othr_fin_xix_srvc_ctgry,
    max(
        case
            when CLL_STUS_CD in ('542', '585', '654') then 1
            else 0
        end
    ) over (partition by submtg_state_cd, rx_link_key) as ever_denied_line
from
    rxL