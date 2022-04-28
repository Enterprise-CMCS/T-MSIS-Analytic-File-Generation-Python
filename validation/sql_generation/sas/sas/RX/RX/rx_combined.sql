create temp table rx_combined as
select
    b.*,
    case
        when (
            msis_ident_num like '8{len(trim(msis_ident_num))}'
            or msis_ident_num like '9{len(trim(msis_ident_num))}'
            or msis_ident_num like '0{len(trim(msis_ident_num))}'
            or msis_ident_num !~ '[(a-z)|(A-Z)|(0-9)]'
            or msis_ident_num = '&'
            or msis_ident_num is null
        ) then 1
        else 0
    end as inval_msis_id,
    case
        when (
            cmc_php = 0
            and other_pmpm = 0
            and dsh_flag = 0
            and clm_type_cd is null
            and ever_cap_pymt_tos = 1
        ) then 1
        else 0
    end as cap_tos_null_toc,
    case
        when (
            cmc_php = 0
            and other_pmpm = 0
            and dsh_flag = 0
            and clm_type_cd in ('4', 'D', 'X')
        ) then 1
        else 0
    end as srvc_trkg,
    case
        when (
            cmc_php = 0
            and other_pmpm = 0
            and dsh_flag = 0
            and clm_type_cd in ('5', 'E', 'Y')
            and (
                (
                    msis_ident_num like '8{len(trim(msis_ident_num))}'
                    or msis_ident_num like '9{len(trim(msis_ident_num))}'
                    or msis_ident_num like '0{len(trim(msis_ident_num))}'
                    or msis_ident_num !~ '[(a-z)|(A-Z)|(0-9)]'
                    or msis_ident_num = '&'
                    or msis_ident_num is null
                )
                or (all_null_ndc_cd = 1)
            )
        ) then 1
        else 0
    end as supp_clms,
    row_number() over (
        partition by submtg_state_cd,
        rx_link_key
        order by
            submtg_state_cd,
            rx_link_key
    ) as rec_cnt
from
    (
        select
            a.*,
            0 as dsh_flag
        from
            (
                select
                    h.*,
                    l.line_num,
                    l.mdcd_pd_amt,
                    l.xix_srvc_ctgry_cd,
                    l.tos_cd,
                    l.xxi_srvc_ctgry_cd,
                    l.bnft_type_cd,
                    l.ever_icf_bnft_typ,
                    l.ndc_cd,
                    l.all_null_ndc_cd,
                    l.ever_valid_ndc,
                    l.ever_dsh_drg_ehr_tos,
                    l.ever_cap_tos,
                    l.ever_cap_pymt_tos,
                    l.ever_php_tos,
                    l.ever_dsh_tos,
                    l.ever_drg_rbt_tos,
                    l.ever_dme_hhs_tos,
                    l.ever_dsh_xix_srvc_ctgry,
                    l.ever_othr_fin_xix_srvc_ctgry,
                    l.ever_denied_line,
                    case
                        when (
                            srvc_trkng_type_cd not in ('01')
                            or srvc_trkng_type_cd is null
                        )
                        and (
                            clm_type_cd in ('2', 'B', 'V')
                            and (
                                ever_dsh_drg_ehr_tos = 0
                                or ever_dsh_drg_ehr_tos is null
                            )
                        )
                        and (
                            cmpnd_drug_ind is null
                            and all_null_ndc_cd = 1
                        )
                        and (ever_php_tos = 1) then 1
                        else 0
                    end as cmc_php,
                    case
                        when (
                            srvc_trkng_type_cd not in ('01')
                            or srvc_trkng_type_cd is null
                        )
                        and (
                            (
                                clm_type_cd in ('2', 'B', 'V')
                                and (
                                    ever_dsh_drg_ehr_tos = 0
                                    or ever_dsh_drg_ehr_tos is null
                                )
                            )
                            and (
                                cmpnd_drug_ind is null
                                and all_null_ndc_cd = 1
                            )
                        )
                        and (
                            ever_php_tos = 0
                            or ever_php_tos is null
                        ) then 1
                        else 0
                    end as other_pmpm
                from
                    rx_header_0 h
                    left join rx_lne l on h.submtg_state_cd = l.submtg_state_cd
                    and h.rx_link_key = l.rx_link_key
            ) a
    ) b