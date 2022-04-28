create temp table ot_combined as
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
                or (
                    all_null_rev_cd = 1
                    and all_null_prcdr_cd = 1
                    and all_null_hcpcs_cd = 1
                )
            )
        ) then 1
        else 0
    end as supp_clms,
    row_number() over (
        partition by submtg_state_cd,
        ot_link_key
        order by
            submtg_state_cd,
            ot_link_key
    ) as rec_cnt
from
    (
        select
            a.*,
            case
                when cmc_php = 0
                and other_pmpm = 0
                and (
                    (
                        clm_type_cd in ('4', 'D', 'X', '5', 'E', 'Y')
                        and (
                            srvc_trkng_type_cd in ('02')
                            or ever_dsh_tos = 1
                        )
                    )
                    or (
                        clm_type_cd in ('4', 'X', '5', 'Y')
                        and ever_dsh_xix_srvc_ctgry = 1
                    )
                ) then 1
                else 0
            end as dsh_flag
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
                    l.rev_cd,
                    l.all_null_rev_cd,
                    case
                        when bill_type_cd_upd is null then 1
                        else 0
                    end as upd_bill_type_cd_null,
                    substring(bill_type_cd_upd, 2, 1) as bill_typ_byte2,
                    substring(bill_type_cd_upd, 3, 1) as bill_typ_byte3,
                    l.ever_clinic_rev,
                    l.ever_hospice_rev,
                    l.only_hh_rev,
                    l.srvcng_prvdr_txnmy_cd,
                    case
                        when h.srvc_plc_cd is null then 1
                        else 0
                    end as srvc_plc_cd_null,
                    l.hcbs_txnmy,
                    l.prcdr_cd,
                    l.hcpcs_rate,
                    l.all_null_prcdr_cd,
                    l.all_null_hcpcs_cd,
                    l.ever_null_prcdr_hcpcs_cd,
                    l.ever_valid_prcdr_hcpcs_cd,
                    l.srvcng_prvdr_num,
                    l.srvcng_prvdr_npi_num,
                    l.only_hh_procs,
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
                            (
                                clm_type_cd in ('2', 'B', 'V')
                                and (
                                    ever_dsh_drg_ehr_tos = 0
                                    or ever_dsh_drg_ehr_tos is null
                                )
                            )
                            OR (
                                clm_type_cd in ('4', 'D', 'X', '5', 'E', 'Y')
                                and (ever_cap_tos = 1)
                            )
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
                            OR (
                                clm_type_cd in ('4', 'D', 'X', '5', 'E', 'Y')
                                and (ever_cap_tos = 1)
                            )
                        )
                        and (
                            ever_php_tos = 0
                            or ever_php_tos is null
                        ) then 1
                        else 0
                    end as other_pmpm
                from
                    ot_header_0 h
                    left join ot_lne l on h.submtg_state_cd = l.submtg_state_cd
                    and h.ot_link_key = l.ot_link_key
            ) a
    ) b