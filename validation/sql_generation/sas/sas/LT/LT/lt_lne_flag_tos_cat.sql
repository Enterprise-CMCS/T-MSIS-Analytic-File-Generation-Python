create temp table lt_lne_flag_tos_cat as
select
    *
from
    (
        select
            a.*,
            b.prvdr_txnmy_icf,
            b.prvdr_txnmy_nf,
            b.prvdr_txnmy_othr_res,
            case
                when not_fin_clm = 1
                and (
                    (
                        substring(bill_type_cd_upd, 2, 1) in ('1', '4')
                        and substring(bill_type_cd_upd, 3, 1) in ('1', '2')
                    )
                    or b.prvdr_txnmy_ip = 1
                ) then 1
                else 0
            end as inp_clms,
            case
                when not_fin_clm = 1
                and (
                    b.prvdr_txnmy_icf = 1
                    or (
                        substring(bill_type_cd_upd, 2, 1) in ('6')
                        and substring(bill_type_cd_upd, 3, 1) in ('5', '6')
                    )
                ) then 1
                else 0
            end as ic_clms,
            case
                when not_fin_clm = 1
                and (
                    b.prvdr_txnmy_nf = 1
                    or (substring(bill_type_cd_upd, 2, 1) in ('2'))
                    or (substring(bill_type_cd_upd, 2, 2) in ('18'))
                ) then 1
                else 0
            end as nf_clms,
            case
                when not_fin_clm = 1
                and (
                    b.prvdr_txnmy_othr_res = 1
                    or (substring(bill_type_cd_upd, 2, 2) in ('86'))
                ) then 1
                else 0
            end as othr_res_clms,
            case
                when not_fin_clm = 1
                and (
                    (
                        substring(bill_type_cd_upd, 2, 1) in ('1')
                        and substring(bill_type_cd_upd, 3, 1) in ('3', '4')
                    )
                    or (
                        substring(bill_type_cd_upd, 2, 1) in ('8')
                        and substring(bill_type_cd_upd, 3, 1) in ('3', '4', '5', '9')
                    )
                ) then 1
                else 0
            end as op_hosp_clms,
            case
                when not_fin_clm = 1
                and (
                    substring(bill_type_cd_upd, 2, 1) in ('7')
                    or ever_clinic_rev = 1
                ) then 1
                else 0
            end as clinic_clms,
            case
                when not_fin_clm = 1
                and (
                    substring(bill_type_cd_upd, 2, 2) in ('81', '82')
                    or ever_hospice_rev = 1
                ) then 1
                else 0
            end as hospice_clms,
            case
                when not_fin_clm = 1
                and (
                    substring(bill_type_cd_upd, 1, 3) in ('032', '033', '034')
                    or only_hh_rev = 1
                ) then 1
                else 0
            end as HH_clms
        from
            (
                select
                    *,
                    case
                        when cmc_php = 0
                        and other_pmpm = 0
                        and dsh_flag = 0
                        and srvc_trkg = 0
                        and supp_clms = 0
                        and cap_tos_null_toc = 0 then 1
                        else 0
                    end as not_fin_clm
                from
                    lt_combined
            ) a
            left join nppes_npi b on a.blg_prvdr_npi_num = b.prvdr_npi
    ) s1