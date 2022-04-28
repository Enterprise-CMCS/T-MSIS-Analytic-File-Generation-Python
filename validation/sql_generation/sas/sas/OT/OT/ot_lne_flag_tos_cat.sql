create temp table ot_lne_flag_tos_cat as
select
    *,
    max(dental_lne_clms) over (partition by submtg_state_cd, ot_link_key) as dental_clms,
    sum(dental_lne_clms) over (partition by submtg_state_cd, ot_link_key) as dental_lne_cnts,
    max(trnsprt_lne_clms) over (partition by submtg_state_cd, ot_link_key) as trnsprt_clms,
    sum(trnsprt_lne_clms) over (partition by submtg_state_cd, ot_link_key) as trnsprt_lne_cnts,
    max(othr_hcbs_lne_clms) over (partition by submtg_state_cd, ot_link_key) as othr_hcbs_clms,
    sum(othr_hcbs_lne_clms) over (partition by submtg_state_cd, ot_link_key) as othr_hcbs_lne_cnts,
    min(
        case
            when Lab_lne_clms = 1 then 1
            when prcdr_cd is null
            and hcpcs_rate is null then null
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as Lab_clms,
    sum(Lab_lne_clms) over (partition by submtg_state_cd, ot_link_key) as Lab_lne_cnts,
    min(
        case
            when Rad_lne_clms = 1 then 1
            when prcdr_cd is null
            and hcpcs_rate is null then null
            else 0
        end
    ) over (partition by submtg_state_cd, ot_link_key) as Rad_clms,
    sum(Rad_lne_clms) over (partition by submtg_state_cd, ot_link_key) as Rad_lne_cnts,
    max(DME_lne_clms) over (partition by submtg_state_cd, ot_link_key) as DME_clms,
    sum(DME_lne_clms) over (partition by submtg_state_cd, ot_link_key) as DME_lne_cnts
from
    (
        select
            a.*,
            b.code_cat as prcdr_ccs_cat,
            c.code_cat as hcpcs_ccs_cat,
            case
                when not_fin_clm = 1
                and (
                    all_null_rev_cd = 0
                    or (
                        bill_type_cd_upd is not null
                        and srvc_plc_cd is null
                    )
                ) then 1
                else 0
            end as inst_clms,
            case
                when not_fin_clm = 1
                and (
                    (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is not null
                        and srvc_plc_cd is not null
                    )
                    or (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is null
                        and srvc_plc_cd is not null
                    )
                    or (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is null
                        and srvc_plc_cd is null
                        and (ever_null_prcdr_hcpcs_cd = 0)
                    )
                ) then 1
                else 0
            end as prof_clms,
            case
                when not_fin_clm = 1
                and (
                    (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is not null
                        and srvc_plc_cd is not null
                    )
                    or (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is null
                        and srvc_plc_cd is not null
                    )
                    or (
                        (
                            all_null_rev_cd = 1
                            or all_null_rev_cd is null
                        )
                        and bill_type_cd_upd is null
                        and srvc_plc_cd is null
                        and (ever_valid_prcdr_hcpcs_cd = 1)
                    )
                ) then 1
                else 0
            end as prof_clms_2,
            case
                when not_fin_clm = 1
                and (
                    (
                        length(prcdr_cd) = 5
                        and substring(prcdr_cd, 1, 1) = 'D'
                    )
                    or (
                        length(hcpcs_rate) = 5
                        and substring(hcpcs_rate, 1, 1) = 'D'
                    )
                ) then 1
                else 0
            end as dental_lne_clms,
            case
                when not_fin_clm = 1
                and (
                    b.code_cat = 'Transprt'
                    or c.code_cat = 'Transprt'
                ) then 1
                else 0
            end as trnsprt_lne_clms,
            case
                when not_fin_clm = 1
                and (
                    substring(rev_cd, 1, 3) in ('066', '310')
                    or prcdr_cd in (
                        'T1019',
                        'T1020',
                        'S5125',
                        'S5126',
                        'T0005',
                        'T1028',
                        'S5100',
                        'S5101',
                        'S5102',
                        'S5105',
                        'S5120',
                        'S5121',
                        'S5130',
                        'S5131',
                        'S5135',
                        'S5136',
                        'S5150',
                        'S5151',
                        'S5170'
                    )
                    or hcpcs_rate in (
                        'T1019',
                        'T1020',
                        'S5125',
                        'S5126',
                        'T0005',
                        'T1028',
                        'S5100',
                        'S5101',
                        'S5102',
                        'S5105',
                        'S5120',
                        'S5121',
                        'S5130',
                        'S5131',
                        'S5135',
                        'S5136',
                        'S5150',
                        'S5151',
                        'S5170'
                    )
                    or (
                        prcdr_cd in ('T2025')
                        and srvc_plc_cd in ('12')
                    )
                    or (
                        hcpcs_rate in ('T2025')
                        and srvc_plc_cd in ('12')
                    )
                    or bnft_type_cd in ('045')
                    or hcbs_txnmy in (
                        '04050',
                        '04060',
                        '06010',
                        '07010',
                        '08030',
                        '08040',
                        '08050',
                        '08060',
                        '09012'
                    )
                ) then 1
                else 0
            end as othr_hcbs_lne_clms,
            case
                when not_fin_clm = 1
                and (
                    b.code_cat = 'Lab'
                    or c.code_cat = 'Lab'
                ) then 1
                else 0
            end as Lab_lne_clms,
            case
                when not_fin_clm = 1
                and (
                    b.code_cat = 'Rad'
                    or c.code_cat = 'Rad'
                ) then 1
                else 0
            end as Rad_lne_clms,
            case
                when not_fin_clm = 1
                and (
                    b.code_cat = 'DME'
                    or c.code_cat = 'DME'
                ) then 1
                else 0
            end as DME_lne_clms,
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
                    or only_hh_procs = 1
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
                    ot_combined
            ) a
            left join ccs_proc b on a.prcdr_cd = b.cd_rng
            left join ccs_proc c on a.hcpcs_rate = c.cd_rng
    ) s1