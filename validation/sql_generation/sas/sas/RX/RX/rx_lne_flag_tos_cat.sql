create temp table rx_lne_flag_tos_cat as
select
    *,
    max(DME_lne_clms) over (partition by submtg_state_cd, rx_link_key) as DME_clms,
    sum(DME_lne_clms) over (partition by submtg_state_cd, rx_link_key) as DME_lne_cnts
from
    (
        select
            a.*,
            case
                when not_fin_clm = 1
                and (
                    ever_valid_ndc = 1
                    or ever_valid_ndc is null
                    or (
                        ever_valid_ndc = 0
                        and ever_dme_hhs_tos = 0
                    )
                ) then 1
                else 0
            end as rx_clms,
            case
                when not_fin_clm = 1
                and (
                    ever_valid_ndc = 0
                    and ever_dme_hhs_tos = 1
                ) then 1
                else 0
            end as DME_lne_clms
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
                    rx_combined
            ) a
    ) s1