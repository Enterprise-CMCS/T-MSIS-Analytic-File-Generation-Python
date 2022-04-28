create temp table nppes_npi_step2 distkey (prvdr_npi) sortkey (prvdr_npi) as
select
    a.prvdr_npi,
    taxo_switches,
    sw_positions,
    taxo_cds,
    cd_positions,
    position('Y' in sw_positions) as primary_switch_position,
    case
        when taxo_switches = 1 then subarray(
            array(
                nvl(hc_prvdr_txnmy_cd_1, ' '),
                nvl(hc_prvdr_txnmy_cd_2, ' '),
                nvl(hc_prvdr_txnmy_cd_3, ' '),
                nvl(hc_prvdr_txnmy_cd_4, ' '),
                nvl(hc_prvdr_txnmy_cd_5, ' '),
                nvl(hc_prvdr_txnmy_cd_6, ' '),
                nvl(hc_prvdr_txnmy_cd_7, ' '),
                nvl(hc_prvdr_txnmy_cd_8, ' '),
                nvl(hc_prvdr_txnmy_cd_9, ' '),
                nvl(hc_prvdr_txnmy_cd_10, ' '),
                nvl(hc_prvdr_txnmy_cd_11, ' '),
                nvl(hc_prvdr_txnmy_cd_12, ' '),
                nvl(hc_prvdr_txnmy_cd_13, ' '),
                nvl(hc_prvdr_txnmy_cd_14, ' '),
                nvl(hc_prvdr_txnmy_cd_15, ' ')
            ),
            position('Y' in sw_positions) -1,
            1
        )
        when taxo_switches = 0
        and taxo_cds = 1 then subarray(
            array(
                nvl(hc_prvdr_txnmy_cd_1, ' '),
                nvl(hc_prvdr_txnmy_cd_2, ' '),
                nvl(hc_prvdr_txnmy_cd_3, ' '),
                nvl(hc_prvdr_txnmy_cd_4, ' '),
                nvl(hc_prvdr_txnmy_cd_5, ' '),
                nvl(hc_prvdr_txnmy_cd_6, ' '),
                nvl(hc_prvdr_txnmy_cd_7, ' '),
                nvl(hc_prvdr_txnmy_cd_8, ' '),
                nvl(hc_prvdr_txnmy_cd_9, ' '),
                nvl(hc_prvdr_txnmy_cd_10, ' '),
                nvl(hc_prvdr_txnmy_cd_11, ' '),
                nvl(hc_prvdr_txnmy_cd_12, ' '),
                nvl(hc_prvdr_txnmy_cd_13, ' '),
                nvl(hc_prvdr_txnmy_cd_14, ' '),
                nvl(hc_prvdr_txnmy_cd_15, ' ')
            ),
            position('X' in cd_positions) -1,
            1
        )
        when taxo_switches = 0
        and taxo_cds > 1 then null
        when taxo_switches > 1 then subarray(
            array(
                nvl(hc_prvdr_txnmy_cd_1, ' '),
                nvl(hc_prvdr_txnmy_cd_2, ' '),
                nvl(hc_prvdr_txnmy_cd_3, ' '),
                nvl(hc_prvdr_txnmy_cd_4, ' '),
                nvl(hc_prvdr_txnmy_cd_5, ' '),
                nvl(hc_prvdr_txnmy_cd_6, ' '),
                nvl(hc_prvdr_txnmy_cd_7, ' '),
                nvl(hc_prvdr_txnmy_cd_8, ' '),
                nvl(hc_prvdr_txnmy_cd_9, ' '),
                nvl(hc_prvdr_txnmy_cd_10, ' '),
                nvl(hc_prvdr_txnmy_cd_11, ' '),
                nvl(hc_prvdr_txnmy_cd_12, ' '),
                nvl(hc_prvdr_txnmy_cd_13, ' '),
                nvl(hc_prvdr_txnmy_cd_14, ' '),
                nvl(hc_prvdr_txnmy_cd_15, ' ')
            ),
            least(
                taxo1,
                taxo2,
                taxo3,
                taxo4,
                taxo5,
                taxo6,
                taxo7,
                taxo8,
                taxo9,
                taxo10,
                taxo11,
                taxo12,
                taxo13,
                taxo14,
                taxo15
            ) -1,
            1
        )
        else null
    end as selected_txnmy_cdx,
    hc_prvdr_txnmy_cd_1,
    hc_prvdr_txnmy_cd_2,
    hc_prvdr_txnmy_cd_3,
    hc_prvdr_txnmy_cd_4,
    hc_prvdr_txnmy_cd_5,
    hc_prvdr_txnmy_cd_6,
    hc_prvdr_txnmy_cd_7,
    hc_prvdr_txnmy_cd_8,
    hc_prvdr_txnmy_cd_9,
    hc_prvdr_txnmy_cd_10,
    hc_prvdr_txnmy_cd_11,
    hc_prvdr_txnmy_cd_12,
    hc_prvdr_txnmy_cd_13,
    hc_prvdr_txnmy_cd_14,
    hc_prvdr_txnmy_cd_15,
    taxo1,
    taxo2,
    taxo3,
    taxo4,
    taxo5,
    taxo6,
    taxo7,
    taxo8,
    taxo9,
    taxo10,
    taxo11,
    taxo12,
    taxo13,
    taxo14,
    taxo15
from
    cms_prod.data_anltcs_prvdr_npi_data_vw a
    inner join taxo_switches b on a.prvdr_npi = b.prvdr_npi
where
    b.taxo_cds > 0