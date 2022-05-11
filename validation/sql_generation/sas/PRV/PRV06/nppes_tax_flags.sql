create table nppes_tax_flags as
select
    nppes.*,
    t2.prvdr_id,
    nvl(nppes.hc_prvdr_prmry_txnmy_sw_1, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_2, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_3, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_4, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_5, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_6, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_7, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_8, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_9, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_10, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_11, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_12, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_13, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_14, ' ') || nvl(nppes.hc_prvdr_prmry_txnmy_sw_15, ' ') as sw_positions,
    regexp_count(sw_positions, 'Y') as taxo_switches,
    nvl(substring(nppes.hc_prvdr_txnmy_cd_1, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_2, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_3, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_4, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_5, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_6, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_7, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_8, 10, 1), ' ') || nvl(substring(nppes.hc_prvdr_txnmy_cd_9, 10, 1), ' ') || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_10, 10, 1),
        ' '
    ) || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_11, 10, 1),
        ' '
    ) || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_12, 10, 1),
        ' '
    ) || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_13, 10, 1),
        ' '
    ) || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_14, 10, 1),
        ' '
    ) || nvl(
        substring(nppes.hc_prvdr_txnmy_cd_15, 10, 1),
        ' '
    ) as cd_positions,
    regexp_count(cd_positions, 'X') as taxo_cnt
from
    #nppes_id2 t2 left join cms_prod.data_anltcs_prvdr_npi_data_vw nppes on t2.prvdr_id=cast(nppes.prvdr_npi as varchar)